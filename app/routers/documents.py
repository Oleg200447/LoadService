"""Document management endpoints."""
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
import uuid
import os
from typing import Annotated

from app.database import get_db
from app.dependencies import get_jwt_data
from app.schemas import (
    JWTData,
    DocumentUploadResponse,
    DocumentStatusResponse,
    DocumentDownloadResponse
)
from app.models import Document, Topic, TopicAccess
from app.services.minio_service import minio_service
from app.services.kafka_producer import kafka_producer
from app.utils.logger import logger

router = APIRouter(prefix="/documents", tags=["Documents"])


@router.post("/upload", response_model=DocumentUploadResponse, status_code=status.HTTP_201_CREATED)
async def upload_document(
    file: Annotated[UploadFile, File(...)],
    topic_id: Annotated[str, Form(...)],
    jwt_data: JWTData = Depends(get_jwt_data),
    db: AsyncSession = Depends(get_db)
):
    """
    Upload a document to a topic.
    
    - **file**: Document file (PDF, PPTX, or DOCX)
    - **topic_id**: ID of the topic to upload to
    
    Requires owner access to the topic or system role for system topics.
    """
    logger.info(f"Upload request from user {jwt_data.sub} for topic {topic_id}")
    
    try:
        topic_uuid = uuid.UUID(topic_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid topic_id format"
        )
    
    # Get topic
    stmt = select(Topic).where(Topic.id == topic_uuid)
    result = await db.execute(stmt)
    topic = result.scalar_one_or_none()
    
    if not topic:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Topic not found"
        )
    
    # Check access rights
    user_uuid = uuid.UUID(jwt_data.sub)
    
    if topic.is_system:
        # System topics: require system role and matching topic number
        if jwt_data.role != "system":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="System role required to upload to system topics"
            )
        # Check if user ID matches topic number (1-5)
        topic_number = topic.name.split('_')[-1]
        if jwt_data.nickname != topic_number:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User ID must match system topic number"
            )
    else:
        # User topics: require owner access
        stmt = select(TopicAccess).where(
            TopicAccess.topic_id == topic_uuid,
            TopicAccess.user_id == user_uuid,
            TopicAccess.role == "owner"
        )
        result = await db.execute(stmt)
        access = result.scalar_one_or_none()
        
        if not access:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Owner access required to upload documents"
            )
    
    # Validate file extension
    filename = file.filename or "unnamed"
    file_ext = os.path.splitext(filename)[1].lower()
    
    if file_ext not in [".pdf", ".pptx", ".docx"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only PDF, PPTX, and DOCX files are supported"
        )
    
    # Read file content
    file_content = await file.read()
    
    if len(file_content) == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Empty file not allowed"
        )
    
    # Create document record
    document = Document(
        topic_id=topic_uuid,
        uploader_id=user_uuid,
        filename=filename,
        storage_url="",  # Will be updated after MinIO upload
        status="pending"
    )
    db.add(document)
    await db.flush()  # Get the document ID
    
    # Upload to MinIO
    try:
        object_name = f"{document.id}" #change to f"{document.id}_{filename}
        storage_url = await minio_service.upload_file(
            bucket_name=str(topic_uuid),
            object_name=object_name,
            file_data=file_content,
            content_type=file.content_type or "application/octet-stream"
        )
        
        # Update document with storage URL
        document.storage_url = storage_url
        await db.commit()
        await db.refresh(document)
        
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to upload file to MinIO: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to upload file to storage"
        )
    
    # Send message to Kafka
    try:
        await kafka_producer.send_document_message(
            doc_id=str(document.id),
            doc_name = str(filename),
            user_id=jwt_data.sub,
            topic_name=str(topic_uuid),
            document_url=storage_url,
            file_extension=file_ext
        )
    except Exception as e:
        logger.error(f"Failed to send Kafka message: {e}")
        # Don't fail the request if Kafka fails
    
    logger.info(f"Document uploaded successfully: {document.id}")
    return document


@router.delete("/{doc_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_document(
    doc_id: str,
    jwt_data: JWTData = Depends(get_jwt_data),
    db: AsyncSession = Depends(get_db)
):
    """
    Delete a document.
    
    - **doc_id**: ID of the document to delete
    
    Requires owner access to the topic containing the document.
    """
    logger.info(f"Delete request from user {jwt_data.sub} for document {doc_id}")
    
    try:
        doc_uuid = uuid.UUID(doc_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid document_id format"
        )
    
    # Get document with topic
    stmt = select(Document).where(Document.id == doc_uuid)
    result = await db.execute(stmt)
    document = result.scalar_one_or_none()
    
    if not document:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Document not found"
        )
    
    # Get topic
    stmt = select(Topic).where(Topic.id == document.topic_id)
    result = await db.execute(stmt)
    topic = result.scalar_one_or_none()
    
    if not topic:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Topic not found"
        )
    
    # Check access rights
    user_uuid = uuid.UUID(jwt_data.sub)
    
    if topic.is_system:
        # System topics: require system role
        if jwt_data.role != "system":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="System role required to delete from system topics"
            )
        topic_number = topic.name.split('_')[-1]
        if jwt_data.sub != topic_number:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User ID must match system topic number"
            )
    else:
        # User topics: require owner access
        stmt = select(TopicAccess).where(
            TopicAccess.topic_id == document.topic_id,
            TopicAccess.user_id == user_uuid,
            TopicAccess.role == "owner"
        )
        result = await db.execute(stmt)
        access = result.scalar_one_or_none()
        
        if not access:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Owner access required to delete documents"
            )
    
    # Delete from MinIO
    try:
        object_name = document.storage_url.split('/')[-1]
        await minio_service.delete_file(
            bucket_name=str(document.topic_id),
            object_name=object_name
        )
    except Exception as e:
        logger.error(f"Failed to delete file from MinIO: {e}")
        # Continue with database deletion even if MinIO fails
    
    # Delete from database
    await db.delete(document)
    await db.commit()
    
    # Send message to Kafka
    try:
        await kafka_producer.send_delete_document_message(
            doc_id=str(document.id),
            user_id=jwt_data.sub,
            topic_name=str(document.topic_id)
        )
    except Exception as e:
        logger.error(f"Failed to send Kafka delete message: {e}")
        # Don't fail the request if Kafka fails
    
    logger.info(f"Document deleted successfully: {doc_id}")
    return None


@router.get("/{doc_id}/status", response_model=DocumentStatusResponse)
async def get_document_status(
    doc_id: str,
    jwt_data: JWTData = Depends(get_jwt_data),
    db: AsyncSession = Depends(get_db)
):
    """
    Get document processing status.
    
    - **doc_id**: ID of the document
    
    Returns the current processing status of the document (pending/success).
    """
    logger.info(f"Status request from user {jwt_data.sub} for document {doc_id}")
    
    try:
        doc_uuid = uuid.UUID(doc_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid document_id format"
        )
    
    # Get document
    stmt = select(Document).where(Document.id == doc_uuid)
    result = await db.execute(stmt)
    document = result.scalar_one_or_none()
    
    if not document:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Document not found"
        )
    
    return document


@router.get("/{doc_id}/download", response_model=DocumentDownloadResponse)
async def download_document(
    doc_id: str,
    jwt_data: JWTData = Depends(get_jwt_data),
    db: AsyncSession = Depends(get_db)
):
    """
    Get presigned URL for downloading a document.
    
    - **doc_id**: ID of the document to download
    
    Requires read or owner access to the topic containing the document.
    """
    logger.info(f"Download request from user {jwt_data.sub} for document {doc_id}")
    
    try:
        doc_uuid = uuid.UUID(doc_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid document_id format"
        )
    
    # Get document with topic
    stmt = select(Document).where(Document.id == doc_uuid)
    result = await db.execute(stmt)
    document = result.scalar_one_or_none()
    
    if not document:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Document not found"
        )
    
    # Get topic
    stmt = select(Topic).where(Topic.id == document.topic_id)
    result = await db.execute(stmt)
    topic = result.scalar_one_or_none()
    
    if not topic:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Topic not found"
        )
    
    # Check access rights
    user_uuid = uuid.UUID(jwt_data.sub)
    
    if not topic.is_system:
        # User topics: require owner or reader access
        stmt = select(TopicAccess).where(
            TopicAccess.topic_id == document.topic_id,
            TopicAccess.user_id == user_uuid
        )
        result = await db.execute(stmt)
        access = result.scalar_one_or_none()
        
        if not access:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access to topic required to download documents"
            )
    # System topics are accessible to all users for reading
    
    # Generate presigned URL
    try:
        object_name = document.storage_url.split('/')[-1]
        download_url = await minio_service.get_presigned_url(
            bucket_name=str(document.topic_id),
            object_name=object_name
        )
        
        logger.info(f"Download URL generated for document: {doc_id}")
        return DocumentDownloadResponse(download_url=download_url)
        
    except Exception as e:
        logger.error(f"Failed to generate download URL: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to generate download URL"
        )
