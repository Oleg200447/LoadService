"""Topic management endpoints."""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, or_
import uuid
from typing import List

from app.database import get_db
from app.dependencies import get_jwt_data
from app.schemas import (
    JWTData,
    TopicCreate,
    TopicResponse,
    TopicListItem,
    DocumentListItem
)
from app.models import Topic, TopicAccess, Document, User
from app.services.minio_service import minio_service
from app.services.kafka_producer import kafka_producer
from app.utils.logger import logger

router = APIRouter(prefix="/topics", tags=["Topics"])


@router.post("", response_model=TopicResponse, status_code=status.HTTP_201_CREATED)
async def create_topic(
    topic_data: TopicCreate,
    jwt_data: JWTData = Depends(get_jwt_data),
    db: AsyncSession = Depends(get_db)
):
    """
    Create a new topic.
    
    - **name**: Name of the topic
    
    The creator automatically becomes the owner of the topic.
    """
    logger.info(f"Create topic request from user {jwt_data.sub}: {topic_data.name}")
    
    user_uuid = uuid.UUID(jwt_data.sub)
    
    # Check if user exists
    stmt = select(User).where(User.id == user_uuid)
    result = await db.execute(stmt)
    user = result.scalar_one_or_none()
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found. Please ensure user is registered."
        )
    
    # Create topic
    topic = Topic(
        name=topic_data.name,
        owner_id=user_uuid,
        is_system=False
    )
    db.add(topic)
    await db.flush()  # Get the topic ID
    
    # Add creator as owner in topic_access
    topic_access = TopicAccess(
        user_id=user_uuid,
        topic_id=topic.id,
        role="owner"
    )
    db.add(topic_access)
    await db.commit()
    await db.refresh(topic)
    
    # Create bucket in MinIO
    try:
        await minio_service.create_bucket(str(topic.id))
    except Exception as e:
        logger.error(f"Failed to create MinIO bucket: {e}")
        # Rollback database changes
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create storage bucket"
        )
    
    # Send message to Kafka
    try:
        await kafka_producer.send_create_topic_message(
            user_id=jwt_data.sub,
            topic_name=str(topic.id)
        )
    except Exception as e:
        logger.error(f"Failed to send Kafka message: {e}")
        # Don't fail the request if Kafka fails
    
    logger.info(f"Topic created successfully: {topic.id}")
    return topic


@router.delete("/{topic_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_topic(
    topic_id: str,
    jwt_data: JWTData = Depends(get_jwt_data),
    db: AsyncSession = Depends(get_db)
):
    """
    Delete a topic.
    
    - **topic_id**: ID of the topic to delete
    
    Only the owner can delete the topic. System topics cannot be deleted.
    """
    logger.info(f"Delete topic request from user {jwt_data.sub}: {topic_id}")
    
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
    
    # Check if system topic
    if topic.is_system:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="System topics cannot be deleted"
        )
    
    # Check if user is owner
    user_uuid = uuid.UUID(jwt_data.sub)
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
            detail="Only topic owner can delete the topic"
        )
    
    # Delete bucket from MinIO
    try:
        await minio_service.delete_bucket(str(topic_uuid))
    except Exception as e:
        logger.error(f"Failed to delete MinIO bucket: {e}")
        # Continue with database deletion even if MinIO fails
    
    # Delete from database (cascades to topic_access and documents)
    await db.delete(topic)
    await db.commit()
    
    # Send message to Kafka
    try:
        await kafka_producer.send_delete_topic_message(
            user_id=jwt_data.sub,
            topic_name=str(topic_uuid)
        )
    except Exception as e:
        logger.error(f"Failed to send Kafka delete message: {e}")
        # Don't fail the request if Kafka fails
    
    logger.info(f"Topic deleted successfully: {topic_id}")
    return None


@router.get("/editable", response_model=List[TopicListItem])
async def get_editable_topics(
    jwt_data: JWTData = Depends(get_jwt_data),
    db: AsyncSession = Depends(get_db)
):
    """
    Get list of topics available for editing.
    
    Returns all topics where the current user has owner access.
    """
    logger.info(f"Get editable topics request from user {jwt_data.sub}")
    
    user_uuid = uuid.UUID(jwt_data.sub)
    
    # Get topics where user is owner
    stmt = (
        select(Topic)
        .join(TopicAccess, TopicAccess.topic_id == Topic.id)
        .where(
            TopicAccess.user_id == user_uuid,
            TopicAccess.role == "owner"
        )
    )
    result = await db.execute(stmt)
    topics = result.scalars().all()
    
    logger.info(f"Found {len(topics)} editable topics for user {jwt_data.sub}")
    return [TopicListItem(id=t.id, name=t.name, is_system=t.is_system) for t in topics]


@router.get("/readable", response_model=List[TopicListItem])
async def get_readable_topics(
    jwt_data: JWTData = Depends(get_jwt_data),
    db: AsyncSession = Depends(get_db)
):
    """
    Get list of topics available for reading (search).
    
    Returns all topics where the user has any access (owner or reader) plus all system topics.
    """
    logger.info(f"Get readable topics request from user {jwt_data.sub}")
    
    user_uuid = uuid.UUID(jwt_data.sub)
    
    # Get all system topics and topics where user has access
    stmt = (
        select(Topic)
        .outerjoin(TopicAccess, TopicAccess.topic_id == Topic.id)
        .where(
            or_(
                Topic.is_system == True,
                TopicAccess.user_id == user_uuid
            )
        )
        .distinct()
    )
    result = await db.execute(stmt)
    topics = result.scalars().all()
    
    logger.info(f"Found {len(topics)} readable topics for user {jwt_data.sub}")
    return [TopicListItem(id=t.id, name=t.name, is_system=t.is_system) for t in topics]


@router.get("/{topic_id}/documents", response_model=List[DocumentListItem])
async def get_topic_documents(
    topic_id: str,
    jwt_data: JWTData = Depends(get_jwt_data),
    db: AsyncSession = Depends(get_db)
):
    """
    Get list of documents in a topic.
    
    - **topic_id**: ID of the topic
    
    Returns all documents in the specified topic.
    """
    logger.info(f"Get documents request from user {jwt_data.sub} for topic {topic_id}")
    
    try:
        topic_uuid = uuid.UUID(topic_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid topic_id format"
        )
    
    # Check if topic exists
    stmt = select(Topic).where(Topic.id == topic_uuid)
    result = await db.execute(stmt)
    topic = result.scalar_one_or_none()
    
    if not topic:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Topic not found"
        )
    
    # Get documents in topic
    stmt = select(Document).where(Document.topic_id == topic_uuid)
    result = await db.execute(stmt)
    documents = result.scalars().all()
    
    logger.info(f"Found {len(documents)} documents in topic {topic_id}")
    return [
        DocumentListItem(
            id=d.id,
            filename=d.filename,
            status=d.status,
            created_at=d.created_at
        )
        for d in documents
    ]
