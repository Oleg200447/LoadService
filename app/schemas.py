"""Pydantic schemas for request/response validation."""
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
import uuid


# JWT Token Data
class JWTData(BaseModel):
    """JWT token data extracted from Authorization header."""
    sub: str  # user_id
    nickname: str
    role: str  # "user" or "system"
    iss: str
    exp: int


# Document Schemas
class DocumentUploadResponse(BaseModel):
    """Response for document upload."""
    id: uuid.UUID
    topic_id: uuid.UUID
    filename: str
    status: str
    created_at: datetime
    
    class Config:
        from_attributes = True


class DocumentStatusResponse(BaseModel):
    """Response for document status."""
    id: uuid.UUID
    filename: str
    status: str
    
    class Config:
        from_attributes = True


class DocumentListItem(BaseModel):
    """Document list item."""
    id: uuid.UUID
    filename: str
    status: str
    created_at: datetime
    
    class Config:
        from_attributes = True


class DocumentDownloadResponse(BaseModel):
    """Response for document download."""
    download_url: str


# Topic Schemas
class TopicCreate(BaseModel):
    """Request to create a new topic."""
    name: str = Field(..., min_length=1, max_length=255)


class TopicResponse(BaseModel):
    """Response for topic operations."""
    id: uuid.UUID
    name: str
    is_system: bool
    owner_id: Optional[uuid.UUID]
    created_at: datetime
    
    class Config:
        from_attributes = True


class TopicListItem(BaseModel):
    """Topic list item."""
    id: uuid.UUID
    name: str
    is_system: bool
    
    class Config:
        from_attributes = True


# User Access Schemas
class UserAccessItem(BaseModel):
    """User access information."""
    user_id: uuid.UUID
    username: str
    role: str  # "owner" or "reader"
    
    class Config:
        from_attributes = True


class AddUserToTopic(BaseModel):
    """Request to add user to topic."""
    nickname: str = Field(..., min_length=1)
    role: str = Field(..., pattern="^(owner|reader)$")


# Kafka Message Schemas
class DocumentKafkaMessage(BaseModel):
    """Kafka message for document operations."""
    doc_id: str
    doc_name: str
    user_id: str
    topic_name: str
    document_url: Optional[str] = None
    upload_time: str


class TopicKafkaMessage(BaseModel):
    """Kafka message for topic operations."""
    user_id: str
    topic_name: str
    upload_time: str


class DocumentStatusKafkaMessage(BaseModel):
    """Kafka message for document status update."""
    doc_id: str


class NewUserKafkaMessage(BaseModel):
    """Kafka message for new user creation."""
    id: str
    username: str
    created_at: str
