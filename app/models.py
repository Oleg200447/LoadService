"""SQLAlchemy database models."""
from datetime import datetime
from sqlalchemy import Boolean, String, Text, ForeignKey, CheckConstraint, text
#from sqlalchemy.dialects.postgresql import UUID, TIMESTAMPTZ
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy import TIMESTAMP
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from typing import Optional, List
import uuid


class Base(DeclarativeBase):
    """Base class for all database models."""
    pass


class User(Base):
    """User model."""
    __tablename__ = "users"
    
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), 
        primary_key=True
    )
    username: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("now()")
    )
    
    # Relationships
    owned_topics: Mapped[List["Topic"]] = relationship(
        "Topic", 
        back_populates="owner",
        foreign_keys="Topic.owner_id"
    )
    topic_access: Mapped[List["TopicAccess"]] = relationship(
        "TopicAccess",
        back_populates="user"
    )
    uploaded_documents: Mapped[List["Document"]] = relationship(
        "Document",
        back_populates="uploader"
    )


class Topic(Base):
    """Topic model."""
    __tablename__ = "topics"
    
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()")
    )
    owner_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id"),
        nullable=True
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    is_system: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("now()")
    )
    
    # Relationships
    owner: Mapped[Optional["User"]] = relationship(
        "User",
        back_populates="owned_topics",
        foreign_keys=[owner_id]
    )
    topic_access: Mapped[List["TopicAccess"]] = relationship(
        "TopicAccess",
        back_populates="topic",
        cascade="all, delete-orphan"
    )
    documents: Mapped[List["Document"]] = relationship(
        "Document",
        back_populates="topic",
        cascade="all, delete-orphan"
    )
    
    __table_args__ = (
        CheckConstraint(
            "is_system OR owner_id IS NOT NULL",
            name="owner_required_for_user_topics"
        ),
    )


class TopicAccess(Base):
    """Topic access control model."""
    __tablename__ = "topic_access"
    
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id"),
        primary_key=True,
        nullable=False
    )
    topic_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("topics.id"),
        primary_key=True,
        nullable=False
    )
    role: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="owner"
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("now()")
    )
    
    # Relationships
    user: Mapped["User"] = relationship("User", back_populates="topic_access")
    topic: Mapped["Topic"] = relationship("Topic", back_populates="topic_access")


class Document(Base):
    """Document model."""
    __tablename__ = "documents"
    
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()")
    )
    topic_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("topics.id"),
        nullable=False
    )
    uploader_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id"),
        nullable=False
    )
    filename: Mapped[str] = mapped_column(Text, nullable=False)
    storage_url: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="pending"
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("now()")
    )
    
    # Relationships
    topic: Mapped["Topic"] = relationship("Topic", back_populates="documents")
    uploader: Mapped["User"] = relationship("User", back_populates="uploaded_documents")
