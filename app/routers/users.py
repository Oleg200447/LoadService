"""User access management endpoints."""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
import uuid
from typing import List

from app.database import get_db
from app.dependencies import get_jwt_data
from app.schemas import JWTData, UserAccessItem, AddUserToTopic
from app.models import Topic, TopicAccess, User
from app.utils.logger import logger

router = APIRouter(prefix="/topics/{topic_id}/users", tags=["User Access"])


@router.get("", response_model=List[UserAccessItem])
async def get_topic_users(
    topic_id: str,
    jwt_data: JWTData = Depends(get_jwt_data),
    db: AsyncSession = Depends(get_db)
):
    """
    Get list of users with access to a topic.
    
    - **topic_id**: ID of the topic
    
    Returns all users who have access to the topic with their roles.
    """
    logger.info(f"Get topic users request from user {jwt_data.sub} for topic {topic_id}")
    
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
    
    # Get all users with access to this topic
    stmt = (
        select(TopicAccess, User)
        .join(User, User.id == TopicAccess.user_id)
        .where(TopicAccess.topic_id == topic_uuid)
    )
    result = await db.execute(stmt)
    access_records = result.all()
    
    users_list = [
        UserAccessItem(
            user_id=access.user_id,
            username=user.username,
            role=access.role
        )
        for access, user in access_records
    ]
    
    logger.info(f"Found {len(users_list)} users with access to topic {topic_id}")
    return users_list


@router.post("", status_code=status.HTTP_201_CREATED)
async def add_user_to_topic(
    topic_id: str,
    user_data: AddUserToTopic,
    jwt_data: JWTData = Depends(get_jwt_data),
    db: AsyncSession = Depends(get_db)
):
    """
    Add a user to a topic by nickname.
    
    - **topic_id**: ID of the topic
    - **nickname**: Nickname of the user to add
    - **role**: Role to assign (owner or reader)
    
    Only topic owners can add users.
    """
    logger.info(
        f"Add user request from {jwt_data.sub} for topic {topic_id}: "
        f"{user_data.nickname} as {user_data.role}"
    )
    
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
            detail="Cannot add users to system topics"
        )
    
    # Check if requesting user is owner
    requester_uuid = uuid.UUID(jwt_data.sub)
    stmt = select(TopicAccess).where(
        TopicAccess.topic_id == topic_uuid,
        TopicAccess.user_id == requester_uuid,
        TopicAccess.role == "owner"
    )
    result = await db.execute(stmt)
    requester_access = result.scalar_one_or_none()
    
    if not requester_access:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only topic owners can add users"
        )
    
    # Find user by nickname
    stmt = select(User).where(User.username == user_data.nickname)
    result = await db.execute(stmt)
    target_user = result.scalar_one_or_none()
    
    if not target_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User with nickname '{user_data.nickname}' not found"
        )
    
    # Check if user already has access
    stmt = select(TopicAccess).where(
        TopicAccess.topic_id == topic_uuid,
        TopicAccess.user_id == target_user.id
    )
    result = await db.execute(stmt)
    existing_access = result.scalar_one_or_none()
    
    if existing_access:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"User '{user_data.nickname}' already has access to this topic"
        )
    
    # Add user to topic
    topic_access = TopicAccess(
        user_id=target_user.id,
        topic_id=topic_uuid,
        role=user_data.role
    )
    db.add(topic_access)
    await db.commit()
    
    logger.info(
        f"User {user_data.nickname} added to topic {topic_id} with role {user_data.role}"
    )
    return {"message": "User added successfully"}


@router.delete("/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_user_from_topic(
    topic_id: str,
    user_id: str,
    jwt_data: JWTData = Depends(get_jwt_data),
    db: AsyncSession = Depends(get_db)
):
    """
    Remove a user from a topic.
    
    - **topic_id**: ID of the topic
    - **user_id**: ID of the user to remove
    
    Only topic owners can remove users.
    """
    logger.info(
        f"Remove user request from {jwt_data.sub} for topic {topic_id}: user {user_id}"
    )
    
    try:
        topic_uuid = uuid.UUID(topic_id)
        target_user_uuid = uuid.UUID(user_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid topic_id or user_id format"
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
            detail="Cannot remove users from system topics"
        )
    
    # Check if requesting user is owner
    requester_uuid = uuid.UUID(jwt_data.sub)
    stmt = select(TopicAccess).where(
        TopicAccess.topic_id == topic_uuid,
        TopicAccess.user_id == requester_uuid,
        TopicAccess.role == "owner"
    )
    result = await db.execute(stmt)
    requester_access = result.scalar_one_or_none()
    
    if not requester_access:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only topic owners can remove users"
        )
    
    # Get target user's access
    stmt = select(TopicAccess).where(
        TopicAccess.topic_id == topic_uuid,
        TopicAccess.user_id == target_user_uuid
    )
    result = await db.execute(stmt)
    target_access = result.scalar_one_or_none()
    
    if not target_access:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User does not have access to this topic"
        )
    
    # Prevent removing the last owner
    if target_access.role == "owner":
        stmt = select(TopicAccess).where(
            TopicAccess.topic_id == topic_uuid,
            TopicAccess.role == "owner"
        )
        result = await db.execute(stmt)
        owner_count = len(result.scalars().all())
        
        if owner_count <= 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot remove the last owner from the topic"
            )
    
    # Remove user access
    await db.delete(target_access)
    await db.commit()
    
    logger.info(f"User {user_id} removed from topic {topic_id}")
    return None
