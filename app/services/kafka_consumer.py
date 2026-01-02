"""Kafka consumer service for listening to messages."""
import json
import asyncio
import uuid
from datetime import datetime
from aiokafka import AIOKafkaConsumer
from sqlalchemy import select, update
from app.config import settings
from app.database import AsyncSessionLocal
from app.models import Document, User
from app.utils.logger import logger


class KafkaConsumerService:
    """Service for consuming messages from Kafka topics."""
    
    def __init__(self):
        """Initialize Kafka consumers."""
        self.status_consumer: AIOKafkaConsumer | None = None
        self.user_consumer: AIOKafkaConsumer | None = None
        self.bootstrap_servers = settings.KAFKA_BOOTSTRAP_SERVERS
        self.running = False
    
    async def start(self):
        """Start all Kafka consumers."""
        self.running = True
        
        # Start document status consumer
        asyncio.create_task(self._consume_document_status())
        
        # Start new user consumer
        asyncio.create_task(self._consume_new_users())
        
        logger.info("Kafka consumers started")
    
    async def stop(self):
        """Stop all Kafka consumers."""
        self.running = False
        
        if self.status_consumer:
            await self.status_consumer.stop()
        
        if self.user_consumer:
            await self.user_consumer.stop()
        
        logger.info("Kafka consumers stopped")
    
    async def _consume_document_status(self):
        """
        Consume document status update messages.
        Updates document status to 'success' when processing is complete.
        """
        try:
            self.status_consumer = AIOKafkaConsumer(
                settings.KAFKA_STATUS_CONSUMER_TOPIC,
                bootstrap_servers=self.bootstrap_servers,
                group_id="load_service_status_group",
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest'
            )
            
            await self.status_consumer.start()
            logger.info(f"Started consuming from {settings.KAFKA_STATUS_CONSUMER_TOPIC}")
            
            async for message in self.status_consumer:
                if not self.running:
                    break
                
                try:
                    data = message.value
                    doc_id = data.get("doc_id")
                    
                    if not doc_id:
                        logger.warning("Received status message without doc_id")
                        continue
                    
                    # Update document status in database
                    async with AsyncSessionLocal() as session:
                        stmt = (
                            update(Document)
                            .where(Document.id == uuid.UUID(doc_id))
                            .values(status="success")
                        )
                        result = await session.execute(stmt)
                        await session.commit()
                        
                        if result.rowcount > 0:
                            logger.info(f"Document status updated to success: {doc_id}")
                        else:
                            logger.warning(f"Document not found for status update: {doc_id}")
                
                except Exception as e:
                    logger.error(f"Error processing document status message: {e}")
                    continue
        
        except Exception as e:
            logger.error(f"Error in document status consumer: {e}")
        finally:
            if self.status_consumer:
                await self.status_consumer.stop()
    
    async def _consume_new_users(self):
        """
        Consume new user creation messages.
        Creates user records in the database.
        """
        try:
            self.user_consumer = AIOKafkaConsumer(
                settings.KAFKA_USER_CONSUMER_TOPIC,
                bootstrap_servers=self.bootstrap_servers,
                group_id="load_service_user_group",
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest'
            )
            
            await self.user_consumer.start()
            logger.info(f"Started consuming from {settings.KAFKA_USER_CONSUMER_TOPIC}")
            
            async for message in self.user_consumer:
                if not self.running:
                    break
                
                try:
                    data = message.value
                    user_id = data.get("id")
                    username = data.get("username")
                    created_at_str = data.get("created_at")
                    
                    if not user_id or not username:
                        logger.warning("Received user message with missing fields")
                        continue
                    
                    # Parse created_at if provided, otherwise use current time
                    if created_at_str:
                        try:
                            created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                        except Exception:
                            created_at = datetime.utcnow()
                    else:
                        created_at = datetime.utcnow()
                    
                    # Create user in database
                    async with AsyncSessionLocal() as session:
                        # Check if user already exists
                        stmt = select(User).where(User.id == uuid.UUID(user_id))
                        result = await session.execute(stmt)
                        existing_user = result.scalar_one_or_none()
                        
                        if existing_user:
                            logger.info(f"User already exists: {user_id}")
                            continue
                        
                        # Create new user
                        new_user = User(
                            id=uuid.UUID(user_id),
                            username=username,
                            created_at=created_at
                        )
                        session.add(new_user)
                        await session.commit()
                        
                        logger.info(f"New user created: {user_id} ({username})")
                
                except Exception as e:
                    logger.error(f"Error processing new user message: {e}")
                    continue
        
        except Exception as e:
            logger.error(f"Error in new user consumer: {e}")
        finally:
            if self.user_consumer:
                await self.user_consumer.stop()


# Singleton instance
kafka_consumer = KafkaConsumerService()
