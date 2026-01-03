"""Kafka producer service for sending messages."""
import json
from datetime import datetime
from aiokafka import AIOKafkaProducer
from app.config import settings
from app.utils.logger import logger


class KafkaProducerService:
    """Service for producing messages to Kafka topics."""
    
    def __init__(self):
        """Initialize Kafka producer."""
        self.producer: AIOKafkaProducer | None = None
        self.bootstrap_servers = settings.KAFKA_BOOTSTRAP_SERVERS
    
    async def start(self):
        """Start the Kafka producer."""
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            await self.producer.start()
            logger.info("Kafka producer started")
        except Exception as e:
            logger.error(f"Failed to start Kafka producer: {e}")
            raise
    
    async def stop(self):
        """Stop the Kafka producer."""
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka producer stopped")
    
    async def send_document_message(
        self,
        doc_id: str,
        doc_name: str,
        user_id: str,
        topic_name: str,
        document_url: str,
        file_extension: str
    ):
        """
        Send document upload message to appropriate Kafka topic based on file type.
        
        Args:
            doc_id: Document ID
            user_id: User ID
            topic_name: Topic ID where document belongs
            document_url: MinIO storage URL
            file_extension: File extension (pdf, pptx, docx)
        """
        message = {
            "doc_id": doc_id,
            "user_id": user_id,
            "doc_name": doc_name,
            "topic_name": topic_name,
            "document_url": document_url,
            "upload_time": datetime.utcnow().isoformat() + "Z"
        }
        
        # Determine Kafka topic based on file extension
        kafka_topic_map = {
            ".pdf": settings.KAFKA_PDF_TOPIC,
            ".pptx": settings.KAFKA_PPTX_TOPIC,
            ".docx": settings.KAFKA_DOCX_TOPIC
        }
        
        kafka_topic = kafka_topic_map.get(file_extension.lower())
        if not kafka_topic:
            logger.warning(f"Unknown file extension: {file_extension}, skipping Kafka message")
            return
        
        try:
            await self.producer.send_and_wait(kafka_topic, message)
            logger.info(f"Document message sent to {kafka_topic}: {doc_id}")
        except Exception as e:
            logger.error(f"Failed to send document message to Kafka: {e}")
            raise
    
    async def send_delete_document_message(
        self,
        doc_id: str,
        user_id: str,
        topic_name: str
    ):
        """
        Send document deletion message to Kafka.
        
        Args:
            doc_id: Document ID
            user_id: User ID
            topic_name: Topic ID where document belongs
        """
        message = {
            "doc_id": doc_id,
            "user_id": user_id,
            "topic_name": topic_name,
            "upload_time": datetime.utcnow().isoformat() + "Z"
        }
        
        try:
            await self.producer.send_and_wait(settings.KAFKA_DELETE_DOC_TOPIC, message)
            logger.info(f"Delete document message sent: {doc_id}")
        except Exception as e:
            logger.error(f"Failed to send delete document message to Kafka: {e}")
            raise
    
    async def send_create_topic_message(
        self,
        user_id: str,
        topic_name: str
    ):
        """
        Send topic creation message to Kafka.
        
        Args:
            user_id: User ID who created the topic
            topic_name: Topic ID
        """
        message = {
            "user_id": user_id,
            "topic_name": topic_name,
            "upload_time": datetime.utcnow().isoformat() + "Z"
        }
        
        try:
            await self.producer.send_and_wait(settings.KAFKA_CREATE_TOPIC_TOPIC, message)
            logger.info(f"Create topic message sent: {topic_name}")
        except Exception as e:
            logger.error(f"Failed to send create topic message to Kafka: {e}")
            raise
    
    async def send_delete_topic_message(
        self,
        user_id: str,
        topic_name: str
    ):
        """
        Send topic deletion message to Kafka.
        
        Args:
            user_id: User ID who deleted the topic
            topic_name: Topic ID
        """
        message = {
            "user_id": user_id,
            "topic_name": topic_name,
            "upload_time": datetime.utcnow().isoformat() + "Z"
        }
        
        try:
            await self.producer.send_and_wait(settings.KAFKA_DELETE_TOPIC_TOPIC, message)
            logger.info(f"Delete topic message sent: {topic_name}")
        except Exception as e:
            logger.error(f"Failed to send delete topic message to Kafka: {e}")
            raise


# Singleton instance
kafka_producer = KafkaProducerService()
