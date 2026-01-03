"""Application configuration settings."""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Database Configuration
    POSTGRES_HOST: str = "database_load_service"
    POSTGRES_PORT: int = 5432
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    POSTGRES_DB: str = "load_service_db"
    
    # MinIO Configuration
    MINIO_ENDPOINT: str = "minio:9000"
    MINIO_ENDPOINT_UI: str = "localhost:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    MINIO_SECURE: bool = False
    PRESIGNED_URL_EXPIRY: int = 3600
    
    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"
    KAFKA_PDF_TOPIC: str = "pdf_documents"
    KAFKA_PPTX_TOPIC: str = "pptx_documents"
    KAFKA_DOCX_TOPIC: str = "docx_documents"
    KAFKA_DELETE_DOC_TOPIC: str = "delete_documents"
    KAFKA_CREATE_TOPIC_TOPIC: str = "create_topic"
    KAFKA_DELETE_TOPIC_TOPIC: str = "delete_topic"
    KAFKA_STATUS_CONSUMER_TOPIC: str = "document_status_updates"
    KAFKA_USER_CONSUMER_TOPIC: str = "new_users"
    
    # Application Configuration
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000
    LOG_LEVEL: str = "INFO"
    
    @property
    def database_url(self) -> str:
        """Get async PostgreSQL database URL."""
        return (
            f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
