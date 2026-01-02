"""Main FastAPI application."""
from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.config import settings
from app.routers import documents, topics, users
from app.services.kafka_producer import kafka_producer
from app.services.kafka_consumer import kafka_consumer
from app.utils.logger import logger


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage application lifespan events.
    Start and stop Kafka services on application startup/shutdown.
    """
    # Startup
    logger.info("Starting LoadService application...")
    
    try:
        # Start Kafka producer
        await kafka_producer.start()
        logger.info("Kafka producer started successfully")
    except Exception as e:
        logger.error(f"Failed to start Kafka producer: {e}")
    
    try:
        # Start Kafka consumers
        await kafka_consumer.start()
        logger.info("Kafka consumers started successfully")
    except Exception as e:
        logger.error(f"Failed to start Kafka consumers: {e}")
    
    logger.info("LoadService application started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down LoadService application...")
    
    try:
        await kafka_producer.stop()
        logger.info("Kafka producer stopped")
    except Exception as e:
        logger.error(f"Error stopping Kafka producer: {e}")
    
    try:
        await kafka_consumer.stop()
        logger.info("Kafka consumers stopped")
    except Exception as e:
        logger.error(f"Error stopping Kafka consumers: {e}")
    
    logger.info("LoadService application shutdown complete")


# Create FastAPI application
app = FastAPI(
    title="LoadService API",
    description="Async service for document and topic management with MinIO and Kafka integration",
    version="1.0.0",
    lifespan=lifespan
)

# Include routers
app.include_router(documents.router)
app.include_router(topics.router)
app.include_router(users.router)


@app.get("/", tags=["Health"])
async def root():
    """
    Root endpoint - Health check.
    
    Returns a simple status message indicating the service is running.
    """
    return {
        "service": "LoadService",
        "status": "running",
        "version": "1.0.0"
    }


@app.get("/health", tags=["Health"])
async def health_check():
    """
    Health check endpoint.
    
    Returns the health status of the service.
    """
    return {
        "status": "healthy",
        "service": "LoadService"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host=settings.APP_HOST,
        port=settings.APP_PORT,
        reload=False
    )
