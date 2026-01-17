"""MinIO service for object storage operations."""
from minio import Minio
from minio.error import S3Error
from io import BytesIO
import uuid
from app.config import settings
from app.utils.logger import logger


class MinIOService:
    """Service for managing MinIO operations."""
    
    def __init__(self):
        """Initialize MinIO client."""
        self.client = Minio(
            settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE
        )
        logger.info("MinIO client initialized")
    
    async def create_bucket(self, bucket_name: str) -> bool:
        """
        Create a new bucket in MinIO.
        
        Args:
            bucket_name: Name of the bucket to create
            
        Returns:
            bool: True if successful
            
        Raises:
            Exception: If bucket creation fails
        """
        try:
            # Check if bucket already exists
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                logger.info(f"Bucket created: {bucket_name}")
            else:
                logger.info(f"Bucket already exists: {bucket_name}")
            return True
        except S3Error as e:
            logger.error(f"Failed to create bucket {bucket_name}: {e}")
            raise Exception(f"Failed to create bucket: {e}")
    
    async def upload_file(
        self,
        bucket_name: str,
        object_name: str,
        file_data: bytes,
        content_type: str = "application/octet-stream"
    ) -> str:
        """
        Upload file to MinIO.
        
        Args:
            bucket_name: Bucket name (topic_id)
            object_name: Object name in bucket
            file_data: File content as bytes
            content_type: MIME type of the file
            
        Returns:
            str: Storage URL
            
        Raises:
            Exception: If upload fails
        """
        try:
            # Ensure bucket exists
            if not self.client.bucket_exists(bucket_name):
                await self.create_bucket(bucket_name)
            
            # Upload file
            file_stream = BytesIO(file_data)
            self.client.put_object(
                bucket_name,
                object_name,
                file_stream,
                length=len(file_data),
                content_type=content_type
            )
            
            storage_url = f"minio://{bucket_name}/{object_name}"
            logger.info(f"File uploaded: {storage_url}")
            return storage_url
            
        except S3Error as e:
            logger.error(f"Failed to upload file to {bucket_name}/{object_name}: {e}")
            raise Exception(f"Failed to upload file: {e}")
    
    async def delete_file(self, bucket_name: str, object_name: str) -> bool:
        """
        Delete file from MinIO.
        
        Args:
            bucket_name: Bucket name
            object_name: Object name in bucket
            
        Returns:
            bool: True if successful
        """
        try:
            self.client.remove_object(bucket_name, object_name)
            logger.info(f"File deleted: {bucket_name}/{object_name}")
            return True
        except S3Error as e:
            logger.error(f"Failed to delete file {bucket_name}/{object_name}: {e}")
            return False
    
    async def delete_bucket(self, bucket_name: str) -> bool:
        """
        Delete bucket and all its contents from MinIO.
        
        Args:
            bucket_name: Name of the bucket to delete
            
        Returns:
            bool: True if successful
        """
        try:
            # Check if bucket exists
            if not self.client.bucket_exists(bucket_name):
                logger.info(f"Bucket does not exist: {bucket_name}")
                return True
            
            # Delete all objects in the bucket
            objects = self.client.list_objects(bucket_name, recursive=True)
            for obj in objects:
                self.client.remove_object(bucket_name, obj.object_name)
            
            # Delete the bucket
            self.client.remove_bucket(bucket_name)
            logger.info(f"Bucket deleted: {bucket_name}")
            return True
            
        except S3Error as e:
            logger.error(f"Failed to delete bucket {bucket_name}: {e}")
            return False
    
    async def get_presigned_url(
        self,
        bucket_name: str,
        object_name: str
    ) -> str:
        """
        Generate presigned URL for downloading file.

        Args:
            bucket_name: Bucket name
            object_name: Object name in bucket

        Returns:
            str: Presigned URL

        Raises:
            Exception: If URL generation fails
        """
        try:
            from datetime import timedelta

            url = self.client.presigned_get_object(
                bucket_name,
                object_name,
                expires=timedelta(seconds=settings.PRESIGNED_URL_EXPIRY)
            )
            logger.info(f"Presigned URL generated for: {bucket_name}/{object_name}")
            return url

        except S3Error as e:
            logger.error(f"Failed to generate presigned URL for {bucket_name}/{object_name}: {e}")
            raise Exception(f"Failed to generate download URL: {e}")

    async def download_file(
        self,
        bucket_name: str,
        object_name: str
    ) -> bytes:
        """
        Download file content from MinIO.

        Args:
            bucket_name: Bucket name
            object_name: Object name in bucket

        Returns:
            bytes: File content

        Raises:
            Exception: If download fails
        """
        try:
            response = self.client.get_object(bucket_name, object_name)
            file_data = response.read()
            response.close()
            response.release_conn()
            logger.info(f"File downloaded: {bucket_name}/{object_name}")
            return file_data

        except S3Error as e:
            logger.error(f"Failed to download file {bucket_name}/{object_name}: {e}")
            raise Exception(f"Failed to download file: {e}")


# Singleton instance
minio_service = MinIOService()
