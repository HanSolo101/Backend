"""MinIO object storage client wrapper for the Glynac platform.

Provides high-level operations for uploading Parquet files, listing objects,
downloading files, and managing bucket organization.
"""

import io
import json
import structlog
from minio import Minio
from minio.error import S3Error
from datetime import datetime
from typing import Optional

from shared.config import get_settings

logger = structlog.get_logger(__name__)


class MinIOClient:
    """High-level MinIO client for ingestion pipeline storage operations."""

    def __init__(self):
        settings = get_settings()
        self.client = Minio(
            endpoint=settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE,
        )
        self._ensure_buckets()

    def _ensure_buckets(self):
        """Create required buckets if they don't exist."""
        settings = get_settings()
        buckets = [
            settings.MINIO_SALESFORCE_BUCKET,
            settings.MINIO_HUBSPOT_BUCKET,
            settings.MINIO_SLACK_BUCKET,
            settings.MINIO_CHECKPOINT_BUCKET,
        ]
        for bucket in buckets:
            try:
                if not self.client.bucket_exists(bucket):
                    self.client.make_bucket(bucket)
                    logger.info("bucket_created", bucket=bucket)
            except S3Error as e:
                logger.warning("bucket_create_error", bucket=bucket, error=str(e))

    def upload_parquet(
        self,
        bucket: str,
        object_path: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> str:
        """Upload a Parquet file (or any binary data) to MinIO.

        Args:
            bucket: Target bucket name.
            object_path: Object key path (e.g., 'salesforce/accounts/org_001/2026-09-30/batch_001.parquet').
            data: Raw bytes to upload.
            content_type: MIME type of the uploaded object.

        Returns:
            The full object path that was written.
        """
        data_stream = io.BytesIO(data)
        data_length = len(data)

        self.client.put_object(
            bucket_name=bucket,
            object_name=object_path,
            data=data_stream,
            length=data_length,
            content_type=content_type,
        )
        logger.info(
            "object_uploaded",
            bucket=bucket,
            path=object_path,
            size_bytes=data_length,
        )
        return object_path

    def upload_json(self, bucket: str, object_path: str, data: dict) -> str:
        """Upload a JSON object to MinIO."""
        json_bytes = json.dumps(data, default=str).encode("utf-8")
        return self.upload_parquet(
            bucket, object_path, json_bytes, content_type="application/json"
        )

    def download_bytes(self, bucket: str, object_path: str) -> bytes:
        """Download an object as raw bytes."""
        response = self.client.get_object(bucket, object_path)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def download_json(self, bucket: str, object_path: str) -> dict:
        """Download and parse a JSON object."""
        raw = self.download_bytes(bucket, object_path)
        return json.loads(raw.decode("utf-8"))

    def list_objects(
        self, bucket: str, prefix: str = "", recursive: bool = True
    ) -> list[dict]:
        """List objects in a bucket with optional prefix filter.

        Returns:
            List of dicts with keys: name, size, last_modified, is_dir.
        """
        objects = []
        try:
            for obj in self.client.list_objects(
                bucket, prefix=prefix, recursive=recursive
            ):
                objects.append(
                    {
                        "name": obj.object_name,
                        "size": obj.size,
                        "last_modified": (
                            obj.last_modified.isoformat() if obj.last_modified else None
                        ),
                        "is_dir": obj.is_dir,
                    }
                )
        except S3Error as e:
            logger.error("list_objects_error", bucket=bucket, prefix=prefix, error=str(e))
        return objects

    def object_exists(self, bucket: str, object_path: str) -> bool:
        """Check if an object exists in MinIO."""
        try:
            self.client.stat_object(bucket, object_path)
            return True
        except S3Error:
            return False

    def delete_object(self, bucket: str, object_path: str):
        """Delete an object from MinIO."""
        self.client.remove_object(bucket, object_path)
        logger.info("object_deleted", bucket=bucket, path=object_path)

    def get_presigned_url(
        self, bucket: str, object_path: str, expires_hours: int = 1
    ) -> str:
        """Generate a presigned URL for temporary object access."""
        from datetime import timedelta

        return self.client.presigned_get_object(
            bucket, object_path, expires=timedelta(hours=expires_hours)
        )

    def get_bucket_stats(self, bucket: str) -> dict:
        """Get summary statistics for a bucket."""
        objects = self.list_objects(bucket)
        total_size = sum(o.get("size", 0) or 0 for o in objects)
        return {
            "bucket": bucket,
            "total_objects": len(objects),
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
        }


# Singleton instance
_client: Optional[MinIOClient] = None


def get_minio_client() -> MinIOClient:
    """Return singleton MinIO client instance."""
    global _client
    if _client is None:
        _client = MinIOClient()
    return _client
