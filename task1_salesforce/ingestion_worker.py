"""Salesforce Ingestion Worker.

Orchestrates the end-to-end ingestion flow for each Salesforce object:
1. Create a Bulk API v2 query job
2. Wait for completion
3. Retrieve results
4. Convert to Parquet
5. Upload to MinIO
6. Load into ClickHouse

Supports parallel ingestion across multiple objects with checkpoint tracking.
"""

import time
import threading
import structlog
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from shared.config import get_settings
from shared.minio_client import get_minio_client
from shared.parquet_writer import records_to_parquet
from shared.checkpoint import CheckpointManager, PipelineStateManager
from task1_salesforce.bulk_api_client import SalesforceBulkClient, BulkJobError
from task1_salesforce.clickhouse_loader import SalesforceClickHouseLoader

logger = structlog.get_logger(__name__)


class IngestionJob:
    """Tracks the state of a single object ingestion job."""

    def __init__(self, object_name: str):
        self.object_name = object_name
        self.status = "pending"         # pending | running | completed | failed | paused
        self.job_id: Optional[str] = None
        self.records_extracted = 0
        self.parquet_path: Optional[str] = None
        self.clickhouse_rows = 0
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.error: Optional[str] = None
        self.duration_seconds: float = 0

    def to_dict(self) -> dict:
        return {
            "object_name": self.object_name,
            "status": self.status,
            "job_id": self.job_id,
            "records_extracted": self.records_extracted,
            "parquet_path": self.parquet_path,
            "clickhouse_rows": self.clickhouse_rows,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": round(self.duration_seconds, 2),
            "error": self.error,
        }


class SalesforceIngestionWorker:
    """Orchestrates Salesforce Bulk API ingestion across 10+ objects."""

    PIPELINE_NAME = "salesforce"

    def __init__(self):
        settings = get_settings()
        self.sf_client = SalesforceBulkClient()
        self.minio = get_minio_client()
        self.ch_loader = SalesforceClickHouseLoader()
        self.checkpoint = CheckpointManager(self.minio)
        self.state_manager = PipelineStateManager(self.minio)
        self.org_id = settings.DEFAULT_ORG_ID
        self.bucket = settings.MINIO_SALESFORCE_BUCKET

        # Job tracking
        self._jobs: dict[str, IngestionJob] = {}
        self._lock = threading.Lock()

    def _ingest_object(self, object_name: str) -> IngestionJob:
        """Ingest a single Salesforce object end-to-end."""
        job = IngestionJob(object_name)
        job.start_time = time.time()
        job.status = "running"

        with self._lock:
            self._jobs[object_name] = job

        try:
            # Check if pipeline is paused
            if self.state_manager.is_paused(self.PIPELINE_NAME):
                job.status = "paused"
                logger.info("[INGEST] Pipeline paused, skipping", object=object_name)
                return job

            # Step 1: Check checkpoint for resume
            checkpoint_state = self.checkpoint.load(self.PIPELINE_NAME, object_name)
            if checkpoint_state and checkpoint_state.get("completed"):
                logger.info("[INGEST] Already completed (checkpoint found), skipping", object=object_name)
                job.status = "completed"
                job.records_extracted = checkpoint_state.get("records_extracted", 0)
                return job

            logger.info(f"[INGEST] Starting ingestion for {object_name}")

            # Step 2: Create bulk query job
            bulk_job = self.sf_client.create_bulk_job(object_name)
            job.job_id = bulk_job["id"]

            # Step 3: Wait for job to complete
            final_status = self.sf_client.wait_for_job(job.job_id)
            logger.info(
                f"[INGEST] Job completed for {object_name}",
                records=final_status.get("numberRecordsProcessed", 0),
            )

            # Step 4: Retrieve results
            records = self.sf_client.get_results(job.job_id)
            job.records_extracted = len(records)

            if not records:
                logger.warning(f"[INGEST] No records returned for {object_name}")
                job.status = "completed"
                return job

            # Step 5: Convert to Parquet and upload to MinIO
            parquet_bytes = records_to_parquet(records)
            date_str = datetime.utcnow().strftime("%Y-%m-%d")
            parquet_path = f"{object_name}/{self.org_id}/{date_str}/data.parquet"

            self.minio.upload_parquet(self.bucket, parquet_path, parquet_bytes)
            job.parquet_path = f"{self.bucket}/{parquet_path}"

            logger.info(
                f"[INGEST] Parquet uploaded to MinIO",
                path=parquet_path,
                size_bytes=len(parquet_bytes),
            )

            # Step 6: Load into ClickHouse
            table_name = f"bronze_salesforce_{object_name.lower()}"
            self.ch_loader.create_table_for_object(object_name, records)
            self.ch_loader.insert_records(table_name, records)
            job.clickhouse_rows = len(records)

            logger.info(
                f"[INGEST] ClickHouse loaded",
                table=table_name,
                rows=len(records),
            )

            # Step 7: Save checkpoint
            job.status = "completed"
            job.end_time = time.time()
            job.duration_seconds = job.end_time - job.start_time

            self.checkpoint.save(
                self.PIPELINE_NAME,
                object_name,
                {
                    "completed": True,
                    "records_extracted": job.records_extracted,
                    "parquet_path": job.parquet_path,
                    "timestamp": datetime.utcnow().isoformat(),
                },
            )

            logger.info(
                f"[INGEST] ✅ Ingestion complete for {object_name}",
                records=job.records_extracted,
                duration=f"{job.duration_seconds:.1f}s",
            )

        except BulkJobError as e:
            job.status = "failed"
            job.error = str(e)
            job.end_time = time.time()
            job.duration_seconds = job.end_time - job.start_time
            logger.error(f"[INGEST] ❌ Bulk job error for {object_name}", error=str(e))

        except Exception as e:
            job.status = "failed"
            job.error = str(e)
            job.end_time = time.time()
            job.duration_seconds = job.end_time - (job.start_time or time.time())
            logger.error(f"[INGEST] ❌ Ingestion failed for {object_name}", error=str(e))

        return job

    def run_all(
        self,
        objects: list[str] = None,
        max_workers: int = None,
    ) -> dict[str, IngestionJob]:
        """Run ingestion for all (or specified) Salesforce objects in parallel.

        Args:
            objects: List of object names to ingest. Defaults to all 12 objects.
            max_workers: Max parallel threads. Defaults to config setting.

        Returns:
            Dict mapping object_name → IngestionJob with final status.
        """
        settings = get_settings()
        if objects is None:
            objects = SalesforceBulkClient.OBJECTS
        if max_workers is None:
            max_workers = settings.DEFAULT_PARALLEL_WORKERS

        # Authenticate once
        self.sf_client.authenticate()

        # Set pipeline state to running
        self.state_manager.set_state(self.PIPELINE_NAME, PipelineStateManager.RUNNING)

        logger.info(
            f"[INGEST] Starting parallel ingestion",
            objects=len(objects),
            workers=max_workers,
        )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._ingest_object, obj): obj
                for obj in objects
            }

            for future in as_completed(futures):
                obj_name = futures[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"[INGEST] Thread exception for {obj_name}", error=str(e))

        # Set pipeline state to completed
        self.state_manager.set_state(self.PIPELINE_NAME, PipelineStateManager.COMPLETED)

        # Summary
        completed = sum(1 for j in self._jobs.values() if j.status == "completed")
        failed = sum(1 for j in self._jobs.values() if j.status == "failed")
        total_records = sum(j.records_extracted for j in self._jobs.values())

        logger.info(
            f"[INGEST] ✅ All ingestion complete",
            total_objects=len(objects),
            completed=completed,
            failed=failed,
            total_records=total_records,
        )

        return dict(self._jobs)

    def run_single(self, object_name: str) -> IngestionJob:
        """Run ingestion for a single Salesforce object."""
        self.sf_client.authenticate()
        self.state_manager.set_state(self.PIPELINE_NAME, PipelineStateManager.RUNNING)
        job = self._ingest_object(object_name)
        return job

    def pause(self):
        """Pause the ingestion pipeline."""
        self.state_manager.set_state(self.PIPELINE_NAME, PipelineStateManager.PAUSED)
        logger.info("[INGEST] Pipeline PAUSED")

    def resume(self):
        """Resume the ingestion pipeline."""
        self.state_manager.set_state(self.PIPELINE_NAME, PipelineStateManager.RUNNING)
        logger.info("[INGEST] Pipeline RESUMED")

    def get_status(self) -> dict:
        """Get current status of all ingestion jobs."""
        return {
            "pipeline_state": self.state_manager.get_state(self.PIPELINE_NAME),
            "jobs": {name: job.to_dict() for name, job in self._jobs.items()},
            "summary": {
                "total": len(self._jobs),
                "completed": sum(1 for j in self._jobs.values() if j.status == "completed"),
                "failed": sum(1 for j in self._jobs.values() if j.status == "failed"),
                "running": sum(1 for j in self._jobs.values() if j.status == "running"),
                "pending": sum(1 for j in self._jobs.values() if j.status == "pending"),
                "total_records": sum(j.records_extracted for j in self._jobs.values()),
            },
        }

    def reset_checkpoints(self):
        """Clear all checkpoints to allow full re-ingestion."""
        for obj in SalesforceBulkClient.OBJECTS:
            self.checkpoint.clear(self.PIPELINE_NAME, obj)
        logger.info("[INGEST] All checkpoints cleared")
