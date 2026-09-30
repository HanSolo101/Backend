"""Salesforce Ingestion API — FastAPI backend for the monitoring console.

Provides REST endpoints for:
- Triggering bulk sync jobs (all or single object)
- Monitoring job status and progress
- Pausing / resuming pipeline
- Viewing ClickHouse table stats
- Browsing MinIO file objects
"""

import threading
import structlog
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

from shared.config import get_settings
from shared.minio_client import get_minio_client
from shared.clickhouse_client import get_clickhouse_client
from task1_salesforce.ingestion_worker import SalesforceIngestionWorker
from task1_salesforce.bulk_api_client import SalesforceBulkClient

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/salesforce", tags=["Salesforce"])

# Singleton worker instance
_worker: Optional[SalesforceIngestionWorker] = None
_sync_thread: Optional[threading.Thread] = None


def _get_worker() -> SalesforceIngestionWorker:
    global _worker
    if _worker is None:
        _worker = SalesforceIngestionWorker()
    return _worker


class SyncRequest(BaseModel):
    objects: Optional[list[str]] = None
    max_workers: int = 4


class SingleSyncRequest(BaseModel):
    object_name: str


@router.post("/sync")
async def trigger_bulk_sync(request: SyncRequest):
    """Trigger a bulk sync of Salesforce objects.

    Runs in a background thread to avoid blocking the API.
    """
    global _sync_thread
    worker = _get_worker()

    if _sync_thread and _sync_thread.is_alive():
        return {
            "status": "already_running",
            "message": "A sync is already in progress. Check /status for details.",
        }

    objects = request.objects or SalesforceBulkClient.OBJECTS

    def _run():
        try:
            worker.run_all(objects=objects, max_workers=request.max_workers)
        except Exception as e:
            logger.error("[API] Sync thread failed", error=str(e))

    _sync_thread = threading.Thread(target=_run, daemon=True)
    _sync_thread.start()

    return {
        "status": "started",
        "objects": objects,
        "max_workers": request.max_workers,
        "message": f"Bulk sync started for {len(objects)} objects",
    }


@router.post("/sync/single")
async def trigger_single_sync(request: SingleSyncRequest):
    """Trigger sync for a single Salesforce object."""
    worker = _get_worker()

    if request.object_name not in SalesforceBulkClient.OBJECTS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown object: {request.object_name}. Available: {SalesforceBulkClient.OBJECTS}",
        )

    def _run():
        try:
            worker.run_single(request.object_name)
        except Exception as e:
            logger.error("[API] Single sync failed", error=str(e))

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()

    return {
        "status": "started",
        "object_name": request.object_name,
    }


@router.get("/status")
async def get_status():
    """Get current ingestion pipeline status and all job progress."""
    worker = _get_worker()
    return worker.get_status()


@router.post("/pause")
async def pause_pipeline():
    """Pause the ingestion pipeline."""
    worker = _get_worker()
    worker.pause()
    return {"status": "paused", "message": "Pipeline paused. New jobs will not start."}


@router.post("/resume")
async def resume_pipeline():
    """Resume the ingestion pipeline."""
    worker = _get_worker()
    worker.resume()
    return {"status": "resumed", "message": "Pipeline resumed."}


@router.post("/reset")
async def reset_checkpoints():
    """Clear all checkpoints to allow full re-ingestion."""
    worker = _get_worker()
    worker.reset_checkpoints()
    return {"status": "reset", "message": "All checkpoints cleared."}


@router.get("/objects")
async def list_objects():
    """List all available Salesforce objects."""
    return {
        "objects": SalesforceBulkClient.OBJECTS,
        "count": len(SalesforceBulkClient.OBJECTS),
    }


@router.get("/clickhouse/tables")
async def clickhouse_tables():
    """Get ClickHouse table statistics for Salesforce objects."""
    try:
        ch = get_clickhouse_client()
        tables = ch.list_tables()
        sf_tables = [t for t in tables if t.startswith("bronze_salesforce_")]

        stats = []
        for table in sf_tables:
            try:
                count = ch.get_table_row_count(table)
                columns = ch.get_table_columns(table)
                stats.append({
                    "table": table,
                    "rows": count,
                    "columns": len(columns),
                    "column_details": columns,
                })
            except Exception:
                stats.append({"table": table, "rows": 0, "columns": 0, "column_details": []})

        return {"tables": stats, "count": len(stats)}
    except Exception as e:
        return {"tables": [], "count": 0, "error": str(e)}


@router.get("/clickhouse/views")
async def clickhouse_views():
    """List ClickHouse analytical views for Salesforce data."""
    try:
        ch = get_clickhouse_client()
        views = ch.list_views()
        sf_views = [v for v in views if v.startswith("v_salesforce_")]
        return {"views": sf_views, "count": len(sf_views)}
    except Exception as e:
        return {"views": [], "count": 0, "error": str(e)}


@router.get("/clickhouse/query")
async def clickhouse_query(table: str, limit: int = 100):
    """Query a ClickHouse table and return results."""
    try:
        ch = get_clickhouse_client()
        if not table.startswith(("bronze_salesforce_", "v_salesforce_")):
            raise HTTPException(status_code=400, detail="Only Salesforce tables/views allowed")
        results = ch.query(f"SELECT * FROM `{table}` LIMIT {limit}")
        return {"table": table, "rows": results, "count": len(results)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/minio/files")
async def minio_files(prefix: str = ""):
    """Browse MinIO files in the Salesforce bucket."""
    try:
        minio = get_minio_client()
        settings = get_settings()
        objects = minio.list_objects(settings.MINIO_SALESFORCE_BUCKET, prefix=prefix)
        stats = minio.get_bucket_stats(settings.MINIO_SALESFORCE_BUCKET)
        return {"files": objects, "stats": stats}
    except Exception as e:
        return {"files": [], "stats": {}, "error": str(e)}


@router.get("/minio/stats")
async def minio_stats():
    """Get MinIO bucket statistics for all ingestion buckets."""
    try:
        minio = get_minio_client()
        settings = get_settings()
        buckets = [settings.MINIO_SALESFORCE_BUCKET, settings.MINIO_HUBSPOT_BUCKET, settings.MINIO_SLACK_BUCKET]
        stats = {}
        for bucket in buckets:
            stats[bucket] = minio.get_bucket_stats(bucket)
        return stats
    except Exception as e:
        return {"error": str(e)}
