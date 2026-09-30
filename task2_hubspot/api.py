"""HubSpot Pipeline API — FastAPI backend for pipeline management.

Provides REST endpoints for:
- Running the dlt pipeline (all or single resource)
- Monitoring pipeline status and progress
- Pausing / Resuming the pipeline
- Viewing ClickHouse tables and analytical views
- Browsing MinIO Parquet files
"""

import threading
import structlog
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

from shared.config import get_settings
from shared.minio_client import get_minio_client
from shared.clickhouse_client import get_clickhouse_client
from task2_hubspot.hubspot_pipeline import HubSpotPipeline, HUBSPOT_RESOURCES
from task2_hubspot.clickhouse_views import HubSpotClickHouseViews

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/hubspot", tags=["HubSpot"])

_pipeline: Optional[HubSpotPipeline] = None
_ch_views: Optional[HubSpotClickHouseViews] = None
_run_thread: Optional[threading.Thread] = None


def _get_pipeline() -> HubSpotPipeline:
    global _pipeline
    if _pipeline is None:
        _pipeline = HubSpotPipeline()
    return _pipeline


def _get_ch_views() -> HubSpotClickHouseViews:
    global _ch_views
    if _ch_views is None:
        _ch_views = HubSpotClickHouseViews()
    return _ch_views


class PipelineRunRequest(BaseModel):
    resources: Optional[list[str]] = None
    max_workers: int = 4


class SingleResourceRequest(BaseModel):
    resource: str


@router.post("/run")
async def run_pipeline(request: PipelineRunRequest):
    """Run the HubSpot dlt pipeline for all or specified resources."""
    global _run_thread
    pipeline = _get_pipeline()
    ch_views = _get_ch_views()

    if _run_thread and _run_thread.is_alive():
        return {"status": "already_running", "message": "Pipeline already running"}

    resources = request.resources or HUBSPOT_RESOURCES

    def _run():
        try:
            # Create ClickHouse tables first
            ch_views.create_all_tables()

            # Run extraction
            result = pipeline.run_all(resources=resources, max_workers=request.max_workers)

            # Load into ClickHouse
            minio = get_minio_client()
            settings = get_settings()
            for resource in resources:
                job = pipeline._jobs.get(resource, {})
                if job.get("status") == "completed" and job.get("records_extracted", 0) > 0:
                    # Re-extract and load into ClickHouse
                    try:
                        from task2_hubspot.hubspot_pipeline import HubSpotExtractor
                        extractor = HubSpotExtractor()
                        all_records = []
                        for batch in extractor.extract_resource(resource):
                            flat_records = [pipeline._flatten_record(r) for r in batch]
                            all_records.extend(flat_records)
                        if all_records:
                            ch_views.insert_records(resource, all_records)
                        extractor.close()
                    except Exception as e:
                        logger.warning(f"[HUBSPOT API] CH load for {resource} deferred", error=str(e))

            # Create analytical views
            ch_views.create_analytical_views()

        except Exception as e:
            logger.error("[HUBSPOT API] Pipeline run failed", error=str(e))

    _run_thread = threading.Thread(target=_run, daemon=True)
    _run_thread.start()

    return {
        "status": "started",
        "resources": resources,
        "max_workers": request.max_workers,
    }


@router.post("/run/single")
async def run_single_resource(request: SingleResourceRequest):
    """Run pipeline for a single HubSpot resource."""
    pipeline = _get_pipeline()
    ch_views = _get_ch_views()

    if request.resource not in HUBSPOT_RESOURCES:
        raise HTTPException(status_code=400, detail=f"Unknown resource: {request.resource}")

    def _run():
        try:
            ch_views.create_all_tables()
            pipeline.run_single(request.resource)
        except Exception as e:
            logger.error(f"[HUBSPOT API] Single run failed for {request.resource}", error=str(e))

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()

    return {"status": "started", "resource": request.resource}


@router.get("/status")
async def get_status():
    """Get current pipeline status."""
    pipeline = _get_pipeline()
    return pipeline.get_status()


@router.post("/pause")
async def pause_pipeline():
    """Pause the HubSpot pipeline."""
    pipeline = _get_pipeline()
    pipeline.pause()
    return {"status": "paused"}


@router.post("/resume")
async def resume_pipeline():
    """Resume the HubSpot pipeline."""
    pipeline = _get_pipeline()
    pipeline.resume()
    return {"status": "resumed"}


@router.post("/reset")
async def reset_checkpoints():
    """Clear all HubSpot checkpoints."""
    pipeline = _get_pipeline()
    pipeline.reset_checkpoints()
    return {"status": "reset"}


@router.get("/resources")
async def list_resources():
    """List available HubSpot resources."""
    return {"resources": HUBSPOT_RESOURCES}


@router.get("/clickhouse/tables")
async def clickhouse_tables():
    """Get ClickHouse table statistics for HubSpot."""
    try:
        ch_views = _get_ch_views()
        stats = ch_views.get_table_stats()
        return {"tables": stats}
    except Exception as e:
        return {"tables": [], "error": str(e)}


@router.get("/clickhouse/views")
async def clickhouse_views():
    """List ClickHouse analytical views for HubSpot."""
    try:
        ch = get_clickhouse_client()
        views = ch.list_views()
        hs_views = [v for v in views if v.startswith("v_hubspot_")]
        return {"views": hs_views}
    except Exception as e:
        return {"views": [], "error": str(e)}


@router.get("/clickhouse/query")
async def clickhouse_query(table: str, limit: int = 100):
    """Query a ClickHouse HubSpot table or view."""
    try:
        ch = get_clickhouse_client()
        if not table.startswith(("bronze_hubspot_", "v_hubspot_")):
            raise HTTPException(status_code=400, detail="Only HubSpot tables/views allowed")
        results = ch.query(f"SELECT * FROM `{table}` LIMIT {limit}")
        return {"table": table, "rows": results, "count": len(results)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/minio/files")
async def minio_files(prefix: str = ""):
    """Browse MinIO files in the HubSpot bucket."""
    try:
        minio = get_minio_client()
        settings = get_settings()
        objects = minio.list_objects(settings.MINIO_HUBSPOT_BUCKET, prefix=prefix)
        stats = minio.get_bucket_stats(settings.MINIO_HUBSPOT_BUCKET)
        return {"files": objects, "stats": stats}
    except Exception as e:
        return {"files": [], "stats": {}, "error": str(e)}
