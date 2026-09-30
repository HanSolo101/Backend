"""Slack Dual-Mode Ingestion API — FastAPI backend.

Provides REST endpoints for:
- Starting/stopping Historical Backfill mode
- Starting/stopping Real-time Streaming mode
- Switching between modes
- Monitoring status of both modes
- Pause/Resume controls
- ClickHouse table and view queries
- MinIO file browsing
"""

import threading
import structlog
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

from shared.config import get_settings
from shared.minio_client import get_minio_client
from shared.clickhouse_client import get_clickhouse_client
from task3_slack.historical_backfill import SlackHistoricalBackfill
from task3_slack.realtime_stream import SlackRealtimeStream
from task3_slack.clickhouse_loader import SlackClickHouseLoader

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/slack", tags=["Slack"])

_backfill: Optional[SlackHistoricalBackfill] = None
_realtime: Optional[SlackRealtimeStream] = None
_ch_loader: Optional[SlackClickHouseLoader] = None
_backfill_thread: Optional[threading.Thread] = None


def _get_backfill() -> SlackHistoricalBackfill:
    global _backfill
    if _backfill is None:
        _backfill = SlackHistoricalBackfill()
    return _backfill


def _get_realtime() -> SlackRealtimeStream:
    global _realtime
    if _realtime is None:
        _realtime = SlackRealtimeStream()
    return _realtime


def _get_ch_loader() -> SlackClickHouseLoader:
    global _ch_loader
    if _ch_loader is None:
        _ch_loader = SlackClickHouseLoader()
    return _ch_loader


class BackfillRequest(BaseModel):
    max_workers: int = 4


# ─── Historical Backfill Endpoints ───

@router.post("/backfill/start")
async def start_backfill(request: BackfillRequest):
    """Start the historical backfill engine."""
    global _backfill_thread
    backfill = _get_backfill()
    ch_loader = _get_ch_loader()

    if _backfill_thread and _backfill_thread.is_alive():
        return {"status": "already_running", "message": "Backfill already running"}

    def _run():
        try:
            # Create ClickHouse tables
            ch_loader.create_all_tables()

            # Run backfill
            result = backfill.run(max_workers=request.max_workers)

            # Load messages into ClickHouse
            minio = get_minio_client()
            settings = get_settings()
            from shared.parquet_writer import parquet_to_records
            import pyarrow.parquet as pq
            import io

            # Load historical messages from MinIO Parquet files
            objects = minio.list_objects(settings.MINIO_SLACK_BUCKET, prefix="historical/")
            for obj in objects:
                if obj["name"].endswith("messages.parquet"):
                    try:
                        data = minio.download_bytes(settings.MINIO_SLACK_BUCKET, obj["name"])
                        table = pq.read_table(io.BytesIO(data))
                        records = table.to_pylist()
                        if records:
                            ch_loader.insert_messages(records)
                    except Exception as e:
                        logger.warning(f"[SLACK API] CH load failed for {obj['name']}", error=str(e))

                elif obj["name"].endswith("users.parquet"):
                    try:
                        data = minio.download_bytes(settings.MINIO_SLACK_BUCKET, obj["name"])
                        table = pq.read_table(io.BytesIO(data))
                        records = table.to_pylist()
                        if records:
                            ch_loader.insert_users(records)
                    except Exception as e:
                        logger.warning(f"[SLACK API] User CH load failed", error=str(e))

                elif obj["name"].endswith("channels.parquet"):
                    try:
                        data = minio.download_bytes(settings.MINIO_SLACK_BUCKET, obj["name"])
                        table = pq.read_table(io.BytesIO(data))
                        records = table.to_pylist()
                        if records:
                            ch_loader.insert_channels(records)
                    except Exception as e:
                        logger.warning(f"[SLACK API] Channel CH load failed", error=str(e))

            # Create views
            ch_loader.create_analytical_views()

        except Exception as e:
            logger.error("[SLACK API] Backfill failed", error=str(e))

    _backfill_thread = threading.Thread(target=_run, daemon=True)
    _backfill_thread.start()

    return {"status": "started", "mode": "historical_backfill", "max_workers": request.max_workers}


@router.get("/backfill/status")
async def backfill_status():
    """Get historical backfill status."""
    backfill = _get_backfill()
    return backfill.get_status()


@router.post("/backfill/pause")
async def pause_backfill():
    """Pause historical backfill."""
    backfill = _get_backfill()
    backfill.pause()
    return {"status": "paused", "mode": "historical_backfill"}


@router.post("/backfill/resume")
async def resume_backfill():
    """Resume historical backfill."""
    backfill = _get_backfill()
    backfill.resume()
    return {"status": "resumed", "mode": "historical_backfill"}


@router.post("/backfill/reset")
async def reset_backfill():
    """Reset backfill checkpoints."""
    backfill = _get_backfill()
    backfill.reset_checkpoints()
    return {"status": "reset"}


# ─── Real-time Stream Endpoints ───

@router.post("/realtime/start")
async def start_realtime():
    """Start the real-time WebSocket streaming engine."""
    realtime = _get_realtime()
    ch_loader = _get_ch_loader()
    ch_loader.create_all_tables()
    realtime.start()
    return {"status": "started", "mode": "realtime_stream"}


@router.post("/realtime/stop")
async def stop_realtime():
    """Stop the real-time streaming engine."""
    realtime = _get_realtime()
    realtime.stop()
    return {"status": "stopped", "mode": "realtime_stream"}


@router.get("/realtime/status")
async def realtime_status():
    """Get real-time stream status."""
    realtime = _get_realtime()
    return realtime.get_status()


@router.post("/realtime/pause")
async def pause_realtime():
    """Pause real-time stream."""
    realtime = _get_realtime()
    realtime.pause()
    return {"status": "paused", "mode": "realtime_stream"}


@router.post("/realtime/resume")
async def resume_realtime():
    """Resume real-time stream."""
    realtime = _get_realtime()
    realtime.resume()
    return {"status": "resumed", "mode": "realtime_stream"}


# ─── Combined Status ───

@router.get("/status")
async def combined_status():
    """Get combined status of both modes."""
    backfill = _get_backfill()
    realtime = _get_realtime()
    return {
        "backfill": backfill.get_status(),
        "realtime": realtime.get_status(),
    }


# ─── ClickHouse Endpoints ───

@router.get("/clickhouse/tables")
async def clickhouse_tables():
    """Get ClickHouse table statistics for Slack data."""
    try:
        ch_loader = _get_ch_loader()
        stats = ch_loader.get_table_stats()
        return {"tables": stats}
    except Exception as e:
        return {"tables": [], "error": str(e)}


@router.get("/clickhouse/views")
async def clickhouse_views():
    """List ClickHouse analytical views for Slack."""
    try:
        ch = get_clickhouse_client()
        views = ch.list_views()
        slack_views = [v for v in views if v.startswith("v_slack_")]
        return {"views": slack_views}
    except Exception as e:
        return {"views": [], "error": str(e)}


@router.get("/clickhouse/query")
async def clickhouse_query(table: str, limit: int = 100):
    """Query a Slack ClickHouse table or view."""
    try:
        ch = get_clickhouse_client()
        if not table.startswith(("bronze_slack_", "v_slack_")):
            raise HTTPException(status_code=400, detail="Only Slack tables/views allowed")
        results = ch.query(f"SELECT * FROM `{table}` LIMIT {limit}")
        return {"table": table, "rows": results, "count": len(results)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── MinIO Endpoints ───

@router.get("/minio/files")
async def minio_files(prefix: str = ""):
    """Browse MinIO files in the Slack bucket."""
    try:
        minio = get_minio_client()
        settings = get_settings()
        objects = minio.list_objects(settings.MINIO_SLACK_BUCKET, prefix=prefix)
        stats = minio.get_bucket_stats(settings.MINIO_SLACK_BUCKET)
        return {"files": objects, "stats": stats}
    except Exception as e:
        return {"files": [], "stats": {}, "error": str(e)}
