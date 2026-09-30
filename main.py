"""Glynac Backend Ingestion Platform — Unified API Server.

Main entry point that mounts all three task API routers and serves
the unified monitoring dashboard.

Launch with: python main.py
"""

import sys
import os
import subprocess
import structlog
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.dev.ConsoleRenderer(colors=True),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    logger_factory=structlog.PrintLoggerFactory(),
)

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle manager."""
    logger.info("=" * 60)
    logger.info("  GLYNAC BACKEND INGESTION PLATFORM")
    logger.info("  Starting up...")
    logger.info("=" * 60)
    yield
    logger.info("Shutting down...")


app = FastAPI(
    title="Glynac Backend Ingestion Platform",
    description="Enterprise-grade ingestion services for Salesforce, HubSpot, and Slack",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS for web UI
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount task API routers
from task1_salesforce.api import router as salesforce_router
from task2_hubspot.api import router as hubspot_router
from task3_slack.api import router as slack_router

app.include_router(salesforce_router)
app.include_router(hubspot_router)
app.include_router(slack_router)


# ─── Health & System Endpoints ───

@app.get("/api/health")
async def health():
    """System health check."""
    return {
        "status": "ok",
        "service": "glynac-backend-platform",
        "version": "1.0.0",
        "pipelines": ["salesforce", "hubspot", "slack"],
    }


@app.get("/api/system/info")
async def system_info():
    """Get system configuration info."""
    from shared.config import get_settings
    settings = get_settings()
    return {
        "minio_endpoint": settings.MINIO_ENDPOINT,
        "clickhouse_host": settings.CLICKHOUSE_HOST,
        "clickhouse_port": settings.CLICKHOUSE_PORT,
        "clickhouse_database": settings.CLICKHOUSE_DATABASE,
        "mock_salesforce_url": settings.MOCK_SALESFORCE_URL,
        "mock_hubspot_url": settings.MOCK_HUBSPOT_URL,
        "mock_slack_url": settings.MOCK_SLACK_URL,
    }


@app.get("/api/clickhouse/all-tables")
async def all_clickhouse_tables():
    """List all ClickHouse tables and views across all pipelines."""
    try:
        from shared.clickhouse_client import get_clickhouse_client
        ch = get_clickhouse_client()
        tables = ch.list_tables()
        views = ch.list_views()

        table_stats = []
        for table in tables:
            try:
                count = ch.get_table_row_count(table)
                table_stats.append({"name": table, "type": "table", "rows": count})
            except Exception:
                table_stats.append({"name": table, "type": "table", "rows": 0})

        for view in views:
            table_stats.append({"name": view, "type": "view", "rows": None})

        return {"items": table_stats, "tables": len(tables), "views": len(views)}
    except Exception as e:
        return {"items": [], "tables": 0, "views": 0, "error": str(e)}


@app.get("/api/minio/all-stats")
async def all_minio_stats():
    """Get MinIO statistics across all buckets."""
    try:
        from shared.minio_client import get_minio_client
        from shared.config import get_settings
        minio = get_minio_client()
        settings = get_settings()

        stats = {}
        for bucket in [settings.MINIO_SALESFORCE_BUCKET, settings.MINIO_HUBSPOT_BUCKET, settings.MINIO_SLACK_BUCKET]:
            stats[bucket] = minio.get_bucket_stats(bucket)
        return stats
    except Exception as e:
        return {"error": str(e)}


# Serve web UI static files
web_ui_dir = os.path.join(os.path.dirname(__file__), "web_ui")
if os.path.exists(web_ui_dir):
    app.mount("/static", StaticFiles(directory=web_ui_dir), name="static")

    @app.get("/")
    async def serve_ui():
        return FileResponse(os.path.join(web_ui_dir, "index.html"))


def start_mock_servers():
    """Start all mock API servers as background processes."""
    mock_dir = os.path.join(os.path.dirname(__file__), "mock_servers")
    processes = []

    mocks = [
        ("salesforce_mock.py", 8100),
        ("hubspot_mock.py", 8200),
        ("slack_mock.py", 8300),
    ]

    for filename, port in mocks:
        filepath = os.path.join(mock_dir, filename)
        if os.path.exists(filepath):
            proc = subprocess.Popen(
                [sys.executable, "-m", "uvicorn",
                 f"mock_servers.{filename[:-3]}:app",
                 "--host", "0.0.0.0",
                 "--port", str(port),
                 "--log-level", "warning"],
                cwd=os.path.dirname(__file__),
            )
            processes.append(proc)
            logger.info(f"Mock server started: {filename} on port {port}")

    return processes


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Glynac Backend Ingestion Platform")
    parser.add_argument("--host", default="0.0.0.0", help="API server host")
    parser.add_argument("--port", type=int, default=8000, help="API server port")
    parser.add_argument("--no-mocks", action="store_true", help="Don't start mock servers")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload")
    args = parser.parse_args()

    mock_procs = []
    if not args.no_mocks:
        mock_procs = start_mock_servers()

    try:
        uvicorn.run(
            "main:app",
            host=args.host,
            port=args.port,
            reload=args.reload,
            log_level="info",
        )
    finally:
        for proc in mock_procs:
            proc.terminate()
