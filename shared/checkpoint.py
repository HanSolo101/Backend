"""Checkpoint manager for pause/resume and crash recovery.

Stores pipeline state (cursors, offsets, timestamps) persistently
in MinIO so pipelines can recover from crashes and support pause/resume.
"""

import json
import time
import structlog
from datetime import datetime
from typing import Any, Optional

logger = structlog.get_logger(__name__)


class CheckpointManager:
    """Manages persistent checkpoints for ingestion pipelines.

    Stores checkpoint state as JSON files in MinIO under the 'checkpoints' bucket.
    Each pipeline/resource gets its own checkpoint file.
    """

    def __init__(self, minio_client, bucket: str = "checkpoints"):
        self.minio = minio_client
        self.bucket = bucket
        self._cache: dict[str, dict] = {}

    def _checkpoint_path(self, pipeline: str, resource: str) -> str:
        """Build the MinIO object path for a checkpoint."""
        return f"{pipeline}/{resource}/checkpoint.json"

    def save(
        self,
        pipeline: str,
        resource: str,
        state: dict[str, Any],
    ):
        """Save checkpoint state for a pipeline resource.

        Args:
            pipeline: Pipeline name (e.g., 'salesforce', 'hubspot', 'slack').
            resource: Resource name (e.g., 'accounts', 'contacts', 'channel_C123').
            state: Dict of state values to persist (cursor, offset, timestamp, etc.).
        """
        checkpoint = {
            "pipeline": pipeline,
            "resource": resource,
            "state": state,
            "saved_at": datetime.utcnow().isoformat(),
            "epoch_ms": int(time.time() * 1000),
        }

        path = self._checkpoint_path(pipeline, resource)
        self.minio.upload_json(self.bucket, path, checkpoint)
        self._cache[f"{pipeline}/{resource}"] = checkpoint

        logger.info(
            "[CHECKPOINT] State saved",
            pipeline=pipeline,
            resource=resource,
            state=state,
        )

    def load(self, pipeline: str, resource: str) -> Optional[dict]:
        """Load checkpoint state for a pipeline resource.

        Returns:
            The state dict if a checkpoint exists, else None.
        """
        cache_key = f"{pipeline}/{resource}"
        if cache_key in self._cache:
            return self._cache[cache_key].get("state")

        path = self._checkpoint_path(pipeline, resource)
        try:
            if self.minio.object_exists(self.bucket, path):
                checkpoint = self.minio.download_json(self.bucket, path)
                self._cache[cache_key] = checkpoint
                logger.info(
                    "[CHECKPOINT] State loaded",
                    pipeline=pipeline,
                    resource=resource,
                    state=checkpoint.get("state"),
                )
                return checkpoint.get("state")
        except Exception as e:
            logger.warning(
                "[CHECKPOINT] Load failed, starting fresh",
                pipeline=pipeline,
                resource=resource,
                error=str(e),
            )
        return None

    def clear(self, pipeline: str, resource: str):
        """Clear a checkpoint (e.g., after successful completion)."""
        path = self._checkpoint_path(pipeline, resource)
        cache_key = f"{pipeline}/{resource}"
        try:
            if self.minio.object_exists(self.bucket, path):
                self.minio.delete_object(self.bucket, path)
            self._cache.pop(cache_key, None)
            logger.info("[CHECKPOINT] Cleared", pipeline=pipeline, resource=resource)
        except Exception as e:
            logger.warning("[CHECKPOINT] Clear failed", error=str(e))

    def list_checkpoints(self, pipeline: str) -> list[dict]:
        """List all checkpoints for a pipeline."""
        objects = self.minio.list_objects(self.bucket, prefix=f"{pipeline}/")
        checkpoints = []
        for obj in objects:
            if obj["name"].endswith("checkpoint.json"):
                try:
                    data = self.minio.download_json(self.bucket, obj["name"])
                    checkpoints.append(data)
                except Exception:
                    pass
        return checkpoints

    def get_high_water_mark(
        self, pipeline: str, resource: str, field: str = "last_timestamp"
    ) -> Optional[Any]:
        """Get a specific high-water-mark value from a checkpoint."""
        state = self.load(pipeline, resource)
        if state:
            return state.get(field)
        return None


class PipelineStateManager:
    """Manages the run state (running/paused/stopped) of pipelines.

    Stores state in memory with persistence to MinIO for crash recovery.
    """

    # Pipeline states
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"
    COMPLETED = "completed"
    FAILED = "failed"

    def __init__(self, minio_client, bucket: str = "checkpoints"):
        self.minio = minio_client
        self.bucket = bucket
        self._states: dict[str, dict] = {}

    def _state_path(self, pipeline: str) -> str:
        return f"{pipeline}/_pipeline_state.json"

    def set_state(self, pipeline: str, state: str, metadata: dict = None):
        """Set the run state of a pipeline."""
        entry = {
            "pipeline": pipeline,
            "state": state,
            "metadata": metadata or {},
            "updated_at": datetime.utcnow().isoformat(),
        }
        self._states[pipeline] = entry
        try:
            self.minio.upload_json(self.bucket, self._state_path(pipeline), entry)
        except Exception as e:
            logger.warning("state_persist_error", pipeline=pipeline, error=str(e))
        logger.info("[PIPELINE STATE]", pipeline=pipeline, state=state)

    def get_state(self, pipeline: str) -> str:
        """Get the current state of a pipeline."""
        if pipeline in self._states:
            return self._states[pipeline]["state"]
        # Try loading from MinIO
        try:
            path = self._state_path(pipeline)
            if self.minio.object_exists(self.bucket, path):
                entry = self.minio.download_json(self.bucket, path)
                self._states[pipeline] = entry
                return entry["state"]
        except Exception:
            pass
        return self.STOPPED

    def is_running(self, pipeline: str) -> bool:
        return self.get_state(pipeline) == self.RUNNING

    def is_paused(self, pipeline: str) -> bool:
        return self.get_state(pipeline) == self.PAUSED

    def get_all_states(self) -> dict[str, dict]:
        """Return all known pipeline states."""
        return dict(self._states)
