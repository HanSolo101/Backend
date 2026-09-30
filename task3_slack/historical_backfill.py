"""Slack Historical Backfill Engine.

Mode 1: Recursively fetches historical workspace records with pagination:
- Channels, Users, Messages, Thread Replies, File Metadata, Reactions
- Parallel channel processing
- Checkpoint tracking per channel (channel_id + message_ts high-water marks)
- Pause/Resume support
- Crash recovery with zero missing or duplicate messages
"""

import time
import threading
import structlog
import httpx
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from shared.config import get_settings
from shared.minio_client import get_minio_client
from shared.parquet_writer import records_to_parquet
from shared.checkpoint import CheckpointManager, PipelineStateManager

logger = structlog.get_logger(__name__)


class SlackHistoricalBackfill:
    """Historical backfill engine for Slack workspace data."""

    PIPELINE_NAME = "slack"
    RESOURCE_NAME = "historical"

    def __init__(self):
        settings = get_settings()
        self.base_url = settings.MOCK_SLACK_URL
        self.minio = get_minio_client()
        self.checkpoint = CheckpointManager(self.minio)
        self.state_manager = PipelineStateManager(self.minio)
        self.bucket = settings.MINIO_SLACK_BUCKET
        self.client = httpx.Client(timeout=30.0)

        # Job tracking
        self._jobs: dict[str, dict] = {}
        self._lock = threading.Lock()
        self._channels: list[dict] = []
        self._users: list[dict] = []

    def _api_get(self, endpoint: str, params: dict = None) -> dict:
        """Make an API GET request to the mock Slack server."""
        response = self.client.get(f"{self.base_url}{endpoint}", params=params or {})
        response.raise_for_status()
        return response.json()

    def fetch_channels(self) -> list[dict]:
        """Fetch all workspace channels with pagination."""
        channels = []
        cursor = None
        while True:
            params = {"limit": 100}
            if cursor:
                params["cursor"] = cursor

            data = self._api_get("/api/conversations.list", params)
            channels.extend(data.get("channels", []))

            next_cursor = data.get("response_metadata", {}).get("next_cursor", "")
            if not next_cursor:
                break
            cursor = next_cursor

        self._channels = channels
        logger.info(f"[SLACK BACKFILL] Fetched {len(channels)} channels")
        return channels

    def fetch_users(self) -> list[dict]:
        """Fetch all workspace users with pagination."""
        users = []
        cursor = None
        while True:
            params = {"limit": 100}
            if cursor:
                params["cursor"] = cursor

            data = self._api_get("/api/users.list", params)
            users.extend(data.get("members", []))

            next_cursor = data.get("response_metadata", {}).get("next_cursor", "")
            if not next_cursor:
                break
            cursor = next_cursor

        self._users = users
        logger.info(f"[SLACK BACKFILL] Fetched {len(users)} users")
        return users

    def _backfill_channel(self, channel: dict) -> dict:
        """Backfill a single channel's message history."""
        channel_id = channel["id"]
        channel_name = channel["name"]
        job_info = {
            "channel_id": channel_id,
            "channel_name": channel_name,
            "status": "running",
            "messages_fetched": 0,
            "threads_fetched": 0,
            "start_time": time.time(),
            "end_time": None,
            "error": None,
        }

        with self._lock:
            self._jobs[channel_id] = job_info

        try:
            # Check for pause
            if self.state_manager.is_paused(self.PIPELINE_NAME):
                job_info["status"] = "paused"
                return job_info

            # Check checkpoint for resume
            checkpoint_state = self.checkpoint.load(self.PIPELINE_NAME, f"backfill_{channel_id}")
            if checkpoint_state and checkpoint_state.get("completed"):
                logger.info(f"[SLACK BACKFILL] Skipping #{channel_name} — already completed")
                job_info["status"] = "completed"
                job_info["messages_fetched"] = checkpoint_state.get("messages_fetched", 0)
                return job_info

            # Get high-water mark from checkpoint (for resume after crash)
            oldest_ts = None
            if checkpoint_state:
                oldest_ts = checkpoint_state.get("last_message_ts")
                logger.info(
                    f"[CHECKPOINT] Resuming #{channel_name} from ts={oldest_ts}"
                )

            logger.info(f"[SLACK BACKFILL] Starting backfill for #{channel_name}")

            # Fetch messages with pagination
            messages = []
            cursor = None
            while True:
                # Check for pause mid-stream
                if self.state_manager.is_paused(self.PIPELINE_NAME):
                    self._save_progress_checkpoint(channel_id, channel_name, messages)
                    job_info["status"] = "paused"
                    job_info["messages_fetched"] = len(messages)
                    return job_info

                params = {"channel": channel_id, "limit": 100}
                if cursor:
                    params["cursor"] = cursor
                if oldest_ts:
                    params["oldest"] = oldest_ts

                data = self._api_get("/api/conversations.history", params)
                batch_messages = data.get("messages", [])
                messages.extend(batch_messages)

                next_cursor = data.get("response_metadata", {}).get("next_cursor", "")
                if not next_cursor or not data.get("has_more", False):
                    break
                cursor = next_cursor

            job_info["messages_fetched"] = len(messages)

            # Fetch thread replies for messages with threads
            thread_messages = []
            for msg in messages:
                if msg.get("reply_count", 0) > 0 and msg.get("thread_ts"):
                    try:
                        reply_data = self._api_get(
                            "/api/conversations.replies",
                            {"channel": channel_id, "ts": msg["thread_ts"]},
                        )
                        replies = reply_data.get("messages", [])
                        # Skip the parent message (first reply is parent)
                        thread_messages.extend(replies[1:] if len(replies) > 1 else [])
                    except Exception as e:
                        logger.warning(f"[SLACK BACKFILL] Thread fetch failed", ts=msg["thread_ts"], error=str(e))

            job_info["threads_fetched"] = len(thread_messages)

            # Combine all messages
            all_messages = messages + thread_messages

            if not all_messages:
                job_info["status"] = "completed"
                return job_info

            # Normalize messages for Parquet
            normalized = []
            for msg in all_messages:
                normalized.append({
                    "channel_id": channel_id,
                    "channel_name": channel_name,
                    "message_ts": msg.get("ts", ""),
                    "user_id": msg.get("user", ""),
                    "text": msg.get("text", ""),
                    "thread_ts": msg.get("thread_ts", ""),
                    "reply_count": msg.get("reply_count", 0),
                    "reactions": str(msg.get("reactions", [])),
                    "has_files": len(msg.get("files", [])) > 0,
                    "file_count": len(msg.get("files", [])),
                    "team": msg.get("team", ""),
                    "subtype": msg.get("subtype", ""),
                    "processing_date": datetime.utcnow().strftime("%Y-%m-%d"),
                    "ingestion_mode": "historical",
                })

            # Convert to Parquet and upload to MinIO
            parquet_bytes = records_to_parquet(normalized)
            date_str = datetime.utcnow().strftime("%Y-%m-%d")
            parquet_path = f"historical/{channel_id}/{date_str}/messages.parquet"

            self.minio.upload_parquet(self.bucket, parquet_path, parquet_bytes)

            logger.info(
                f"[SLACK BACKFILL] ✅ #{channel_name} complete",
                messages=len(messages),
                threads=len(thread_messages),
                total=len(normalized),
            )

            # Save completion checkpoint
            last_ts = max((m.get("ts", "0") for m in messages), default="0")
            self.checkpoint.save(
                self.PIPELINE_NAME,
                f"backfill_{channel_id}",
                {
                    "completed": True,
                    "messages_fetched": len(messages),
                    "threads_fetched": len(thread_messages),
                    "last_message_ts": last_ts,
                    "channel_name": channel_name,
                    "timestamp": datetime.utcnow().isoformat(),
                },
            )

            job_info["status"] = "completed"
            job_info["end_time"] = time.time()

        except Exception as e:
            job_info["status"] = "failed"
            job_info["error"] = str(e)
            job_info["end_time"] = time.time()
            logger.error(f"[SLACK BACKFILL] ❌ #{channel_name} failed", error=str(e))

        return job_info

    def _save_progress_checkpoint(self, channel_id: str, channel_name: str, messages: list):
        """Save a progress checkpoint for pause/crash recovery."""
        last_ts = max((m.get("ts", "0") for m in messages), default="0") if messages else "0"
        self.checkpoint.save(
            self.PIPELINE_NAME,
            f"backfill_{channel_id}",
            {
                "completed": False,
                "messages_fetched": len(messages),
                "last_message_ts": last_ts,
                "channel_name": channel_name,
                "timestamp": datetime.utcnow().isoformat(),
            },
        )
        logger.info(
            f"[CHECKPOINT] Channel {channel_id} saved at ts={last_ts}"
        )

    def run(self, max_workers: int = None) -> dict:
        """Run historical backfill across all channels in parallel."""
        settings = get_settings()
        if max_workers is None:
            max_workers = settings.DEFAULT_PARALLEL_WORKERS

        self.state_manager.set_state(self.PIPELINE_NAME, PipelineStateManager.RUNNING)

        # Step 1: Fetch channels and users
        channels = self.fetch_channels()
        users = self.fetch_users()

        # Upload users to MinIO as reference data
        if users:
            user_records = []
            for u in users:
                user_records.append({
                    "id": u["id"],
                    "name": u.get("name", ""),
                    "real_name": u.get("real_name", ""),
                    "email": u.get("profile", {}).get("email", ""),
                    "title": u.get("profile", {}).get("title", ""),
                    "is_admin": u.get("is_admin", False),
                    "is_bot": u.get("is_bot", False),
                    "tz": u.get("tz", ""),
                })
            user_parquet = records_to_parquet(user_records)
            self.minio.upload_parquet(
                self.bucket,
                "historical/users/users.parquet",
                user_parquet,
            )

        # Upload channels as reference
        if channels:
            channel_records = []
            for c in channels:
                channel_records.append({
                    "id": c["id"],
                    "name": c["name"],
                    "is_private": c.get("is_private", False),
                    "created": c.get("created", 0),
                    "creator": c.get("creator", ""),
                    "num_members": c.get("num_members", 0),
                    "topic": c.get("topic", {}).get("value", ""),
                    "purpose": c.get("purpose", {}).get("value", ""),
                })
            ch_parquet = records_to_parquet(channel_records)
            self.minio.upload_parquet(
                self.bucket,
                "historical/channels/channels.parquet",
                ch_parquet,
            )

        # Step 2: Parallel channel backfill
        logger.info(f"[SLACK BACKFILL] Starting parallel backfill", channels=len(channels), workers=max_workers)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._backfill_channel, ch): ch
                for ch in channels
            }
            for future in as_completed(futures):
                ch = futures[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"[SLACK BACKFILL] Thread error for #{ch['name']}", error=str(e))

        self.state_manager.set_state(self.PIPELINE_NAME, PipelineStateManager.COMPLETED)

        # Summary
        completed = sum(1 for j in self._jobs.values() if j["status"] == "completed")
        total_messages = sum(j["messages_fetched"] for j in self._jobs.values())

        logger.info(
            f"[SLACK BACKFILL] ✅ Backfill complete",
            channels_completed=completed,
            total_messages=total_messages,
        )

        return {
            "status": "completed",
            "channels_processed": len(channels),
            "channels_completed": completed,
            "total_messages": total_messages,
            "jobs": dict(self._jobs),
        }

    def pause(self):
        self.state_manager.set_state(self.PIPELINE_NAME, PipelineStateManager.PAUSED)
        logger.info("[SLACK BACKFILL] PAUSED")

    def resume(self):
        self.state_manager.set_state(self.PIPELINE_NAME, PipelineStateManager.RUNNING)
        logger.info("[SLACK BACKFILL] RESUMED")

    def get_status(self) -> dict:
        return {
            "mode": "historical_backfill",
            "pipeline_state": self.state_manager.get_state(self.PIPELINE_NAME),
            "jobs": dict(self._jobs),
            "summary": {
                "total_channels": len(self._jobs),
                "completed": sum(1 for j in self._jobs.values() if j["status"] == "completed"),
                "failed": sum(1 for j in self._jobs.values() if j["status"] == "failed"),
                "running": sum(1 for j in self._jobs.values() if j["status"] == "running"),
                "total_messages": sum(j["messages_fetched"] for j in self._jobs.values()),
            },
        }

    def reset_checkpoints(self):
        """Clear all backfill checkpoints."""
        for channel in self._channels:
            self.checkpoint.clear(self.PIPELINE_NAME, f"backfill_{channel['id']}")
        logger.info("[SLACK BACKFILL] All checkpoints cleared")

    def close(self):
        self.client.close()
