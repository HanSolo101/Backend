"""Slack Real-time Event Streaming Engine.

Mode 2: Establishes a WebSocket connection to Slack Events API for live
message streaming with <1 second latency:
- Connects to WebSocket events endpoint
- Processes message, reaction_added, member_joined_channel events
- Writes incoming events to Parquet files in MinIO
- Maintains high-water mark checkpoints per channel
- Supports pause/resume
- Crash recovery via persistent checkpoint state
"""

import time
import json
import asyncio
import threading
import structlog
import websockets
from datetime import datetime
from typing import Optional
from collections import defaultdict

from shared.config import get_settings
from shared.minio_client import get_minio_client
from shared.parquet_writer import records_to_parquet
from shared.checkpoint import CheckpointManager, PipelineStateManager

logger = structlog.get_logger(__name__)


class SlackRealtimeStream:
    """Real-time WebSocket event streaming engine for Slack."""

    PIPELINE_NAME = "slack"
    RESOURCE_NAME = "realtime"

    # Flush buffer to Parquet every N messages or N seconds
    FLUSH_INTERVAL_SECONDS = 10.0
    FLUSH_BATCH_SIZE = 50

    def __init__(self):
        settings = get_settings()
        self.ws_url = settings.MOCK_SLACK_WS_URL + "/ws/events"
        self.minio = get_minio_client()
        self.checkpoint = CheckpointManager(self.minio)
        self.state_manager = PipelineStateManager(self.minio)
        self.bucket = settings.MINIO_SLACK_BUCKET

        # Event buffer
        self._buffer: list[dict] = []
        self._buffer_lock = threading.Lock()
        self._last_flush = time.time()

        # Tracking
        self._running = False
        self._event_count = 0
        self._message_count = 0
        self._flush_count = 0
        self._start_time: Optional[float] = None
        self._ws_connection = None
        self._stream_thread: Optional[threading.Thread] = None

        # Per-channel high water marks
        self._high_water_marks: dict[str, str] = {}

    def _process_event(self, raw_event: dict) -> Optional[dict]:
        """Process a raw WebSocket event into a normalized record."""
        payload = raw_event.get("payload", {})
        event_data = payload.get("event", {})
        event_type = event_data.get("type", "unknown")

        self._event_count += 1

        if event_type == "message":
            self._message_count += 1
            channel_id = event_data.get("channel", "")
            message_ts = event_data.get("ts", "")

            # Update high-water mark
            if channel_id:
                current_hwm = self._high_water_marks.get(channel_id, "0")
                if message_ts > current_hwm:
                    self._high_water_marks[channel_id] = message_ts

            return {
                "event_type": "message",
                "channel_id": channel_id,
                "user_id": event_data.get("user", ""),
                "text": event_data.get("text", ""),
                "message_ts": message_ts,
                "thread_ts": event_data.get("thread_ts", ""),
                "team": event_data.get("team", ""),
                "channel_type": event_data.get("channel_type", ""),
                "subtype": event_data.get("subtype", ""),
                "event_time": str(payload.get("event_time", "")),
                "processing_date": datetime.utcnow().strftime("%Y-%m-%d"),
                "ingestion_mode": "realtime",
                "envelope_id": raw_event.get("envelope_id", ""),
            }

        elif event_type == "reaction_added":
            return {
                "event_type": "reaction_added",
                "channel_id": event_data.get("item", {}).get("channel", ""),
                "user_id": event_data.get("user", ""),
                "text": f":{event_data.get('reaction', '')}:",
                "message_ts": event_data.get("item", {}).get("ts", ""),
                "thread_ts": "",
                "team": "",
                "channel_type": "",
                "subtype": "reaction",
                "event_time": event_data.get("event_ts", ""),
                "processing_date": datetime.utcnow().strftime("%Y-%m-%d"),
                "ingestion_mode": "realtime",
                "envelope_id": raw_event.get("envelope_id", ""),
            }

        elif event_type == "member_joined_channel":
            return {
                "event_type": "member_joined_channel",
                "channel_id": event_data.get("channel", ""),
                "user_id": event_data.get("user", ""),
                "text": "joined channel",
                "message_ts": event_data.get("event_ts", ""),
                "thread_ts": "",
                "team": event_data.get("team", ""),
                "channel_type": event_data.get("channel_type", ""),
                "subtype": "member_join",
                "event_time": event_data.get("event_ts", ""),
                "processing_date": datetime.utcnow().strftime("%Y-%m-%d"),
                "ingestion_mode": "realtime",
                "envelope_id": raw_event.get("envelope_id", ""),
            }

        return None

    def _flush_buffer(self):
        """Flush the event buffer to Parquet in MinIO."""
        with self._buffer_lock:
            if not self._buffer:
                return
            records = list(self._buffer)
            self._buffer.clear()

        if not records:
            return

        try:
            parquet_bytes = records_to_parquet(records)
            timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H%M%S")
            parquet_path = f"realtime/{datetime.utcnow().strftime('%Y-%m-%d')}/events_{timestamp}_{self._flush_count}.parquet"

            self.minio.upload_parquet(self.bucket, parquet_path, parquet_bytes)
            self._flush_count += 1

            # Save checkpoint with high-water marks
            self.checkpoint.save(
                self.PIPELINE_NAME,
                "realtime_stream",
                {
                    "high_water_marks": self._high_water_marks,
                    "total_events": self._event_count,
                    "total_messages": self._message_count,
                    "total_flushes": self._flush_count,
                    "last_flush": datetime.utcnow().isoformat(),
                },
            )

            logger.info(
                f"[SLACK REALTIME] Flushed {len(records)} events to Parquet",
                path=parquet_path,
                total_events=self._event_count,
            )

        except Exception as e:
            logger.error("[SLACK REALTIME] Flush failed", error=str(e))
            # Put records back in buffer
            with self._buffer_lock:
                self._buffer = records + self._buffer

    async def _stream_loop(self):
        """Main WebSocket streaming loop."""
        logger.info(f"[SLACK REALTIME] Connecting to {self.ws_url}")

        # Load checkpoint for crash recovery
        checkpoint_state = self.checkpoint.load(self.PIPELINE_NAME, "realtime_stream")
        if checkpoint_state:
            self._high_water_marks = checkpoint_state.get("high_water_marks", {})
            self._event_count = checkpoint_state.get("total_events", 0)
            self._message_count = checkpoint_state.get("total_messages", 0)
            self._flush_count = checkpoint_state.get("total_flushes", 0)
            logger.info(
                "[SLACK REALTIME] Resuming from checkpoint",
                events=self._event_count,
                messages=self._message_count,
            )

        try:
            async with websockets.connect(self.ws_url) as ws:
                self._ws_connection = ws
                logger.info("[SLACK REALTIME] ✅ WebSocket connected")

                while self._running:
                    # Check for pause
                    if self.state_manager.is_paused(self.PIPELINE_NAME):
                        self._flush_buffer()
                        logger.info("[SLACK REALTIME] Paused, waiting for resume...")
                        while self.state_manager.is_paused(self.PIPELINE_NAME) and self._running:
                            await asyncio.sleep(1.0)
                        if not self._running:
                            break
                        logger.info("[SLACK REALTIME] Resumed")

                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        event = json.loads(raw)

                        if event.get("type") == "hello":
                            logger.info("[SLACK REALTIME] Received hello event")
                            continue

                        record = self._process_event(event)
                        if record:
                            with self._buffer_lock:
                                self._buffer.append(record)

                            # Flush on batch size
                            if len(self._buffer) >= self.FLUSH_BATCH_SIZE:
                                self._flush_buffer()

                    except asyncio.TimeoutError:
                        # No message received, check if we should flush by time
                        if time.time() - self._last_flush >= self.FLUSH_INTERVAL_SECONDS:
                            self._flush_buffer()
                            self._last_flush = time.time()

                    except websockets.exceptions.ConnectionClosed:
                        logger.warning("[SLACK REALTIME] WebSocket closed, reconnecting...")
                        break

        except Exception as e:
            logger.error("[SLACK REALTIME] Stream error", error=str(e))
            # Flush any remaining buffer
            self._flush_buffer()

    def _run_stream(self):
        """Run the streaming loop in a thread."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        while self._running:
            try:
                loop.run_until_complete(self._stream_loop())
            except Exception as e:
                logger.error("[SLACK REALTIME] Loop error, retrying in 5s", error=str(e))
                time.sleep(5)

        loop.close()

    def start(self):
        """Start the real-time streaming engine."""
        if self._running:
            logger.warning("[SLACK REALTIME] Already running")
            return

        self._running = True
        self._start_time = time.time()
        self.state_manager.set_state(self.PIPELINE_NAME, PipelineStateManager.RUNNING)

        self._stream_thread = threading.Thread(target=self._run_stream, daemon=True)
        self._stream_thread.start()

        logger.info("[SLACK REALTIME] ✅ Real-time streaming started")

    def stop(self):
        """Stop the real-time streaming engine."""
        self._running = False
        self._flush_buffer()  # Final flush
        self.state_manager.set_state(self.PIPELINE_NAME, PipelineStateManager.STOPPED)
        logger.info("[SLACK REALTIME] Stopped")

    def pause(self):
        """Pause the stream (stops processing but keeps connection)."""
        self.state_manager.set_state(self.PIPELINE_NAME, PipelineStateManager.PAUSED)
        logger.info("[SLACK REALTIME] PAUSED")

    def resume(self):
        """Resume the stream."""
        self.state_manager.set_state(self.PIPELINE_NAME, PipelineStateManager.RUNNING)
        logger.info("[SLACK REALTIME] RESUMED")

    def get_status(self) -> dict:
        """Get current streaming status."""
        uptime = time.time() - self._start_time if self._start_time else 0
        return {
            "mode": "realtime_stream",
            "pipeline_state": self.state_manager.get_state(self.PIPELINE_NAME),
            "running": self._running,
            "uptime_seconds": round(uptime, 1),
            "total_events": self._event_count,
            "total_messages": self._message_count,
            "total_flushes": self._flush_count,
            "buffer_size": len(self._buffer),
            "high_water_marks": dict(self._high_water_marks),
        }
