"""Mock Slack API Server with WebSocket support.

Simulates Slack Web API and WebSocket Events API:
- GET  /api/conversations.list     → List channels
- GET  /api/conversations.history  → Channel message history (paginated)
- GET  /api/conversations.replies  → Thread replies
- GET  /api/users.list             → List users
- GET  /api/reactions.list         → List reactions
- WS   /ws/events                  → Real-time events WebSocket

Generates realistic Slack workspace data with channels, users, messages,
threads, reactions, and file metadata.
"""

import uuid
import json
import random
import asyncio
import time
from datetime import datetime, timedelta
from typing import Optional

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from faker import Faker

fake = Faker()

app = FastAPI(title="Mock Slack API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ─── Pre-generate workspace data ───

NUM_CHANNELS = 15
NUM_USERS = 30
MESSAGES_PER_CHANNEL = 200

# Generate users
USERS = []
for i in range(NUM_USERS):
    user_id = f"U{fake.bothify('??#####').upper()}"
    USERS.append({
        "id": user_id,
        "team_id": "T0001WORKSPACE",
        "name": fake.user_name(),
        "real_name": fake.name(),
        "profile": {
            "email": fake.email(),
            "display_name": fake.user_name(),
            "real_name": fake.name(),
            "title": fake.job(),
            "phone": fake.phone_number(),
            "status_text": random.choice(["", "In a meeting", "Focusing", "Out of office", "Available"]),
            "status_emoji": random.choice(["", ":calendar:", ":headphones:", ":palm_tree:", ":white_check_mark:"]),
            "image_72": f"https://placecats.com/72/72",
        },
        "is_admin": i == 0,
        "is_bot": False,
        "deleted": False,
        "tz": random.choice(["America/New_York", "America/Chicago", "America/Los_Angeles", "Europe/London", "Asia/Tokyo"]),
        "updated": int((datetime.utcnow() - timedelta(days=random.randint(0, 60))).timestamp()),
    })

# Generate channels
CHANNELS = []
channel_names = [
    "general", "engineering", "sales", "marketing", "product", "design",
    "support", "random", "announcements", "devops", "data-team",
    "compliance", "leadership", "onboarding", "feedback"
]
for i, name in enumerate(channel_names[:NUM_CHANNELS]):
    channel_id = f"C{fake.bothify('??#####').upper()}"
    CHANNELS.append({
        "id": channel_id,
        "name": name,
        "is_channel": True,
        "is_private": name in ("leadership", "compliance"),
        "is_archived": False,
        "created": int((datetime.utcnow() - timedelta(days=random.randint(200, 1000))).timestamp()),
        "creator": random.choice(USERS)["id"],
        "topic": {"value": fake.sentence(), "creator": random.choice(USERS)["id"]},
        "purpose": {"value": fake.sentence(), "creator": random.choice(USERS)["id"]},
        "num_members": random.randint(5, len(USERS)),
    })

# Pre-generate message pools per channel
MESSAGES: dict[str, list] = {}

REACTION_EMOJIS = ["+1", "heart", "tada", "eyes", "fire", "rocket", "100", "clap", "thinking_face", "white_check_mark"]

for channel in CHANNELS:
    channel_messages = []
    base_ts = time.time() - (MESSAGES_PER_CHANNEL * 60 * 5)  # 5 min intervals going back

    for j in range(MESSAGES_PER_CHANNEL):
        msg_ts = str(base_ts + j * random.uniform(120, 600))
        user = random.choice(USERS)

        msg = {
            "type": "message",
            "subtype": None,
            "ts": msg_ts,
            "user": user["id"],
            "text": fake.paragraph(nb_sentences=random.randint(1, 4)),
            "channel": channel["id"],
            "team": "T0001WORKSPACE",
        }

        # 20% chance of reactions
        if random.random() < 0.2:
            msg["reactions"] = [
                {
                    "name": random.choice(REACTION_EMOJIS),
                    "users": [random.choice(USERS)["id"] for _ in range(random.randint(1, 5))],
                    "count": random.randint(1, 5),
                }
                for _ in range(random.randint(1, 3))
            ]

        # 15% chance of thread replies
        if random.random() < 0.15:
            msg["reply_count"] = random.randint(1, 8)
            msg["reply_users_count"] = random.randint(1, min(4, msg["reply_count"]))
            msg["latest_reply"] = str(float(msg_ts) + random.uniform(60, 3600))
            msg["thread_ts"] = msg_ts

        # 10% chance of file attachment
        if random.random() < 0.1:
            msg["files"] = [{
                "id": f"F{fake.bothify('??#####').upper()}",
                "name": fake.file_name(extension=random.choice(["pdf", "docx", "xlsx", "png", "csv"])),
                "mimetype": "application/octet-stream",
                "size": random.randint(1024, 10485760),
                "url_private": f"https://files.slack.com/files-pri/{fake.bothify('??#####')}/file",
                "created": int(float(msg_ts)),
            }]

        channel_messages.append(msg)

    MESSAGES[channel["id"]] = channel_messages


# ─── API Endpoints ───

@app.get("/api/conversations.list")
async def conversations_list(
    limit: int = Query(default=100),
    cursor: Optional[str] = Query(default=None),
    types: str = Query(default="public_channel,private_channel"),
):
    """List workspace channels with pagination."""
    offset = int(cursor) if cursor else 0
    batch = CHANNELS[offset : offset + limit]
    next_cursor = str(offset + limit) if offset + limit < len(CHANNELS) else ""

    return {
        "ok": True,
        "channels": batch,
        "response_metadata": {"next_cursor": next_cursor},
    }


@app.get("/api/conversations.history")
async def conversations_history(
    channel: str,
    limit: int = Query(default=100),
    cursor: Optional[str] = Query(default=None),
    oldest: Optional[str] = Query(default=None),
    latest: Optional[str] = Query(default=None),
):
    """Fetch channel message history with pagination."""
    msgs = MESSAGES.get(channel, [])

    # Apply time filters
    if oldest:
        msgs = [m for m in msgs if float(m["ts"]) >= float(oldest)]
    if latest:
        msgs = [m for m in msgs if float(m["ts"]) <= float(latest)]

    offset = int(cursor) if cursor else 0
    batch = msgs[offset : offset + limit]
    next_cursor = str(offset + limit) if offset + limit < len(msgs) else ""

    return {
        "ok": True,
        "messages": batch,
        "has_more": bool(next_cursor),
        "response_metadata": {"next_cursor": next_cursor},
    }


@app.get("/api/conversations.replies")
async def conversations_replies(
    channel: str,
    ts: str,
    limit: int = Query(default=100),
    cursor: Optional[str] = Query(default=None),
):
    """Fetch thread replies for a message."""
    # Generate synthetic replies
    reply_count = random.randint(1, 8)
    base_ts = float(ts)
    replies = []

    # Include parent message
    parent = {
        "type": "message",
        "ts": ts,
        "user": random.choice(USERS)["id"],
        "text": fake.paragraph(),
        "thread_ts": ts,
        "reply_count": reply_count,
    }
    replies.append(parent)

    for i in range(reply_count):
        reply_ts = str(base_ts + (i + 1) * random.uniform(30, 600))
        replies.append({
            "type": "message",
            "ts": reply_ts,
            "user": random.choice(USERS)["id"],
            "text": fake.sentence(),
            "thread_ts": ts,
        })

    return {
        "ok": True,
        "messages": replies,
        "has_more": False,
        "response_metadata": {"next_cursor": ""},
    }


@app.get("/api/users.list")
async def users_list(
    limit: int = Query(default=100),
    cursor: Optional[str] = Query(default=None),
):
    """List workspace users with pagination."""
    offset = int(cursor) if cursor else 0
    batch = USERS[offset : offset + limit]
    next_cursor = str(offset + limit) if offset + limit < len(USERS) else ""

    return {
        "ok": True,
        "members": batch,
        "response_metadata": {"next_cursor": next_cursor},
    }


@app.get("/api/reactions.list")
async def reactions_list(
    user: Optional[str] = Query(default=None),
    limit: int = Query(default=100),
    cursor: Optional[str] = Query(default=None),
):
    """List reactions across workspace."""
    items = []
    for channel_id, msgs in MESSAGES.items():
        for msg in msgs:
            if "reactions" in msg:
                items.append({
                    "type": "message",
                    "channel": channel_id,
                    "message": msg,
                })
    offset = int(cursor) if cursor else 0
    batch = items[offset : offset + limit]
    next_cursor = str(offset + limit) if offset + limit < len(items) else ""

    return {
        "ok": True,
        "items": batch,
        "response_metadata": {"next_cursor": next_cursor},
    }


# ─── WebSocket Real-time Events ───

_ws_connections: list[WebSocket] = []


@app.websocket("/ws/events")
async def websocket_events(websocket: WebSocket):
    """Real-time event stream via WebSocket.

    Sends synthetic Slack events every 1-5 seconds simulating
    live workspace activity (new messages, reactions, user status changes).
    """
    await websocket.accept()
    _ws_connections.append(websocket)

    try:
        # Send hello event
        await websocket.send_json({
            "type": "hello",
            "connection_info": {"app_id": "A001MOCK"},
        })

        # Stream synthetic events
        while True:
            await asyncio.sleep(random.uniform(1.0, 5.0))

            event_type = random.choice(["message", "message", "message", "reaction_added", "member_joined_channel"])
            channel = random.choice(CHANNELS)
            user = random.choice(USERS)

            if event_type == "message":
                event = {
                    "type": "events_api",
                    "envelope_id": str(uuid.uuid4()),
                    "payload": {
                        "type": "event_callback",
                        "event": {
                            "type": "message",
                            "subtype": None,
                            "channel": channel["id"],
                            "user": user["id"],
                            "text": fake.sentence(),
                            "ts": str(time.time()),
                            "team": "T0001WORKSPACE",
                            "channel_type": "channel",
                        },
                        "event_time": int(time.time()),
                    },
                }
            elif event_type == "reaction_added":
                event = {
                    "type": "events_api",
                    "envelope_id": str(uuid.uuid4()),
                    "payload": {
                        "type": "event_callback",
                        "event": {
                            "type": "reaction_added",
                            "user": user["id"],
                            "reaction": random.choice(REACTION_EMOJIS),
                            "item": {
                                "type": "message",
                                "channel": channel["id"],
                                "ts": str(time.time() - random.uniform(60, 3600)),
                            },
                            "event_ts": str(time.time()),
                        },
                    },
                }
            else:
                event = {
                    "type": "events_api",
                    "envelope_id": str(uuid.uuid4()),
                    "payload": {
                        "type": "event_callback",
                        "event": {
                            "type": "member_joined_channel",
                            "user": user["id"],
                            "channel": channel["id"],
                            "channel_type": "C",
                            "team": "T0001WORKSPACE",
                            "event_ts": str(time.time()),
                        },
                    },
                }

            await websocket.send_json(event)

    except WebSocketDisconnect:
        _ws_connections.remove(websocket)
    except Exception:
        if websocket in _ws_connections:
            _ws_connections.remove(websocket)


@app.get("/api/workspace_stats")
async def workspace_stats():
    """Get workspace statistics."""
    total_messages = sum(len(msgs) for msgs in MESSAGES.values())
    return {
        "channels": len(CHANNELS),
        "users": len(USERS),
        "total_messages": total_messages,
        "channel_list": [{"id": c["id"], "name": c["name"], "message_count": len(MESSAGES.get(c["id"], []))} for c in CHANNELS],
    }


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "mock-slack",
        "channels": len(CHANNELS),
        "users": len(USERS),
        "ws_connections": len(_ws_connections),
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8300)
