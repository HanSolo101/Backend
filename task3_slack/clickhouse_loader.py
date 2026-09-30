"""Slack ClickHouse Loader.

Creates ClickHouse tables and analytical views for Slack data:
- Messages table (historical + realtime)
- Users reference table
- Channels reference table
- Compliance timeline view
- Channel activity view
"""

import structlog
from shared.clickhouse_client import get_clickhouse_client

logger = structlog.get_logger(__name__)

SLACK_SCHEMAS = {
    "messages": {
        "channel_id": "String",
        "channel_name": "String",
        "message_ts": "String",
        "user_id": "String",
        "text": "String",
        "thread_ts": "String",
        "reply_count": "Int32",
        "reactions": "String",
        "has_files": "UInt8",
        "file_count": "Int32",
        "team": "String",
        "subtype": "String",
        "processing_date": "String",
        "ingestion_mode": "String",
    },
    "realtime_events": {
        "event_type": "String",
        "channel_id": "String",
        "user_id": "String",
        "text": "String",
        "message_ts": "String",
        "thread_ts": "String",
        "team": "String",
        "channel_type": "String",
        "subtype": "String",
        "event_time": "String",
        "processing_date": "String",
        "ingestion_mode": "String",
        "envelope_id": "String",
    },
    "users": {
        "id": "String",
        "name": "String",
        "real_name": "String",
        "email": "String",
        "title": "String",
        "is_admin": "UInt8",
        "is_bot": "UInt8",
        "tz": "String",
    },
    "channels": {
        "id": "String",
        "name": "String",
        "is_private": "UInt8",
        "created": "Int64",
        "creator": "String",
        "num_members": "Int32",
        "topic": "String",
        "purpose": "String",
    },
}


class SlackClickHouseLoader:
    """Manages ClickHouse tables and views for Slack data."""

    def __init__(self):
        self.ch = get_clickhouse_client()

    def create_all_tables(self):
        """Create all Slack ClickHouse tables."""
        for resource, schema in SLACK_SCHEMAS.items():
            table_name = f"bronze_slack_{resource}"
            try:
                first_col = list(schema.keys())[0]
                self.ch.create_table(
                    table_name=table_name,
                    columns=schema,
                    order_by=[first_col],
                )
                logger.info(f"[CH] Slack table created: {table_name}")
            except Exception as e:
                logger.warning(f"[CH] Slack table issue: {table_name}", error=str(e))

    def insert_messages(self, records: list[dict]):
        """Insert message records into the messages table."""
        if not records:
            return
        table_name = "bronze_slack_messages"
        schema = SLACK_SCHEMAS["messages"]
        columns = list(schema.keys())

        coerced = []
        for record in records:
            row = {}
            for col in columns:
                val = record.get(col)
                expected_type = schema.get(col, "String")
                if val is None:
                    row[col] = val
                elif expected_type == "UInt8":
                    row[col] = 1 if val in (True, "true", "True", 1) else 0
                elif expected_type == "Int32":
                    try:
                        row[col] = int(val)
                    except (ValueError, TypeError):
                        row[col] = 0
                else:
                    row[col] = str(val) if val is not None else None
            coerced.append(row)

        self.ch.insert_data(table_name, coerced, columns)
        logger.info(f"[CH] Inserted {len(coerced)} messages into {table_name}")

    def insert_realtime_events(self, records: list[dict]):
        """Insert real-time event records."""
        if not records:
            return
        table_name = "bronze_slack_realtime_events"
        schema = SLACK_SCHEMAS["realtime_events"]
        columns = list(schema.keys())

        coerced = []
        for record in records:
            row = {}
            for col in columns:
                val = record.get(col)
                row[col] = str(val) if val is not None else None
            coerced.append(row)

        self.ch.insert_data(table_name, coerced, columns)

    def insert_users(self, records: list[dict]):
        """Insert user records."""
        if not records:
            return
        table_name = "bronze_slack_users"
        schema = SLACK_SCHEMAS["users"]
        columns = list(schema.keys())

        coerced = []
        for record in records:
            row = {}
            for col in columns:
                val = record.get(col)
                expected_type = schema.get(col, "String")
                if val is None:
                    row[col] = val
                elif expected_type == "UInt8":
                    row[col] = 1 if val in (True, "true", "True", 1) else 0
                elif expected_type == "Int64":
                    try:
                        row[col] = int(val)
                    except (ValueError, TypeError):
                        row[col] = 0
                else:
                    row[col] = str(val) if val is not None else None
            coerced.append(row)

        self.ch.insert_data(table_name, coerced, columns)

    def insert_channels(self, records: list[dict]):
        """Insert channel records."""
        if not records:
            return
        table_name = "bronze_slack_channels"
        schema = SLACK_SCHEMAS["channels"]
        columns = list(schema.keys())

        coerced = []
        for record in records:
            row = {}
            for col in columns:
                val = record.get(col)
                expected_type = schema.get(col, "String")
                if val is None:
                    row[col] = val
                elif expected_type in ("UInt8",):
                    row[col] = 1 if val in (True, "true", "True", 1) else 0
                elif expected_type in ("Int32", "Int64"):
                    try:
                        row[col] = int(val)
                    except (ValueError, TypeError):
                        row[col] = 0
                else:
                    row[col] = str(val) if val is not None else None
            coerced.append(row)

        self.ch.insert_data(table_name, coerced, columns)

    def create_analytical_views(self):
        """Create curated ClickHouse analytical views for Slack data."""
        views = {
            "v_slack_compliance_timeline": """
                SELECT
                    m.channel_id,
                    m.channel_name,
                    m.message_ts,
                    m.user_id,
                    m.text,
                    m.thread_ts,
                    m.reactions,
                    m.ingestion_mode,
                    m.processing_date
                FROM bronze_slack_messages m
                ORDER BY m.message_ts DESC
            """,
            "v_slack_channel_activity": """
                SELECT
                    channel_name,
                    channel_id,
                    count() as message_count,
                    countDistinct(user_id) as unique_users,
                    max(message_ts) as latest_message_ts,
                    min(message_ts) as earliest_message_ts,
                    sum(reply_count) as total_replies,
                    sum(file_count) as total_files
                FROM bronze_slack_messages
                GROUP BY channel_name, channel_id
                ORDER BY message_count DESC
            """,
            "v_slack_user_activity": """
                SELECT
                    user_id,
                    count() as message_count,
                    countDistinct(channel_id) as channels_active,
                    max(message_ts) as latest_message_ts
                FROM bronze_slack_messages
                GROUP BY user_id
                ORDER BY message_count DESC
            """,
            "v_slack_realtime_feed": """
                SELECT
                    event_type,
                    channel_id,
                    user_id,
                    text,
                    message_ts,
                    event_time,
                    processing_date
                FROM bronze_slack_realtime_events
                ORDER BY event_time DESC
            """,
        }

        for view_name, sql in views.items():
            try:
                self.ch.create_view(view_name, sql)
                logger.info(f"[CH] View created: {view_name}")
            except Exception as e:
                logger.warning(f"[CH] View creation deferred: {view_name}", error=str(e))

    def get_table_stats(self) -> list[dict]:
        """Get row counts for all Slack tables."""
        stats = []
        for resource in SLACK_SCHEMAS:
            table_name = f"bronze_slack_{resource}"
            try:
                if self.ch.table_exists(table_name):
                    count = self.ch.get_table_row_count(table_name)
                    stats.append({"table": table_name, "resource": resource, "rows": count})
            except Exception:
                stats.append({"table": table_name, "resource": resource, "rows": 0})
        return stats
