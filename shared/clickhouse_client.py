"""ClickHouse client wrapper for the Glynac platform.

Provides table creation, data insertion, view management,
and query execution for the ingestion pipeline.
"""

import structlog
import clickhouse_connect
from clickhouse_connect.driver.client import Client
from typing import Optional

from shared.config import get_settings

logger = structlog.get_logger(__name__)

# Python type → ClickHouse type mapping
PY_TO_CH_TYPE = {
    "str": "String",
    "string": "String",
    "int": "Int64",
    "int64": "Int64",
    "int32": "Int32",
    "float": "Float64",
    "float64": "Float64",
    "bool": "UInt8",
    "boolean": "UInt8",
    "datetime": "DateTime64(3)",
    "date": "Date",
    "list": "String",       # Serialized as JSON string
    "dict": "String",       # Serialized as JSON string
    "object": "String",
}


class ClickHouseClient:
    """High-level ClickHouse client for the Glynac ingestion platform."""

    def __init__(self):
        settings = get_settings()
        self.client: Client = clickhouse_connect.get_client(
            host=settings.CLICKHOUSE_HOST,
            port=settings.CLICKHOUSE_PORT,
            username=settings.CLICKHOUSE_USER,
            password=settings.CLICKHOUSE_PASSWORD,
            database=settings.CLICKHOUSE_DATABASE,
        )
        self._ensure_database()

    def _ensure_database(self):
        """Create the database if it doesn't exist."""
        settings = get_settings()
        try:
            self.client.command(
                f"CREATE DATABASE IF NOT EXISTS {settings.CLICKHOUSE_DATABASE}"
            )
            logger.info("database_ensured", database=settings.CLICKHOUSE_DATABASE)
        except Exception as e:
            logger.error("database_create_error", error=str(e))

    def create_table(
        self,
        table_name: str,
        columns: dict[str, str],
        order_by: list[str] | None = None,
        partition_by: str | None = None,
        engine: str = "MergeTree",
        if_not_exists: bool = True,
    ) -> str:
        """Create a ClickHouse table with the specified schema.

        Args:
            table_name: Name of the table to create.
            columns: Dict mapping column_name → ClickHouse type (e.g., {'id': 'String', 'amount': 'Float64'}).
            order_by: Columns for ORDER BY clause. Defaults to first column.
            partition_by: Optional partition expression.
            engine: Table engine (default MergeTree).
            if_not_exists: Add IF NOT EXISTS clause.

        Returns:
            The CREATE TABLE DDL that was executed.
        """
        # Build safe column definitions
        col_lines = []
        primary_col = (order_by[0] if order_by else list(columns.keys())[0])
        for col, dtype in columns.items():
            if col == primary_col:
                col_lines.append(f"    `{col}` {dtype}")
            else:
                col_lines.append(f"    `{col}` Nullable({dtype})")
        col_defs = ",\n".join(col_lines)

        if order_by is None:
            order_by = [list(columns.keys())[0]]

        exists_clause = "IF NOT EXISTS " if if_not_exists else ""
        order_clause = ", ".join(f"`{c}`" for c in order_by)

        ddl = f"CREATE TABLE {exists_clause}`{table_name}` (\n{col_defs}\n) ENGINE = {engine}()"

        if partition_by:
            ddl += f"\nPARTITION BY {partition_by}"

        ddl += f"\nORDER BY ({order_clause})"

        try:
            self.client.command(ddl)
            logger.info("table_created", table=table_name, columns=len(columns))
        except Exception as e:
            logger.error("table_create_error", table=table_name, error=str(e))
            raise

        return ddl

    def create_table_from_schema(
        self,
        table_name: str,
        schema: dict[str, str],
        order_by: list[str] | None = None,
        partition_by: str | None = None,
    ) -> str:
        """Create a table using a Python-type schema dict.

        The schema values are Python type names that get mapped to ClickHouse types.
        """
        ch_columns = {}
        for col, py_type in schema.items():
            ch_type = PY_TO_CH_TYPE.get(py_type.lower(), "String")
            ch_columns[col] = ch_type
        return self.create_table(
            table_name, ch_columns, order_by=order_by, partition_by=partition_by
        )

    def insert_data(self, table_name: str, data: list[dict], columns: list[str] | None = None):
        """Insert rows into a ClickHouse table.

        Args:
            table_name: Target table.
            data: List of row dicts.
            columns: Column names (inferred from first row if not provided).
        """
        if not data:
            return

        if columns is None:
            columns = list(data[0].keys())

        rows = []
        for record in data:
            row = []
            for col in columns:
                val = record.get(col)
                # Convert complex types to string
                if isinstance(val, (list, dict)):
                    import json
                    val = json.dumps(val, default=str)
                row.append(val)
            rows.append(row)

        try:
            self.client.insert(table_name, rows, column_names=columns)
            logger.info("data_inserted", table=table_name, rows=len(rows))
        except Exception as e:
            logger.error("insert_error", table=table_name, error=str(e))
            raise

    def query(self, sql: str) -> list[dict]:
        """Execute a SELECT query and return results as list of dicts."""
        try:
            result = self.client.query(sql)
            columns = result.column_names
            rows = []
            for row in result.result_rows:
                rows.append(dict(zip(columns, row)))
            return rows
        except Exception as e:
            logger.error("query_error", sql=sql[:200], error=str(e))
            raise

    def command(self, sql: str):
        """Execute a DDL or non-SELECT command."""
        try:
            self.client.command(sql)
        except Exception as e:
            logger.error("command_error", sql=sql[:200], error=str(e))
            raise

    def create_view(self, view_name: str, select_sql: str, replace: bool = True):
        """Create or replace a ClickHouse view."""
        prefix = "CREATE OR REPLACE VIEW" if replace else "CREATE VIEW IF NOT EXISTS"
        ddl = f"{prefix} `{view_name}` AS {select_sql}"
        self.command(ddl)
        logger.info("view_created", view=view_name)

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        result = self.query(
            f"SELECT count() as cnt FROM system.tables WHERE database = '{get_settings().CLICKHOUSE_DATABASE}' AND name = '{table_name}'"
        )
        return result[0]["cnt"] > 0 if result else False

    def get_table_row_count(self, table_name: str) -> int:
        """Get the row count for a table."""
        result = self.query(f"SELECT count() as cnt FROM `{table_name}`")
        return result[0]["cnt"] if result else 0

    def get_table_columns(self, table_name: str) -> list[dict]:
        """Get column definitions for a table."""
        return self.query(
            f"SELECT name, type FROM system.columns WHERE database = '{get_settings().CLICKHOUSE_DATABASE}' AND table = '{table_name}'"
        )

    def list_tables(self) -> list[str]:
        """List all tables in the database."""
        result = self.query(
            f"SELECT name FROM system.tables WHERE database = '{get_settings().CLICKHOUSE_DATABASE}' AND engine != 'View' ORDER BY name"
        )
        return [r["name"] for r in result]

    def list_views(self) -> list[str]:
        """List all views in the database."""
        result = self.query(
            f"SELECT name FROM system.tables WHERE database = '{get_settings().CLICKHOUSE_DATABASE}' AND engine = 'View' ORDER BY name"
        )
        return [r["name"] for r in result]

    def drop_table(self, table_name: str):
        """Drop a table if it exists."""
        self.command(f"DROP TABLE IF EXISTS `{table_name}`")
        logger.info("table_dropped", table=table_name)

    def truncate_table(self, table_name: str):
        """Truncate a table."""
        self.command(f"TRUNCATE TABLE `{table_name}`")
        logger.info("table_truncated", table=table_name)


# Singleton instance
_client: Optional[ClickHouseClient] = None


def get_clickhouse_client() -> ClickHouseClient:
    """Return singleton ClickHouse client instance."""
    global _client
    if _client is None:
        _client = ClickHouseClient()
    return _client
