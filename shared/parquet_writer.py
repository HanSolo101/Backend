"""Parquet file writer using PyArrow for the Glynac platform.

Converts lists of dicts into compressed Parquet byte buffers
ready for upload to MinIO.
"""

import io
import pyarrow as pa
import pyarrow.parquet as pq
import structlog
from datetime import datetime, date
from typing import Any

logger = structlog.get_logger(__name__)

# Python type → PyArrow type mapping
PY_TO_ARROW = {
    str: pa.string(),
    int: pa.int64(),
    float: pa.float64(),
    bool: pa.bool_(),
    datetime: pa.timestamp("ms"),
    date: pa.date32(),
}


def _infer_arrow_type(value: Any) -> pa.DataType:
    """Infer PyArrow type from a Python value."""
    if value is None:
        return pa.string()
    for py_type, arrow_type in PY_TO_ARROW.items():
        if isinstance(value, py_type):
            return arrow_type
    return pa.string()


def _build_schema(records: list[dict]) -> pa.Schema:
    """Build a PyArrow schema by inspecting the first non-None value for each field."""
    field_types: dict[str, pa.DataType] = {}

    for record in records:
        for key, value in record.items():
            if key not in field_types and value is not None:
                field_types[key] = _infer_arrow_type(value)

    # Assign string type to any field that was always None
    all_keys = set()
    for record in records:
        all_keys.update(record.keys())

    fields = []
    for key in all_keys:
        arrow_type = field_types.get(key, pa.string())
        fields.append(pa.field(key, arrow_type, nullable=True))

    return pa.schema(fields)


def _normalize_value(value: Any, target_type: pa.DataType) -> Any:
    """Normalize a Python value for Arrow conversion."""
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        import json
        return json.dumps(value, default=str)
    if target_type == pa.string() and not isinstance(value, str):
        return str(value)
    return value


def records_to_parquet(
    records: list[dict],
    compression: str = "snappy",
) -> bytes:
    """Convert a list of dicts into a Parquet byte buffer.

    Args:
        records: List of row dicts with consistent keys.
        compression: Parquet compression codec ('snappy', 'gzip', 'zstd', 'none').

    Returns:
        Compressed Parquet file as bytes.
    """
    if not records:
        raise ValueError("Cannot create Parquet from empty records list")

    schema = _build_schema(records)

    # Build columnar arrays
    columns: dict[str, list] = {field.name: [] for field in schema}
    for record in records:
        for field in schema:
            value = record.get(field.name)
            columns[field.name].append(_normalize_value(value, field.type))

    # Create Arrow arrays
    arrays = []
    for field in schema:
        try:
            arr = pa.array(columns[field.name], type=field.type)
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            # Fallback: cast everything to string
            arr = pa.array([str(v) if v is not None else None for v in columns[field.name]], type=pa.string())
            field = pa.field(field.name, pa.string(), nullable=True)
        arrays.append(arr)

    table = pa.table(
        {field.name: arr for field, arr in zip(schema, arrays)},
    )

    # Write to buffer
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=compression)
    parquet_bytes = buf.getvalue()

    logger.info(
        "parquet_created",
        rows=len(records),
        columns=len(schema),
        size_bytes=len(parquet_bytes),
        compression=compression,
    )
    return parquet_bytes


def parquet_to_records(parquet_bytes: bytes) -> list[dict]:
    """Read Parquet bytes back into a list of dicts."""
    buf = io.BytesIO(parquet_bytes)
    table = pq.read_table(buf)
    return table.to_pydict()


def get_parquet_metadata(parquet_bytes: bytes) -> dict:
    """Extract metadata from a Parquet byte buffer."""
    buf = io.BytesIO(parquet_bytes)
    pf = pq.ParquetFile(buf)
    metadata = pf.metadata
    schema = pf.schema_arrow

    return {
        "num_rows": metadata.num_rows,
        "num_columns": metadata.num_columns,
        "num_row_groups": metadata.num_row_groups,
        "created_by": metadata.created_by,
        "columns": [
            {"name": field.name, "type": str(field.type), "nullable": field.nullable}
            for field in schema
        ],
    }
