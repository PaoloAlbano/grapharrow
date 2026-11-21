"""Arrow IPC negotiation and conversion for the REST tabular endpoints."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from io import RawIOBase
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
from fastapi import HTTPException, Response
from fastapi.responses import StreamingResponse
from pydantic import TypeAdapter

ARROW_STREAM = "application/vnd.apache.arrow.stream"
ARROW_FILE = "application/vnd.apache.arrow.file"
_JSON = "application/json"
_ARROW_TYPES = (ARROW_STREAM, ARROW_FILE)
_SCALAR_TYPES = {str, int, bool, float}
_JSON_VALUE = TypeAdapter(Any)
_BATCH_ROWS = 8192
ARROW_CONTENT = {
    media_type: {"schema": {"type": "string", "format": "binary"}} for media_type in _ARROW_TYPES
}


def arrow_response_type(accept: str) -> str | None:
    """Select an explicitly requested Arrow representation by quality and order."""
    choices: list[tuple[float, int, str]] = []
    for position, value in enumerate(accept.split(",")):
        media_type, *parameters = (part.strip().lower() for part in value.split(";"))
        if media_type not in (*_ARROW_TYPES, _JSON):
            continue
        quality = 1.0
        for parameter in parameters:
            name, _, raw = parameter.partition("=")
            if name.strip() == "q":
                try:
                    quality = float(raw)
                except ValueError:
                    quality = 0
                break
        if 0 < quality <= 1:
            choices.append((quality, -position, media_type))
    selected = max(choices, default=(0.0, 0, _JSON))[2]
    return selected if selected in _ARROW_TYPES else None


def read_arrow_body(payload: bytes, content_type: str) -> pa.Table:
    """Decode and validate a complete IPC payload before starting a write."""
    media_type = content_type.split(";", 1)[0].strip().lower()
    if media_type == _JSON or media_type.endswith("+json"):
        raise HTTPException(422, "Expected a JSON bulk object.")
    if media_type not in _ARROW_TYPES:
        raise HTTPException(415, "Expected JSON or an Arrow IPC Content-Type.")
    try:
        reader = pa.BufferReader(payload)
        open_ipc = pa.ipc.open_stream if media_type == ARROW_STREAM else pa.ipc.open_file
        table = open_ipc(reader).read_all()
    except (pa.ArrowException, ValueError, OSError) as exc:
        raise HTTPException(422, "Invalid Arrow IPC payload.") from exc
    if len(set(table.column_names)) != table.num_columns:
        raise HTTPException(422, "Arrow IPC input must have unique column names.")
    return table


def _result_column(name: str, values: list[Any]) -> tuple[pa.Field, pa.Array]:
    present = [value for value in values if value is not None]
    native = _uniform_scalars(present) or (
        bool(present)
        and all(isinstance(value, list) for value in present)
        and _uniform_scalars(item for value in present for item in value if item is not None)
    )
    if native:
        try:
            column = pa.array(values)
            return pa.field(name, column.type), column
        except (OverflowError, pa.ArrowException):
            pass
    # A JSON column preserves heterogeneous values and missing keys without Arrow
    # inferring a lossy numeric promotion or filling sparse objects with nulls.
    column = pa.array(
        [_JSON_VALUE.dump_json(value).decode() if value is not None else None for value in values],
        type=pa.string(),
    )
    return pa.field(name, column.type, metadata={b"grapharrow.encoding": b"json"}), column


def _uniform_scalars(values: Iterable[Any]) -> bool:
    kinds = {str if isinstance(value, str) else type(value) for value in values}
    return len(kinds) <= 1 and kinds <= _SCALAR_TYPES


def arrow_rows_response(
    rows: Sequence[Mapping[str, Any]], columns: Sequence[str], media_type: str
) -> Response:
    """Encode REST result rows as one Arrow IPC stream or file."""
    fields_and_arrays = [_result_column(name, [row.get(name) for row in rows]) for name in columns]
    schema = pa.schema([field for field, _ in fields_and_arrays])
    table = (
        pa.Table.from_arrays([array for _, array in fields_and_arrays], schema=schema)
        if fields_and_arrays
        else pa.table({"empty": pa.nulls(len(rows))}).select([])
    )
    return _buffered_arrow_response(table, media_type)


def arrow_table_response(table: pa.Table, media_type: str) -> Response:
    """Send a columnar query result without rebuilding its rows or buffering an IPC stream."""
    if media_type == ARROW_STREAM:
        return StreamingResponse(
            _ipc_batches(table), media_type=media_type, headers={"Vary": "Accept"}
        )
    return _buffered_arrow_response(table, media_type)


def _buffered_arrow_response(table: pa.Table, media_type: str) -> Response:
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream if media_type == ARROW_STREAM else pa.ipc.new_file
    with writer(sink, table.schema) as output:
        output.write_table(table)
    return Response(memoryview(sink.getvalue()), media_type=media_type, headers={"Vary": "Accept"})


class _BatchBuffer(RawIOBase):
    """Drain encoded batches while retaining the absolute IPC stream position."""

    def __init__(self) -> None:
        self._payload = bytearray()
        self._position = 0

    def writable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._position

    def write(self, data: Any) -> int:
        self._payload.extend(data)
        self._position += len(data)
        return len(data)

    def drain(self) -> bytes:
        payload = bytes(self._payload)
        self._payload.clear()
        return payload


def _ipc_batches(table: pa.Table) -> Iterator[bytes]:
    sink = _BatchBuffer()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        for batch in table.to_reader(max_chunksize=_BATCH_ROWS):
            writer.write_batch(batch)
            yield sink.drain()
    yield sink.drain()
