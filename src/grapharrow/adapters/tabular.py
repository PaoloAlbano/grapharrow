"""Tabular batch operations built on top of PyArrow."""

from __future__ import annotations

from collections.abc import Sequence

import orjson
import pyarrow as pa  # type: ignore[import-untyped]

_JOIN_JSON_PREFIX = "__grapharrow_join_json__:"


class PyArrowTabularAdapter:
    """Encapsulate a minimal set of batch-oriented table operations."""

    def empty_table(self, schema: pa.Schema) -> pa.Table:
        """Return an empty table with ``schema``."""

        return pa.Table.from_pylist([], schema=schema)

    def concatenate_tables(
        self,
        tables: Sequence[pa.Table],
        *,
        schema: pa.Schema,
    ) -> pa.Table:
        """Concatenate ``tables`` or return an empty table with ``schema``."""

        if not tables:
            return self.empty_table(schema)
        return pa.concat_tables(list(tables), promote_options="default")

    def select_columns(self, table: pa.Table, columns: Sequence[str]) -> pa.Table:
        """Project a subset of ``columns`` from ``table``."""

        return table.select(list(columns))

    def sort_table(
        self,
        table: pa.Table,
        by: Sequence[tuple[str, str]],
    ) -> pa.Table:
        """Sort ``table`` by the provided Arrow sort keys."""

        return table.sort_by(list(by))

    def inner_join(
        self,
        left: pa.Table,
        right: pa.Table,
        *,
        left_keys: str | Sequence[str],
        right_keys: str | Sequence[str] | None = None,
    ) -> pa.Table:
        """Perform an inner join between ``left`` and ``right``."""

        resolved_right_keys = right_keys if right_keys is not None else left_keys
        return _encode_nested_join_columns(left).join(
            _encode_nested_join_columns(right),
            keys=left_keys,
            right_keys=resolved_right_keys,
            join_type="inner",
        )

    def left_join(
        self,
        left: pa.Table,
        right: pa.Table,
        *,
        left_keys: str | Sequence[str],
        right_keys: str | Sequence[str] | None = None,
    ) -> pa.Table:
        """Perform a left outer join between ``left`` and ``right``."""

        resolved_right_keys = right_keys if right_keys is not None else left_keys
        return _encode_nested_join_columns(left).join(
            _encode_nested_join_columns(right),
            keys=left_keys,
            right_keys=resolved_right_keys,
            join_type="left outer",
        )


def _encode_nested_join_columns(table: pa.Table) -> pa.Table:
    """Encode nested non-key values so Arrow joins can carry them safely."""
    updated = table
    for index, field in enumerate(table.schema):
        if not _requires_join_encoding(field.type):
            continue
        encoded = pa.array(
            [
                None if value is None else f"{_JOIN_JSON_PREFIX}{_json_dumps(value)}"
                for value in table[field.name].to_pylist()
            ],
            type=pa.string(),
        )
        updated = updated.set_column(index, field.name, encoded)
    return updated


def _json_dumps(value: object) -> str:
    """Serialize join payloads with orjson while preserving Arrow string columns."""
    return orjson.dumps(value).decode("utf-8")


def _requires_join_encoding(data_type: pa.DataType) -> bool:
    """Return true for Arrow nested types unsupported as join payload columns."""
    return bool(
        pa.types.is_list(data_type)
        or pa.types.is_large_list(data_type)
        or pa.types.is_struct(data_type)
        or pa.types.is_map(data_type)
    )
