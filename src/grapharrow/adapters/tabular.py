"""Tabular batch operations built on top of PyArrow."""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np
import orjson
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.compute as pc  # type: ignore[import-untyped]

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

    def sort_window_table(
        self,
        table: pa.Table,
        by: Sequence[tuple[str, str]],
        *,
        skip: int,
        limit: int,
    ) -> pa.Table:
        """Sort selected candidates only, retaining legacy null placement."""
        keys = [(name, direction, "at_end") for name, direction in by]
        return table.take(sort_window_indices(table, keys, skip=skip, limit=limit))

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


def sort_window_indices(
    table: pa.Table,
    by: Sequence[tuple[str, str, str]],
    *,
    skip: int = 0,
    limit: int | None = None,
    top_k: bool = True,
) -> pa.Array:
    """Stable mixed-direction ordering, with O(k) candidate indices for small windows."""
    if limit == 0 or skip >= table.num_rows:
        return pa.array([], type=pa.uint64())
    columns = {f"key_{i}": table[name] for i, (name, _, _) in enumerate(by)}
    # select_k is unstable; a unique final ordinal makes every comparison total
    # and preserves input order for ties, including null and NaN values.
    columns["ordinal"] = pa.array(np.arange(table.num_rows, dtype=np.int64))
    keys = [(f"key_{i}", direction, nulls) for i, (_, direction, nulls) in enumerate(by)]
    keys.append(("ordinal", "ascending", "at_end"))
    ordering = pa.table(columns)
    bound = table.num_rows if limit is None else min(skip + limit, table.num_rows)
    if top_k and limit is not None and bound <= table.num_rows // 4:
        try:
            candidates = pc.select_k_unstable(ordering, k=bound, sort_keys=keys)
            ordered = pc.sort_indices(ordering.take(candidates), sort_keys=keys)
            return pc.take(candidates, ordered).slice(skip, limit)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError):
            pass
    indices = pc.sort_indices(ordering, sort_keys=keys)
    return indices.slice(skip, limit)


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
