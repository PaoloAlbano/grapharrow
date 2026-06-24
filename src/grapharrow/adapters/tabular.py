"""Tabular batch operations built on top of PyArrow."""

from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa  # type: ignore[import-untyped]


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
        return left.join(
            right,
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
        return left.join(
            right,
            keys=left_keys,
            right_keys=resolved_right_keys,
            join_type="left outer",
        )
