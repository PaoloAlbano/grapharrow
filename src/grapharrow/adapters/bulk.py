"""Bulk input normalization helpers."""

from __future__ import annotations

from collections.abc import Mapping
from importlib import import_module
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]


class BulkInputAdapter:
    """Normalize supported Python batch inputs into Arrow tables."""

    def to_table(self, data: object) -> pa.Table:
        """Convert a supported bulk input into a ``pyarrow.Table``."""

        if isinstance(data, pa.Table):
            return data
        if self._is_pandas_dataframe(data):
            return pa.Table.from_pandas(data, preserve_index=False)
        if isinstance(data, list):
            return self._from_pylist(data)
        raise TypeError("Bulk ingest supports pyarrow.Table, pandas.DataFrame, and list[dict].")

    def _from_pylist(self, rows: list[object]) -> pa.Table:
        if not rows:
            return pa.Table.from_pylist([])
        normalized_rows: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, Mapping):
                raise TypeError("Bulk ingest list inputs must contain only dictionaries.")
            normalized_rows.append(dict(row))
        return pa.Table.from_pylist(normalized_rows)

    def _is_pandas_dataframe(self, value: object) -> bool:
        try:
            pandas = import_module("pandas")
        except ImportError:
            return False
        dataframe_type = getattr(pandas, "DataFrame", None)
        return dataframe_type is not None and isinstance(value, dataframe_type)


__all__ = ["BulkInputAdapter"]
