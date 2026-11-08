"""Low-level adapter around PyArrow Parquet operations."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.dataset as ds  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from grapharrow.adapters.filesystem import FileSystemAdapter
from grapharrow.adapters.scan_filter import dataset_filter
from grapharrow.domain.scan import ScanPredicate


class ParquetAdapter:
    """Encapsulate PyArrow Parquet reads and writes behind a small adapter."""

    def __init__(
        self, filesystem: FileSystemAdapter | None = None, *, filter_pushdown_enabled: bool = True
    ) -> None:
        self._filesystem = filesystem or FileSystemAdapter()
        self.filter_pushdown_enabled = filter_pushdown_enabled

    def read_table(
        self,
        path: str | Path | pa.NativeFile,
        *,
        schema: pa.Schema | None = None,
    ) -> pa.Table:
        """Read a Parquet table from ``path``."""

        if isinstance(path, pa.NativeFile):
            return pq.read_table(path, schema=schema)
        filesystem, normalized_path = self._filesystem.resolve(path)
        return pq.read_table(normalized_path, schema=schema, filesystem=filesystem)

    def write_table(self, table: pa.Table, path: str | Path, **kwargs: Any) -> None:
        """Write ``table`` to ``path`` as Parquet."""

        filesystem, normalized_path = self._filesystem.resolve(path)
        pq.write_table(table, normalized_path, filesystem=filesystem, **kwargs)

    def read_dataset_table(
        self,
        paths: list[str | Path],
        *,
        schema: pa.Schema | None = None,
        columns: list[str] | None = None,
        predicates: Sequence[ScanPredicate] = (),
    ) -> pa.Table:
        """Read one logical Parquet dataset from ``paths`` using ``pyarrow.dataset``."""

        if not paths:
            return pa.Table.from_pylist([], schema=schema)

        filesystem = None
        normalized_paths: list[str] = []
        for path in paths:
            resolved_filesystem, normalized_path = self._filesystem.resolve(path)
            if filesystem is None:
                filesystem = resolved_filesystem
            normalized_paths.append(normalized_path)

        dataset = ds.dataset(
            normalized_paths,
            filesystem=filesystem,
            format="parquet",
            schema=schema,
        )
        expression = (
            dataset_filter(dataset.schema, predicates) if self.filter_pushdown_enabled else None
        )
        try:
            return dataset.scanner(columns=columns, filter=expression).to_table()
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError):
            if expression is None:
                raise
            # Keep unsupported scanner kernels and schema coercions on the residual path.
            return dataset.scanner(columns=columns).to_table()
