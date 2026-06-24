"""Low-level adapter around PyArrow Parquet operations."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.dataset as ds  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from grapharrow.adapters.filesystem import FileSystemAdapter


class ParquetAdapter:
    """Encapsulate PyArrow Parquet reads and writes behind a small adapter."""

    def __init__(self, filesystem: FileSystemAdapter | None = None) -> None:
        self._filesystem = filesystem or FileSystemAdapter()

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
        scanner = dataset.scanner(columns=columns)
        return scanner.to_table()
