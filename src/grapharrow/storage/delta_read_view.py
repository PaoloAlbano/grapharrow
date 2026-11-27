"""Read one Delta version independently of the cursor that captured it."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from functools import cached_property

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.dataset as ds  # type: ignore[import-untyped]
import pyarrow.fs as pafs  # type: ignore[import-untyped]
from deltalake import DeltaTable
from deltalake.fs import DeltaStorageHandler


@dataclass(frozen=True)
class DeltaReadView:
    """Keep file descriptors for one snapshot and build fragments on demand."""

    _version: int
    description: str | None
    _schema: pa.Schema
    _schema_json: str
    actions: pa.Table
    _files: dict[str, tuple[tuple[str, int], ...]]
    _uri: str
    _options: dict[str, str] = field(repr=False)

    @classmethod
    def capture(cls, cursor: DeltaTable, uri: str, options: dict[str, str]) -> DeltaReadView | None:
        metadata = cursor.metadata()
        if cursor.protocol().min_reader_version != 1 or metadata.partition_columns != ["kind"]:
            return None
        delta_schema = cursor.schema()
        schema = pa.schema(delta_schema.to_arrow())
        if schema.get_field_index("kind") == -1 or schema.field("kind").type != pa.string():
            return None
        actions = pa.table(cursor.get_add_actions(flatten=True))
        columns = ["path", "partition.kind", "num_records"]
        if not {*columns, "size_bytes"}.issubset(actions.column_names):
            return None
        grouped: dict[str, list[tuple[str, int]]] = defaultdict(list)
        for path, kind, size in zip(
            actions["path"].to_pylist(),
            actions["partition.kind"].to_pylist(),
            actions["size_bytes"].to_pylist(),
            strict=True,
        ):
            if not isinstance(path, str) or not isinstance(kind, str) or type(size) is not int:
                return None
            grouped[kind].append((path, size))
        return cls(
            cursor.version(),
            metadata.description,
            schema,
            delta_schema.to_json(),
            actions.select([*columns, "size_bytes"]),
            {kind: tuple(files) for kind, files in grouped.items()},
            uri.rstrip("/"),
            options,
        )

    def version(self) -> int:
        return self._version

    def metadata_file(self) -> tuple[str, str | None, str] | None:
        files = self._files.get("metadata", ())
        if len(files) != 1:
            return None
        return f"{self._uri}/{files[0][0]}", self.description, self._schema_json

    @cached_property
    def _filesystem(self) -> pafs.FileSystem:
        sizes = {path: size for files in self._files.values() for path, size in files}
        return pafs.PyFileSystem(DeltaStorageHandler(self._uri, self._options, known_sizes=sizes))

    @cached_property
    def _format(self) -> ds.ParquetFileFormat:
        return ds.ParquetFileFormat(
            default_fragment_scan_options=ds.ParquetFragmentScanOptions(pre_buffer=True)
        )

    def partition_dataset(self, kind: str, *, include_ranges: bool) -> ds.Dataset:
        filesystem = self._filesystem
        parquet = self._format
        kinds = (
            (label for label in self._files if label == kind or label.startswith(f"{kind}-"))
            if include_ranges
            else (kind,)
        )
        fragments: list[ds.ParquetFileFragment] = []
        for label in kinds:
            predicate = ds.field("kind") == label
            fragments.extend(
                parquet.make_fragment(path, filesystem=filesystem, partition_expression=predicate)
                for path, _ in self._files.get(label, ())
            )
        return ds.FileSystemDataset(fragments, self._schema, parquet, filesystem)
