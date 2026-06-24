from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.fs as pafs  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from grapharrow.adapters import FileSystemAdapter


@dataclass(frozen=True, slots=True)
class MotoS3Context:
    filesystem: FileSystemAdapter
    client: Any
    bucket_name: str
    bucket_uri: str
    endpoint_url: str


@dataclass(frozen=True, slots=True)
class LocalDiskContext:
    root_dir: Path
    database_dir: Path


@dataclass(frozen=True, slots=True)
class RemoteObjectStorageContext:
    filesystem: FileSystemAdapter
    provider: str
    root_uri: str
    database_uri: str


def list_object_keys(context: MotoS3Context, *, prefix: str) -> list[str]:
    response = cast(
        dict[str, Any],
        context.client.list_objects_v2(Bucket=context.bucket_name, Prefix=prefix),
    )
    contents = cast(list[dict[str, Any]], response.get("Contents", []))
    return sorted(str(item["Key"]) for item in contents)


def load_json_object(context: MotoS3Context, relative_path: str) -> dict[str, object]:
    response = cast(
        dict[str, Any],
        context.client.get_object(
            Bucket=context.bucket_name,
            Key=f"example-db/{relative_path}",
        ),
    )
    body = cast(Any, response["Body"])
    payload = body.read().decode("utf-8")
    return cast(dict[str, object], json.loads(payload))


def read_parquet_object(context: MotoS3Context, relative_path: str) -> pa.Table:
    response = cast(
        dict[str, Any],
        context.client.get_object(
            Bucket=context.bucket_name,
            Key=f"example-db/{relative_path}",
        ),
    )
    body = cast(Any, response["Body"])
    payload = cast(bytes, body.read())
    return pq.read_table(pa.BufferReader(payload))


def has_parquet_magic(context: MotoS3Context, relative_path: str) -> bool:
    response = cast(
        dict[str, Any],
        context.client.get_object(
            Bucket=context.bucket_name,
            Key=f"example-db/{relative_path}",
        ),
    )
    body = cast(Any, response["Body"])
    payload = cast(bytes, body.read())
    return payload[:4] == b"PAR1" and payload[-4:] == b"PAR1"


def assert_expected_storage_layout(
    *,
    keys: list[str],
    node_file: str,
    edge_file: str,
    node_label_file: str,
    adjacency_file: str,
    node_property_file: str | None = None,
    edge_property_file: str | None = None,
) -> None:
    expected_keys = {
        "example-db/current_snapshot.json",
        "example-db/catalog/current_catalog.json",
        f"example-db/{node_file}",
        f"example-db/{edge_file}",
        f"example-db/{node_label_file}",
        f"example-db/{adjacency_file}",
    }
    if node_property_file is not None:
        expected_keys.add(f"example-db/{node_property_file}")
    if edge_property_file is not None:
        expected_keys.add(f"example-db/{edge_property_file}")
    assert expected_keys.issubset(set(keys))
    assert any(key.startswith("example-db/catalog/catalog-") for key in keys)
    assert any(key.startswith("example-db/manifests/snapshot-") for key in keys)
    assert f"example-db/{node_file}".startswith("example-db/data/nodes/")
    assert f"example-db/{edge_file}".startswith("example-db/data/edges/")
    assert f"example-db/{node_label_file}".startswith("example-db/derived/node_labels/")
    assert f"example-db/{adjacency_file}".startswith("example-db/derived/adjacency/by_source/")
    if node_property_file is not None:
        assert f"example-db/{node_property_file}".startswith("example-db/derived/node_properties/")
    if edge_property_file is not None:
        assert f"example-db/{edge_property_file}".startswith("example-db/derived/edge_properties/")
    assert not any(key.startswith("example-db/staged/tx-") for key in keys)


def list_local_paths(root_dir: Path) -> list[str]:
    return sorted(str(path.relative_to(root_dir)) for path in root_dir.rglob("*") if path.is_file())


def load_local_json_object(database_dir: Path, relative_path: str) -> dict[str, object]:
    return cast(
        dict[str, object],
        json.loads((database_dir / relative_path).read_text(encoding="utf-8")),
    )


def read_local_parquet_object(database_dir: Path, relative_path: str) -> pa.Table:
    return pq.read_table(database_dir / relative_path)


def has_local_parquet_magic(database_dir: Path, relative_path: str) -> bool:
    payload = (database_dir / relative_path).read_bytes()
    return payload[:4] == b"PAR1" and payload[-4:] == b"PAR1"


def assert_expected_local_storage_layout(
    *,
    root_dir: Path,
    database_dir: Path,
    node_file: str,
    edge_file: str,
    node_label_file: str,
    adjacency_file: str,
    node_property_file: str | None = None,
    edge_property_file: str | None = None,
) -> None:
    paths = list_local_paths(root_dir)
    database_name = database_dir.name
    expected_paths = {
        f"{database_name}/current_snapshot.json",
        f"{database_name}/catalog/current_catalog.json",
        f"{database_name}/{node_file}",
        f"{database_name}/{edge_file}",
        f"{database_name}/{node_label_file}",
        f"{database_name}/{adjacency_file}",
    }
    if node_property_file is not None:
        expected_paths.add(f"{database_name}/{node_property_file}")
    if edge_property_file is not None:
        expected_paths.add(f"{database_name}/{edge_property_file}")
    assert expected_paths.issubset(set(paths))
    assert any(path.startswith(f"{database_name}/catalog/catalog-") for path in paths)
    assert any(path.startswith(f"{database_name}/manifests/snapshot-") for path in paths)
    assert f"{database_name}/{node_file}".startswith(f"{database_name}/data/nodes/")
    assert f"{database_name}/{edge_file}".startswith(f"{database_name}/data/edges/")
    assert f"{database_name}/{node_label_file}".startswith(f"{database_name}/derived/node_labels/")
    assert f"{database_name}/{adjacency_file}".startswith(
        f"{database_name}/derived/adjacency/by_source/"
    )
    if node_property_file is not None:
        assert f"{database_name}/{node_property_file}".startswith(
            f"{database_name}/derived/node_properties/"
        )
    if edge_property_file is not None:
        assert f"{database_name}/{edge_property_file}".startswith(
            f"{database_name}/derived/edge_properties/"
        )
    assert not any(path.startswith(f"{database_name}/staged/tx-") for path in paths)


def list_remote_paths(context: RemoteObjectStorageContext) -> list[str]:
    return sorted(context.filesystem.list_paths(context.database_uri, recursive=True))


def load_remote_json_object(
    context: RemoteObjectStorageContext,
    relative_path: str,
) -> dict[str, object]:
    payload = context.filesystem.read_json(f"{context.database_uri}/{relative_path}")
    return cast(dict[str, object], payload)


def read_remote_parquet_object(
    context: RemoteObjectStorageContext,
    relative_path: str,
) -> pa.Table:
    location = f"{context.database_uri}/{relative_path}"
    filesystem, normalized_path = context.filesystem.resolve(location)
    return pq.read_table(normalized_path, filesystem=cast(pafs.FileSystem, filesystem))


def has_remote_parquet_magic(
    context: RemoteObjectStorageContext,
    relative_path: str,
) -> bool:
    location = f"{context.database_uri}/{relative_path}"
    filesystem, normalized_path = context.filesystem.resolve(location)
    with cast(pafs.FileSystem, filesystem).open_input_file(normalized_path) as handle:
        payload = cast(bytes, handle.read())
    return payload[:4] == b"PAR1" and payload[-4:] == b"PAR1"


def assert_expected_remote_storage_layout(
    *,
    paths: list[str],
    database_uri: str,
    node_file: str,
    edge_file: str,
    node_label_file: str,
    adjacency_file: str,
) -> None:
    prefix = database_uri.rstrip("/")
    expected_paths = {
        f"{prefix}/current_snapshot.json",
        f"{prefix}/catalog/current_catalog.json",
        f"{prefix}/{node_file}",
        f"{prefix}/{edge_file}",
        f"{prefix}/{node_label_file}",
        f"{prefix}/{adjacency_file}",
    }
    assert expected_paths.issubset(set(paths))
    assert any(path.startswith(f"{prefix}/catalog/catalog-") for path in paths)
    assert any(path.startswith(f"{prefix}/manifests/snapshot-") for path in paths)
    assert f"{prefix}/{node_file}".startswith(f"{prefix}/data/nodes/")
    assert f"{prefix}/{edge_file}".startswith(f"{prefix}/data/edges/")
    assert f"{prefix}/{node_label_file}".startswith(f"{prefix}/derived/node_labels/")
    assert f"{prefix}/{adjacency_file}".startswith(f"{prefix}/derived/adjacency/by_source/")
    assert not any(path.startswith(f"{prefix}/staged/tx-") for path in paths)
