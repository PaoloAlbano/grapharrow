"""Immutable index pages for receipts in published Native manifests."""

from __future__ import annotations

from collections.abc import Iterable
from uuid import uuid4

from grapharrow.adapters import FileSystemAdapter
from grapharrow.core.errors import StorageError
from grapharrow.core.layout import Location
from grapharrow.domain import SnapshotId


class IdempotencyIndex:
    """Map key hashes to receipt manifests, with one shard per first hash byte."""

    def __init__(self, root: Location, filesystem: FileSystemAdapter) -> None:
        self._root = root
        self._filesystem = filesystem

    def lookup(self, root_path: str, key_hash: str) -> str | None:
        directory = self._read_page(root_path, value_prefix="idempotency/")
        shard = directory.get(key_hash[:2])
        if shard is None:
            return None
        return self._read_page(shard, value_prefix="manifests/").get(key_hash)

    def stage(
        self,
        *,
        previous_root: str | None,
        snapshot_id: SnapshotId,
        key_hash: str,
        manifest_path: str,
        legacy_entries: Iterable[tuple[str, str]] = (),
    ) -> tuple[str, tuple[str, ...]]:
        """Stage a replacement root and changed shards for the owning commit."""
        prefix = key_hash[:2]
        directory: dict[str, str] = {}
        shards: dict[str, dict[str, str]] = {}
        if previous_root is None:
            for old_key, old_manifest in legacy_entries:
                shards.setdefault(old_key[:2], {}).setdefault(old_key, old_manifest)
        else:
            directory = self._read_page(previous_root, value_prefix="idempotency/")
            if (shard := directory.get(prefix)) is not None:
                shards[prefix] = self._read_page(shard, value_prefix="manifests/")

        updated = shards.setdefault(prefix, {})
        if key_hash in updated:
            raise StorageError("An idempotency receipt already exists for this commit key.")
        updated[key_hash] = manifest_path

        staged = []
        for shard_prefix, entries in shards.items():
            path = self._write_page(snapshot_id, entries)
            directory[shard_prefix] = path
            staged.append(path)
        root = self._write_page(snapshot_id, directory)
        return root, (*staged, root)

    def _read_page(self, path: str, *, value_prefix: str) -> dict[str, str]:
        if not _is_path_in(path, "idempotency/"):
            raise StorageError("An idempotency index path is invalid.")
        try:
            page = self._filesystem.read_json(f"{self._root!s}/{path}")
        except (FileNotFoundError, ValueError) as exc:
            raise StorageError("An idempotency index page cannot be read.") from exc
        if not isinstance(page, dict):
            raise StorageError("An idempotency index page is invalid.")
        entries: dict[str, str] = {}
        for key, value in page.items():
            if not isinstance(key, str) or not _is_path_in(value, value_prefix):
                raise StorageError("An idempotency index entry is invalid.")
            entries[key] = value
        return entries

    def _write_page(self, snapshot_id: SnapshotId, page: dict[str, str]) -> str:
        path = f"idempotency/snapshot-{snapshot_id.value:06d}-{uuid4().hex}.json"
        self._filesystem.write_json(f"{self._root!s}/{path}", page)
        return path


def _is_path_in(value: object, prefix: str) -> bool:
    if not isinstance(value, str) or not value.startswith(prefix):
        return False
    name = value.removeprefix(prefix)
    return name.startswith("snapshot-") and name.endswith(".json") and "/" not in name
