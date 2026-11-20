"""Read-only Delta storage accounting against a pinned table version."""

from __future__ import annotations

import re
from collections.abc import Collection, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import orjson
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.fs as pafs  # type: ignore[import-untyped]
from deltalake import DeltaTable
from deltalake.exceptions import DeltaError
from deltalake.fs import DeltaStorageHandler

from grapharrow.adapters import FileSystemAdapter
from grapharrow.core.errors import StorageError
from grapharrow.storage.models import (
    DeltaStorageInventory,
    RetentionFile,
    RetentionPlan,
    StorageFileSummary,
)

_LOG_FILE = re.compile(r"_delta_log/([0-9]{20})\.(json|checkpoint\.parquet)")


@dataclass(slots=True)
class _StorageFiles:
    parquet: dict[str, pafs.FileInfo]
    commits: dict[str, pafs.FileInfo]
    checkpoints: dict[int, pafs.FileInfo]


def _list_storage(
    filesystem: FileSystemAdapter, location: str | Path, version: int
) -> _StorageFiles:
    files = _StorageFiles({}, {}, {})
    storage, base = filesystem.resolve(location)
    prefix = base.rstrip("/") + "/"
    # Use Arrow's native listing so a slow object store leaves other threads free.
    for info in storage.get_file_info(pafs.FileSelector(base, recursive=True)):
        if info.type != pafs.FileType.File:
            continue
        path = info.path.removeprefix(prefix)
        if path.startswith("kind=") and path.endswith(".parquet"):
            files.parquet[path] = info
            continue
        match = _LOG_FILE.fullmatch(path)
        if match is None or (log_version := int(match[1])) > version:
            continue
        if match[2] == "json":
            files.commits[path] = info
        else:
            files.checkpoints[log_version] = info

    # GraphArrow retains its commit JSON even after checkpointing.
    if len(files.commits) != version + 1:
        raise StorageError("Delta storage inspection requires the complete commit log.")
    return files


def _active_paths(table: DeltaTable) -> set[str]:
    return {unquote(path) for path in pa.table(table.get_add_actions()).column("path").to_pylist()}


def inspect_delta_storage(
    table: DeltaTable,
    filesystem: FileSystemAdapter,
    location: str | Path,
    storage_options: dict[str, str],
) -> DeltaStorageInventory:
    version = table.version()
    try:
        active_paths = _active_paths(table)
        files = _list_storage(filesystem, location, version)
        active = [files.parquet.pop(path) for path in active_paths if path in files.parquet]
        historical = []
        if files.parquet:
            for _, action in _commit_actions(table.table_uri, storage_options, files.commits):
                if "remove" in action:
                    removed = files.parquet.pop(_action_path(action["remove"]), None)
                    if removed is not None:
                        historical.append(removed)
                if not files.parquet:
                    break
    except (DeltaError, OSError, pa.ArrowException, ValueError) as exc:
        raise StorageError("Could not read the Delta storage inventory.") from exc

    checkpoint = max(files.checkpoints, default=None)
    return DeltaStorageInventory(
        head_snapshot_id=version + 1,
        delta_version=version,
        latest_checkpoint_version=checkpoint,
        commits_since_checkpoint=version + 1 if checkpoint is None else version - checkpoint,
        active_parquet=_summarize(active),
        historical_parquet=_summarize(historical),
        untracked_parquet=_summarize(files.parquet.values()),
        commit_logs=_summarize(files.commits.values()),
        checkpoints=_summarize(files.checkpoints.values()),
        missing_active_parquet_count=len(active_paths) - len(active),
    )


def plan_delta_retention(
    table: DeltaTable,
    filesystem: FileSystemAdapter,
    location: str | Path,
    storage_options: dict[str, str],
    *,
    keep_last: int,
    min_age_hours: int,
    protect_from_snapshot_id: int | None,
    minimum_snapshot_id: int = 1,
) -> RetentionPlan:
    version = table.version()
    if protect_from_snapshot_id is not None and not 1 <= protect_from_snapshot_id <= version + 1:
        raise ValueError("protect_from_snapshot_id must identify a published snapshot.")
    if protect_from_snapshot_id is not None and protect_from_snapshot_id < minimum_snapshot_id:
        raise StorageError("The protected Delta snapshot has already expired.")

    now = datetime.now(UTC)
    age_cutoff_ms = int(now.timestamp() * 1000) - min_age_hours * 3_600_000
    oldest = max(0, version - keep_last + 1)
    if protect_from_snapshot_id is not None:
        oldest = min(oldest, protect_from_snapshot_id - 1)
    additions: dict[str, int] = {}
    removals: dict[str, int] = {}
    timestamps: set[int] = set()
    try:
        files = _list_storage(filesystem, location, version)
        for commit, action in _commit_actions(table.table_uri, storage_options, files.commits):
            if "add" in action:
                path = _action_path(action["add"])
                if path in additions:
                    raise ValueError(f"Delta file path was reused: {path}.")
                additions[path] = commit
            elif "remove" in action:
                path = _action_path(action["remove"])
                if path not in additions or additions[path] >= commit or path in removals:
                    raise ValueError(f"Unexpected removal of Delta file: {path}.")
                removals[path] = commit
            elif min_age_hours and "commitInfo" in action:
                info = action["commitInfo"]
                timestamp = info.get("timestamp") if isinstance(info, dict) else None
                if type(timestamp) is not int or commit in timestamps:
                    raise ValueError(f"Invalid Delta commit timestamp at version {commit}.")
                timestamps.add(commit)
                if timestamp >= age_cutoff_ms:
                    oldest = min(oldest, commit)

        if min_age_hours and len(timestamps) != version + 1:
            raise StorageError("Delta retention planning requires all commit timestamps.")
        if additions.keys() - removals.keys() != _active_paths(table):
            raise StorageError("Delta retention history does not match the captured snapshot.")
    except (DeltaError, OSError, pa.ArrowException, ValueError) as exc:
        raise StorageError("Could not read Delta retention history.") from exc

    oldest = max(oldest, minimum_snapshot_id - 1)
    # Removal at version N makes the file absent in N itself. A file still
    # present at the retained boundary belongs to both parts of the history.
    candidates: list[RetentionFile] = []
    missing: list[str] = []
    shared: list[str] = []
    for path, added in sorted(additions.items()):
        removed = removals.get(path)
        relative = f"delta/{path}"
        if removed is not None and removed <= oldest:
            info = files.parquet.get(path)
            if info is None:
                missing.append(relative)
            else:
                candidates.append(RetentionFile(relative, info.size))
        elif path not in files.parquet:
            raise StorageError(f"Missing Parquet file in the retained Delta range: {relative}.")
        elif added < oldest:
            shared.append(relative)

    return RetentionPlan(
        head_snapshot_id=version + 1,
        evaluated_at=now.isoformat(),
        keep_last=keep_last,
        min_age_hours=min_age_hours,
        retained_snapshot_ids=tuple(range(version + 1, oldest, -1)),
        older_snapshot_ids=tuple(range(oldest, 0, -1)),
        shared_parquet_paths=tuple(shared),
        candidate_files=tuple(candidates),
        missing_candidate_paths=tuple(missing),
        estimated_reclaimable_bytes=sum(file.size_bytes for file in candidates),
    )


def _commit_actions(
    uri: str,
    options: dict[str, str],
    commits: dict[str, pafs.FileInfo],
) -> Iterator[tuple[int, dict[str, Any]]]:
    storage = DeltaStorageHandler(
        uri, options, known_sizes={path: info.size for path, info in commits.items()}
    )
    for log_path in sorted(commits):
        version = int(log_path.removeprefix("_delta_log/").removesuffix(".json"))
        with storage.open_input_file(log_path) as handle:
            lines = handle.read().splitlines()
        for line in lines:
            action = orjson.loads(line)
            if not isinstance(action, dict):
                raise ValueError(f"Invalid action in Delta log {log_path}.")
            yield version, action


def _action_path(action: object) -> str:
    path = action.get("path") if isinstance(action, dict) else None
    if not isinstance(path, str):
        raise ValueError("Delta file action is missing a path.")
    path = unquote(path)
    partition, _, filename = path.partition("/")
    if (
        not partition.startswith("kind=")
        or "/" in filename
        or "\\" in path
        or not filename.endswith(".parquet")
    ):
        raise ValueError("Unexpected Delta partition file path.")
    return path


def _summarize(files: Collection[pafs.FileInfo]) -> StorageFileSummary:
    times = [modified for info in files if (modified := info.mtime) is not None]
    return StorageFileSummary(
        file_count=len(files),
        size_bytes=sum(info.size for info in files),
        oldest_modified_at=min(times).isoformat() if times else None,
        newest_modified_at=max(times).isoformat() if times else None,
    )
