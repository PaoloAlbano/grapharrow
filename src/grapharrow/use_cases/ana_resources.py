"""Bound retained ANA workflow state with a shared, thread-safe byte budget."""

from __future__ import annotations

import sys
from dataclasses import dataclass, fields, is_dataclass
from threading import Lock
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
from pydantic import BaseModel

from grapharrow.ana.handles import EntityPayload


class AnaQuotaExceeded(ValueError):
    """A workflow cannot retain the requested resources without exceeding its quota."""


@dataclass(frozen=True)
class AnaLimits:
    max_handles: int | None = None
    max_rows: int = 100_000
    max_bytes: int = 64 * 1024 * 1024

    def __post_init__(self) -> None:
        if self.max_handles is not None and (
            type(self.max_handles) is not int or self.max_handles <= 0
        ):
            raise ValueError("max_handles must be a positive integer or None")
        for name in ("max_rows", "max_bytes"):
            if type(getattr(self, name)) is not int or getattr(self, name) <= 0:
                raise ValueError(f"{name} must be a positive integer")


class AnaResourceBudget:
    """Admission accounting shared by the workflows of one HTTP application."""

    def __init__(self, max_bytes: int = 512 * 1024 * 1024) -> None:
        if type(max_bytes) is not int or max_bytes <= 0:
            raise ValueError("max_bytes must be a positive integer")
        self.max_bytes = max_bytes
        self._used_bytes = 0
        self._lock = Lock()

    @property
    def used_bytes(self) -> int:
        with self._lock:
            return self._used_bytes

    def adjust(self, delta: int) -> None:
        with self._lock:
            if self._used_bytes + delta > self.max_bytes:
                raise AnaQuotaExceeded(
                    "ANA server byte capacity reached; close a workflow or retry"
                )
            if self._used_bytes + delta < 0:
                raise RuntimeError("Unbalanced ANA resource accounting")
            self._used_bytes += delta


def retained_size(value: Any) -> int:
    """Estimate Python object graphs and Arrow buffers; deduplicate within one charge.

    Shared objects across handles are charged again, deliberately conservatively.
    This estimate is admission accounting, not an RSS or peak-allocation limit.
    """
    size = 0
    pending = [value]
    seen: set[int] = set()
    while pending:
        item = pending.pop()
        if id(item) in seen:
            continue
        seen.add(id(item))
        if isinstance(item, pa.Table):
            size += object.__sizeof__(item) + item.get_total_buffer_size()
        else:
            size += sys.getsizeof(item)
            if isinstance(item, str | bytes | int | float | bool | type(None)):
                continue
            elif isinstance(item, dict):
                pending.extend(item.keys())
                pending.extend(item.values())
                if isinstance(item, EntityPayload):
                    pending.append(item.reference)
            elif isinstance(item, list | tuple | set | frozenset):
                pending.extend(item)
            elif isinstance(item, BaseModel):
                pending.extend((vars(item), item.__pydantic_extra__))
            elif is_dataclass(item) and not isinstance(item, type):
                if hasattr(item, "__dict__"):
                    pending.append(vars(item))
                else:
                    pending.extend(getattr(item, field.name) for field in fields(item))
    return size


class AnaResources:
    """Track retained snapshot, handles, lineage and the most recent fetched page."""

    def __init__(self, limits: AnaLimits, budget: AnaResourceBudget, snapshot: Any) -> None:
        self.limits, self.budget = limits, budget
        self.snapshot_bytes = retained_size(snapshot)
        self.handles: dict[str, tuple[int, int, int]] = {}
        self.bytes = 0
        self._set_bytes(self.snapshot_bytes)

    def _set_bytes(self, value: int) -> None:
        if value > self.limits.max_bytes:
            raise AnaQuotaExceeded(
                "ANA workflow byte quota reached; use a smaller result or new workflow"
            )
        self.budget.adjust(value - self.bytes)
        self.bytes = value

    @property
    def usage(self) -> dict[str, int]:
        return {
            "handles": len(self.handles),
            "rows": sum(value[0] for value in self.handles.values()),
            "bytes": self.bytes,
            "snapshot_bytes": self.snapshot_bytes,
        }

    def reserve_handle(self, rows: list[dict[str, Any]], header: Any) -> tuple[int, int]:
        if self.limits.max_handles is not None and len(self.handles) >= self.limits.max_handles:
            raise AnaQuotaExceeded("ANA workflow handle quota reached; start a new workflow")
        if self.usage["rows"] + len(rows) > self.limits.max_rows:
            raise AnaQuotaExceeded(
                "ANA workflow row quota reached; reduce the result limit or start a new workflow"
            )
        row_bytes = retained_size(rows)
        size = row_bytes + retained_size(header)
        self._set_bytes(self.bytes + size)
        return row_bytes, size

    def finalize(self, store: Any, touched: set[str], last_fetch: Any) -> None:
        sizes = dict(self.handles)
        for key in touched:
            handle = store.handles[key]
            count, row_bytes, _ = sizes[key]
            # Shared operations publish rows only once; later finalization may
            # remove hidden ranking fields, or attach columns/metadata/lineage.
            # Keep the original row charge when fields shrink, and avoid a
            # second full walk of every entity payload on each call.
            if len(handle.rows) > count:
                raise RuntimeError("ANA handle rows grew after publication")
            header = (
                handle.id,
                handle.focus,
                handle.kind,
                handle.columns,
                handle.metadata,
                store.lineage.get(key),
            )
            size = (
                row_bytes
                + retained_size(header)
                + sys.getsizeof(handle)
                + sys.getsizeof(vars(handle))
            )
            sizes[key] = (len(handle.rows), row_bytes, size)
        if sum(value[0] for value in sizes.values()) > self.limits.max_rows:
            raise AnaQuotaExceeded("ANA workflow row quota reached")
        self._set_bytes(
            self.snapshot_bytes
            + sum(value[2] for value in sizes.values())
            + retained_size(last_fetch)
        )
        self.handles = sizes

    def restore(self, handles: dict[str, tuple[int, int, int]], size: int) -> None:
        self._set_bytes(size)
        self.handles = handles

    def close(self) -> None:
        self.budget.adjust(-self.bytes)
        self.bytes = 0
        self.snapshot_bytes = 0
        self.handles.clear()
