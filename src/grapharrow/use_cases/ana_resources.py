"""Bound retained ANA workflow state with a shared, thread-safe byte budget."""

from __future__ import annotations

import sys
from collections.abc import Iterator
from contextlib import contextmanager
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

    def __init__(
        self,
        limits: AnaLimits,
        budget: AnaResourceBudget,
        snapshot: Any,
        *,
        shared_state: Any = None,
        reserved_bytes: int = 0,
    ) -> None:
        if type(reserved_bytes) is not int or reserved_bytes < 0:
            raise ValueError("Reserved bytes must be a non-negative integer")
        self.limits, self.budget = limits, budget
        self.snapshot_bytes = retained_size(snapshot)
        if shared_state is not None:
            # Charge a complete share even when it aliases this workflow's snapshot.
            self.snapshot_bytes += retained_size(shared_state)
        self.snapshot_bytes += reserved_bytes
        self.handles: dict[str, tuple[int, int, int]] = {}
        self._rows = 0
        self._handle_bytes = 0
        self._changes: dict[str, tuple[int, int, int] | None] | None = None
        self.bytes = 0
        self._set_bytes(self.snapshot_bytes)

    @contextmanager
    def track_changes(self) -> Iterator[None]:
        """Restore only modified charges and aggregate counters if a call fails."""
        if self._changes is not None:
            raise RuntimeError("An ANA resource call is already in progress")
        before_bytes, before_rows, before_handles = self.bytes, self._rows, self._handle_bytes
        changes: dict[str, tuple[int, int, int] | None] = {}
        self._changes = changes
        try:
            yield
        except BaseException:
            for key, previous in changes.items():
                if previous is None:
                    self.handles.pop(key, None)
                else:
                    self.handles[key] = previous
            self._rows, self._handle_bytes = before_rows, before_handles
            # Restore an existing admission without rechecking a changed workflow limit.
            self.budget.adjust(before_bytes - self.bytes)
            self.bytes = before_bytes
            raise
        finally:
            self._changes = None

    def _remember(self, key: str) -> None:
        if self._changes is not None and key not in self._changes:
            self._changes[key] = self.handles.get(key)

    def register_handle(self, key: str, count: int, row_bytes: int, size: int) -> None:
        """Record a reservation after its handle has been published successfully."""
        if key in self.handles:
            raise RuntimeError("ANA handle already has a resource charge")
        self._remember(key)
        self.handles[key] = (count, row_bytes, size)
        self._rows += count
        self._handle_bytes += size

    def _set_bytes(self, value: int) -> None:
        if value > self.limits.max_bytes:
            raise AnaQuotaExceeded(
                "ANA workflow byte quota reached; use a smaller result or new workflow"
            )
        self.budget.adjust(value - self.bytes)
        self.bytes = value

    def reserve_snapshot_capacity(self, size: int) -> bool:
        """Reserve optional acceleration during setup without rejecting the base view."""
        if type(size) is not int or size < 0:
            raise ValueError("Snapshot capacity must be a non-negative integer")
        if self._changes is not None:
            raise RuntimeError("Snapshot capacity must be reserved before tool calls")
        try:
            self._set_bytes(self.bytes + size)
        except AnaQuotaExceeded:
            return False
        self.snapshot_bytes += size
        return True

    @property
    def usage(self) -> dict[str, int]:
        return {
            "handles": len(self.handles),
            "rows": self._rows,
            "bytes": self.bytes,
            "snapshot_bytes": self.snapshot_bytes,
        }

    def reserve_handle(self, rows: list[dict[str, Any]], header: Any) -> tuple[int, int]:
        if self.limits.max_handles is not None and len(self.handles) >= self.limits.max_handles:
            raise AnaQuotaExceeded("ANA workflow handle quota reached; start a new workflow")
        if self._rows + len(rows) > self.limits.max_rows:
            raise AnaQuotaExceeded(
                "ANA workflow row quota reached; reduce the result limit or start a new workflow"
            )
        row_bytes = retained_size(rows)
        size = row_bytes + retained_size(header)
        self._set_bytes(self.bytes + size)
        return row_bytes, size

    def finalize(self, store: Any, touched: set[str], last_fetch: Any) -> None:
        sizes = {}
        total_rows, total_handles = self._rows, self._handle_bytes
        for key in touched:
            handle = store.handles[key]
            count, row_bytes, previous_size = self.handles[key]
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
            total_rows += len(handle.rows) - count
            total_handles += size - previous_size
        if total_rows > self.limits.max_rows:
            raise AnaQuotaExceeded("ANA workflow row quota reached")
        total_bytes = self.snapshot_bytes + total_handles + retained_size(last_fetch)
        for key, charge in sizes.items():
            self._remember(key)
            self.handles[key] = charge
        self._rows, self._handle_bytes = total_rows, total_handles
        # Allocate and journal updates before the final shared-budget admission.
        self._set_bytes(total_bytes)

    def close(self) -> None:
        self.budget.adjust(-self.bytes)
        self.bytes = 0
        self.snapshot_bytes = 0
        self.handles.clear()
        self._rows = self._handle_bytes = 0
