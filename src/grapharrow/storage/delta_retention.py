"""Durable snapshot admission for Delta databases with managed retention."""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from socket import gethostname
from threading import RLock
from typing import Literal
from uuid import uuid4

from grapharrow.adapters.filesystem import FileSystemAdapter
from grapharrow.adapters.object_pointer import Revision
from grapharrow.adapters.writer import WriterCoordinator
from grapharrow.core import DatabaseLayout
from grapharrow.core.errors import StorageError
from grapharrow.storage.delta_fencing import local_incarnation_lock, verify_external_fence
from grapharrow.storage.models import (
    DeltaPinRecoveryResult,
    DeltaRetentionPin,
    DeltaRetentionStatus,
)

MANAGED_DESCRIPTION = "GraphArrow managed retention with replica fencing v1"


def _valid_incarnation_id(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 32
        and all(char in "0123456789abcdef" for char in value)
    )


@dataclass(frozen=True, slots=True)
class _Cleanup:
    token: str
    online: bool


@dataclass(frozen=True, slots=True)
class _Pin:
    replica_id: str
    incarnation_id: str
    snapshot_id: int
    acquired_at: str


@dataclass(frozen=True, slots=True)
class _Admission:
    minimum_snapshot_id: int
    pins: dict[str, _Pin]
    cleanup: _Cleanup | None = None
    fenced_incarnations: tuple[str, ...] = ()


@dataclass(slots=True)
class _LocalPin:
    token: str
    references: int = 1


class DeltaRetentionCoordinator:
    """Atomically order new snapshot pins and monotonically advancing cutoffs."""

    def __init__(
        self, layout: DatabaseLayout, filesystem: FileSystemAdapter, replica_id: str | None = None
    ) -> None:
        self._path = layout.delta_admission_path
        self._database_location = str(layout.root)
        self._filesystem = filesystem
        self._lock = WriterCoordinator(layout.root, filesystem)
        self._pid = os.getpid()
        self._replica_id = replica_id or gethostname()
        self._incarnation_id = uuid4().hex
        self._local_lock = RLock()
        self._local_pins: dict[int, _LocalPin] = {}
        self._local_root = self._lock.local_root if os.name == "posix" else None
        self._witness = ExitStack()

    def initialize(self) -> None:
        with self._lock.hold():
            if self._filesystem.read_pointer(self._path) is None:
                self._filesystem.replace_pointer(self._path, asdict(_Admission(1, {})), None)

    def _read(self) -> tuple[_Admission, Revision]:
        state = self._filesystem.read_pointer(self._path)
        if state is None:
            raise StorageError("Missing Delta snapshot admission state.")
        raw, revision = state
        try:
            assert isinstance(raw, dict)
            floor, cleanup = raw["minimum_snapshot_id"], raw["cleanup"]
            encoded_pins, fenced = raw["pins"], raw["fenced_incarnations"]
            if type(floor) is not int or floor < 1 or not isinstance(encoded_pins, dict):
                raise ValueError
            if not isinstance(fenced, list) or not all(
                _valid_incarnation_id(id_) for id_ in fenced
            ):
                raise ValueError
            pins = {token: _Pin(**data) for token, data in encoded_pins.items()}
            if any(
                not isinstance(pin.replica_id, str)
                or not pin.replica_id
                or not _valid_incarnation_id(pin.incarnation_id)
                or type(pin.snapshot_id) is not int
                or pin.snapshot_id < floor
                or not isinstance(pin.acquired_at, str)
                or datetime.fromisoformat(pin.acquired_at).tzinfo is None
                or pin.incarnation_id in fenced
                for pin in pins.values()
            ):
                raise ValueError
            pending = _Cleanup(**cleanup) if cleanup is not None else None
            if pending is not None and (
                not isinstance(pending.token, str) or type(pending.online) is not bool
            ):
                raise ValueError
        except (AssertionError, KeyError, TypeError, ValueError) as exc:
            raise StorageError("Invalid Delta snapshot admission state.") from exc
        return _Admission(floor, pins, pending, tuple(fenced)), revision

    def _change(self, update: Callable[[_Admission], _Admission]) -> _Admission:
        # The OS lock provides local compare-and-swap. Object stores use the
        # provider revision as the precondition on every competing update.
        with self._lock.hold():
            for _ in range(64):
                state, revision = self._read()
                candidate = update(state)
                if candidate == state or self._filesystem.replace_pointer(
                    self._path, asdict(candidate), revision
                ):
                    return candidate
        raise StorageError("Delta snapshot admission is busy. Retry the operation.")

    def minimum_snapshot_id(self) -> int:
        return self._read()[0].minimum_snapshot_id

    def status(self) -> DeltaRetentionStatus:
        state = self._read()[0]
        mode: Literal["maintenance", "online"] | None = None
        if state.cleanup is not None:
            mode = "online" if state.cleanup.online else "maintenance"
        now = datetime.now(UTC)
        pins = tuple(
            DeltaRetentionPin(
                pin.replica_id,
                pin.incarnation_id,
                pin.snapshot_id,
                pin.acquired_at,
                round(max(0, (now - datetime.fromisoformat(pin.acquired_at)).total_seconds()), 3),
            )
            for pin in sorted(
                state.pins.values(), key=lambda pin: (pin.snapshot_id, pin.acquired_at)
            )
        )
        return DeltaRetentionStatus(
            state.minimum_snapshot_id,
            len(pins),
            pins[0].snapshot_id if pins else None,
            mode,
            pins,
        )

    @contextmanager
    def pin(self, snapshot_id: int) -> Iterator[None]:
        pid = os.getpid()
        if pid != self._pid:
            # A forked child must never borrow or release its parent's pin.
            self._local_lock = RLock()
            self._local_pins = {}
            self._witness = ExitStack()
            self._incarnation_id = uuid4().hex
            self._pid = pid

        with self._local_lock:
            pin = self._local_pins.get(snapshot_id)
            if pin is None:
                pin = _LocalPin(uuid4().hex)
                if not self._local_pins and self._local_root is not None:
                    self._witness.enter_context(
                        local_incarnation_lock(self._local_root, self._incarnation_id)
                    )
                try:
                    self._admit(snapshot_id, pin.token)
                except BaseException:
                    if not self._local_pins:
                        self._witness.close()
                    raise
                self._local_pins[snapshot_id] = pin
            else:
                pin.references += 1
        try:
            yield
        finally:
            if os.getpid() == pid:
                with self._local_lock:
                    pin.references -= 1
                    if pin.references == 0:
                        del self._local_pins[snapshot_id]
                        try:
                            self._change(
                                lambda state: replace(
                                    state,
                                    pins={
                                        key: value
                                        for key, value in state.pins.items()
                                        if key != pin.token
                                    },
                                )
                            )
                        finally:
                            if not self._local_pins:
                                self._witness.close()

    def _admit(self, snapshot_id: int, token: str) -> None:
        pin = _Pin(
            self._replica_id, self._incarnation_id, snapshot_id, datetime.now(UTC).isoformat()
        )

        def admit(state: _Admission) -> _Admission:
            if pin.incarnation_id in state.fenced_incarnations:
                raise StorageError("This Delta replica incarnation has been fenced.")
            if snapshot_id < state.minimum_snapshot_id:
                raise StorageError(
                    f"Delta snapshot {snapshot_id} has expired. "
                    f"The first readable snapshot is {state.minimum_snapshot_id}."
                )
            if state.cleanup is not None and not state.cleanup.online:
                raise StorageError("Delta snapshot admission is paused for retention maintenance.")
            return replace(state, pins={**state.pins, token: pin})

        self._change(admit)

    def recover_pins(
        self, incarnation_id: str, *, database_id: str, verifier: Path | None = None
    ) -> DeltaPinRecoveryResult:
        if not _valid_incarnation_id(incarnation_id):
            raise ValueError("A Delta incarnation ID must contain 32 lowercase hex characters.")
        state, _ = self._read()
        if incarnation_id in state.fenced_incarnations:
            return DeltaPinRecoveryResult(incarnation_id, 0)
        owners = {
            pin.replica_id for pin in state.pins.values() if pin.incarnation_id == incarnation_id
        }
        if len(owners) != 1:
            raise StorageError("The Delta replica incarnation does not have an identifiable pin.")

        released = 0

        def fence(current: _Admission) -> _Admission:
            nonlocal released
            pins = {
                token: pin
                for token, pin in current.pins.items()
                if pin.incarnation_id != incarnation_id
            }
            released = len(current.pins) - len(pins)
            if incarnation_id in current.fenced_incarnations:
                return current
            return replace(
                current,
                pins=pins,
                fenced_incarnations=(*current.fenced_incarnations, incarnation_id),
            )

        if self._local_root is not None:
            with local_incarnation_lock(self._local_root, incarnation_id):
                self._change(fence)
        else:
            if verifier is None:
                raise StorageError("Recovering Delta pins requires an external fence verifier.")
            verify_external_fence(
                verifier,
                database_id=database_id,
                database_location=self._database_location,
                replica_id=owners.pop(),
                incarnation_id=incarnation_id,
            )
            self._change(fence)
        return DeltaPinRecoveryResult(incarnation_id, released)

    def begin_cleanup(self, minimum_snapshot_id: int, *, online: bool) -> str:
        token = uuid4().hex

        def begin(state: _Admission) -> _Admission:
            if state.cleanup is not None:
                if (
                    minimum_snapshot_id != state.minimum_snapshot_id
                    or online != state.cleanup.online
                ):
                    raise StorageError(
                        "Resume pending Delta retention at the same cutoff and mode."
                    )
                return state
            if minimum_snapshot_id < state.minimum_snapshot_id:
                raise StorageError("The Delta retention cutoff cannot move backwards.")
            if state.pins and (
                not online
                or min(pin.snapshot_id for pin in state.pins.values()) < minimum_snapshot_id
            ):
                raise StorageError(
                    "Active Delta snapshot pins block this retention cutoff. "
                    "Wait for the operations using them to finish."
                )
            return replace(
                state, minimum_snapshot_id=minimum_snapshot_id, cleanup=_Cleanup(token, online)
            )

        cleanup = self._change(begin).cleanup
        assert cleanup is not None
        return cleanup.token

    def finish_cleanup(self, token: str) -> None:
        def finish(state: _Admission) -> _Admission:
            if state.cleanup is not None and state.cleanup.token == token:
                return replace(state, cleanup=None)
            return state

        self._change(finish)
