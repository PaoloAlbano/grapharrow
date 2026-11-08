"""Coordinate writers to one database across threads and native local processes."""

from __future__ import annotations

import os
import sys
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import wraps
from pathlib import Path
from threading import Lock, RLock
from typing import BinaryIO, ParamSpec, Protocol, TypeVar, cast
from weakref import WeakValueDictionary

import pyarrow.fs as pafs  # type: ignore[import-untyped]

from grapharrow.adapters.filesystem import FileSystemAdapter


@dataclass
class _WriterState:
    lock: RLock = field(default_factory=RLock)
    pid: int = field(default_factory=os.getpid)
    depth: int = 0
    handle: BinaryIO | None = None


_registry: WeakValueDictionary[str, _WriterState] = WeakValueDictionary()
_registry_lock = Lock()


def _after_fork() -> None:
    global _registry, _registry_lock
    for state in list(_registry.values()):
        if state.handle is not None:
            # Closing the child's copy must not explicitly unlock the parent's descriptor.
            state.handle.close()
    _registry = WeakValueDictionary()
    _registry_lock = Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork)


def _state_for(key: str) -> _WriterState:
    with _registry_lock:
        state = _registry.get(key)
        if state is None:
            state = _WriterState()
            _registry[key] = state
        return state


def _lock_file(handle: BinaryIO) -> None:
    if sys.platform == "win32":
        import errno
        import msvcrt

        if os.fstat(handle.fileno()).st_size == 0:
            handle.write(b"\0")
        while True:
            handle.seek(0)
            try:
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                return
            except OSError as exc:
                if exc.errno not in {errno.EACCES, errno.EAGAIN, errno.EDEADLK}:
                    raise
                time.sleep(0.05)
    else:
        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)


class WriterCoordinator:
    """Use a reentrant process lock and an OS file lock for native local storage."""

    def __init__(self, root: str | Path, filesystem: FileSystemAdapter) -> None:
        resolved, location = filesystem.resolve(root)
        while isinstance(resolved, pafs.SubTreeFileSystem):
            location = f"{resolved.base_path.rstrip('/')}/{location.lstrip('/')}"
            resolved = resolved.base_fs
        self.local_root = (
            Path(location).resolve() if isinstance(resolved, pafs.LocalFileSystem) else None
        )
        self._key = str(self.local_root) if self.local_root is not None else str(root).rstrip("/")
        self._state = _state_for(self._key)

    @contextmanager
    def hold(self) -> Iterator[None]:
        if self._state.pid != os.getpid():
            self._state = _state_for(self._key)
        state = self._state
        with state.lock:
            if state.depth == 0 and self.local_root is not None:
                self.local_root.mkdir(parents=True, exist_ok=True)
                handle = (self.local_root / ".grapharrow-writer.lock").open("a+b", buffering=0)
                try:
                    _lock_file(handle)
                except BaseException:
                    handle.close()
                    raise
                state.handle = handle
            state.depth += 1
            try:
                yield
            finally:
                state.depth -= 1
                if state.depth == 0 and state.handle is not None:
                    state.handle.close()
                    state.handle = None


class _Coordinated(Protocol):
    _writer: WriterCoordinator


P = ParamSpec("P")
R = TypeVar("R")


def writer_locked(method: Callable[P, R]) -> Callable[P, R]:
    """Hold a use case's writer coordinator for the complete mutation."""

    @wraps(method)
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
        owner = cast(_Coordinated, args[0])
        with owner._writer.hold():
            return method(*args, **kwargs)

    return wrapped
