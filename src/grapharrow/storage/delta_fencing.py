"""Verifiable process fencing for recovering Delta snapshot pins."""

from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO
from uuid import uuid4
from weakref import WeakSet

from grapharrow.core.errors import StorageError

_process_locks: WeakSet[BinaryIO] = WeakSet()


def _close_inherited_locks() -> None:
    for handle in list(_process_locks):
        handle.close()
    _process_locks.clear()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_close_inherited_locks)


@contextmanager
def local_incarnation_lock(root: Path, incarnation_id: str) -> Iterator[None]:
    """Hold an OS lock until the last operation finishes or the process exits."""
    import fcntl

    directory = root / ".grapharrow-delta-incarnations"
    directory.mkdir(exist_ok=True)
    with (directory / f"{incarnation_id}.lock").open("a+b", buffering=0) as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise StorageError(
                "The Delta replica incarnation still holds its process lock."
            ) from exc
        _process_locks.add(handle)
        try:
            yield
        finally:
            _process_locks.discard(handle)


def verify_external_fence(
    verifier: Path,
    *,
    database_id: str,
    database_location: str,
    replica_id: str,
    incarnation_id: str,
) -> None:
    """Ask an operator-controlled verifier for a fresh, identity-bound fence."""
    if not verifier.is_absolute():
        raise ValueError("The Delta fence verifier must be an absolute executable path.")
    request = {
        "database_id": database_id,
        "database_location": database_location,
        "replica_id": replica_id,
        "incarnation_id": incarnation_id,
        "challenge": uuid4().hex,
    }
    try:
        result = subprocess.run(
            [str(verifier)],
            input=json.dumps(request),
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
        )
        reply = json.loads(result.stdout)
    except (OSError, subprocess.SubprocessError, ValueError) as exc:
        raise StorageError("The Delta fence verifier did not confirm the replica fence.") from exc
    if (
        not isinstance(reply, dict)
        or any(reply.get(key) != value for key, value in request.items())
        or reply.get("fenced") is not True
        or reply.get("drained") is not True
    ):
        raise StorageError("The Delta fence verifier did not confirm the replica fence.")
