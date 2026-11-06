"""Own and serialize GraphArrow ANA workflows."""

from __future__ import annotations

import secrets
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from threading import Condition, Lock
from typing import Any, Protocol

from grapharrow.ana.handles import HandleStore


class SessionNotFound(LookupError):
    """The workflow token is invalid, expired or owned by another client."""


class SessionCapacityError(RuntimeError):
    """The service cannot admit another workflow."""


class Backend(Protocol):
    def call(self, tool: str, args: dict[str, Any]) -> dict[str, Any]: ...
    def close(self) -> None: ...


@dataclass(frozen=True)
class SessionConfig:
    max_sessions: int = 128
    idle_timeout_s: float = 1800


@dataclass
class Workflow:
    backend: Backend
    owner: str
    last_used: float
    lock: Lock = field(default_factory=Lock)
    active: int = 0
    closed: bool = False

    def close(self) -> None:
        with self.lock:
            if not self.closed:
                self.closed = True
                self.backend.close()


class BackendSessions:
    """Manage owned tokens, idle expiry and resource release in one process."""

    def __init__(
        self,
        factory: Callable[[HandleStore], Backend],
        config: SessionConfig,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._factory = factory
        self._config = config
        self._clock = clock
        self._condition = Condition()
        self._sessions: dict[str, Workflow] = {}
        self._closed = False
        self._opening = 0

    def _expire(self) -> None:
        with self._condition:
            cutoff = self._clock() - self._config.idle_timeout_s
            expired = [
                token
                for token, workflow in self._sessions.items()
                if workflow.active == 0 and workflow.last_used <= cutoff
            ]
            removed = [self._sessions.pop(token) for token in expired]
        for workflow in removed:
            workflow.close()

    def create(self, *, owner_id: str) -> str:
        self._expire()
        with self._condition:
            if self._closed:
                raise RuntimeError("Workflow manager is closed")
            if len(self._sessions) + self._opening >= self._config.max_sessions:
                raise SessionCapacityError("Session capacity reached")
            self._opening += 1
        try:
            workflow = Workflow(self._factory(HandleStore()), owner_id, self._clock())
            with self._condition:
                if not self._closed:
                    token = secrets.token_urlsafe(32)
                    self._sessions[token] = workflow
                    return token
            workflow.close()
            raise RuntimeError("Workflow manager is closed")
        finally:
            with self._condition:
                self._opening -= 1
                self._condition.notify_all()

    def _owned(self, token: str, owner: str) -> Workflow:
        workflow = self._sessions.get(token)
        if self._closed or workflow is None or workflow.owner != owner:
            raise SessionNotFound("Unknown or expired session")
        return workflow

    def call(self, token: str, tool: str, args: dict[str, Any], *, owner_id: str) -> dict[str, Any]:
        self._expire()
        with self._condition:
            workflow = self._owned(token, owner_id)
            workflow.active += 1
        try:
            with workflow.lock:
                if workflow.closed:
                    raise SessionNotFound("Unknown or expired session")
                return workflow.backend.call(tool, args)
        finally:
            with self._condition:
                workflow.active -= 1
                workflow.last_used = self._clock()

    def delete(self, token: str, *, owner_id: str) -> None:
        self._expire()
        with self._condition:
            workflow = self._owned(token, owner_id)
            del self._sessions[token]
        workflow.close()

    def close(self) -> None:
        with self._condition:
            self._closed = True
            self._condition.wait_for(lambda: self._opening == 0)
            workflows = list(self._sessions.values())
            self._sessions.clear()
        failures = []
        for workflow in workflows:
            try:
                workflow.close()
            except Exception as exc:
                failures.append(exc)
        if failures:
            raise ExceptionGroup("Workflow cleanup failed", failures)
