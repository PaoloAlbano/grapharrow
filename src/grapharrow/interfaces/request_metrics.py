"""Expose process-local HTTP operation and workflow metrics in Prometheus text format."""

from __future__ import annotations

from bisect import bisect_left
from collections import Counter
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from itertools import accumulate
from secrets import compare_digest
from threading import Lock
from time import perf_counter
from typing import TYPE_CHECKING

from fastapi import FastAPI, Header, HTTPException, Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send

if TYPE_CHECKING:
    from grapharrow.ana.sessions import BackendSessions
    from grapharrow.use_cases.ana_resources import AnaResourceBudget

_BOUNDS = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60)
_BUCKETS = (*map(str, _BOUNDS), "+Inf")


@dataclass
class _Duration:
    buckets: list[int] = field(default_factory=lambda: [0] * len(_BUCKETS))
    total_seconds: float = 0.0

    def record(self, seconds: float) -> None:
        self.buckets[bisect_left(_BOUNDS, seconds)] += 1
        self.total_seconds += seconds

    def samples(self, metric: str, interface: str) -> Iterator[str]:
        labels = f'interface="{interface}"'
        for bound, count in zip(_BUCKETS, accumulate(self.buckets), strict=True):
            yield f'{metric}_bucket{{{labels},le="{bound}"}} {count}'
        yield f"{metric}_count{{{labels}}} {sum(self.buckets)}"
        yield f"{metric}_sum{{{labels}}} {self.total_seconds}"


@dataclass
class _Operations:
    in_flight: int = 0
    executing: int = 0
    responses: Counter[str] = field(default_factory=Counter)
    request_duration: _Duration = field(default_factory=_Duration)
    execution_duration: _Duration = field(default_factory=_Duration)


class RequestMetrics:
    """Keep bounded metrics for the REST Cypher and ANA HTTP interfaces."""

    def __init__(self, interfaces: Iterable[str]) -> None:
        self._operations = {name: _Operations() for name in interfaces}
        self._lock = Lock()
        self._write_conflicts = 0
        self.ana_resources: tuple[BackendSessions, AnaResourceBudget] | None = None

    def start(self, interface: str) -> None:
        with self._lock:
            self._operations[interface].in_flight += 1

    def finish(self, interface: str, status: str, seconds: float) -> None:
        with self._lock:
            operation = self._operations[interface]
            operation.in_flight -= 1
            operation.responses[status] += 1
            operation.request_duration.record(seconds)

    @contextmanager
    def execution(self, interface: str) -> Iterator[None]:
        """Record time in the query executor or ANA session dispatcher."""
        with self._lock:
            operation = self._operations[interface]
            operation.executing += 1
        start = perf_counter()
        try:
            yield
        finally:
            elapsed = perf_counter() - start
            with self._lock:
                operation.executing -= 1
                operation.execution_duration.record(elapsed)

    def write_conflict(self) -> None:
        with self._lock:
            self._write_conflicts += 1

    def render(self) -> str:
        in_flight = "grapharrow_http_operations_in_flight"
        executing = "grapharrow_http_operations_executing"
        requests = "grapharrow_http_operations_total"
        request_duration = "grapharrow_http_operation_duration_seconds"
        execution_duration = "grapharrow_http_operation_execution_seconds"
        with self._lock:
            samples = [
                f"# TYPE {in_flight} gauge",
                *(
                    f'{in_flight}{{interface="{name}"}} {operation.in_flight}'
                    for name, operation in self._operations.items()
                ),
                f"# TYPE {executing} gauge",
                *(
                    f'{executing}{{interface="{name}"}} {operation.executing}'
                    for name, operation in self._operations.items()
                ),
                f"# TYPE {requests} counter",
                *(
                    f'{requests}{{interface="{name}",status="{status}"}} {count}'
                    for name, operation in self._operations.items()
                    for status, count in sorted(operation.responses.items())
                ),
                f"# TYPE {request_duration} histogram",
                *(
                    sample
                    for name, operation in self._operations.items()
                    for sample in operation.request_duration.samples(request_duration, name)
                ),
                f"# TYPE {execution_duration} histogram",
                *(
                    sample
                    for name, operation in self._operations.items()
                    for sample in operation.execution_duration.samples(execution_duration, name)
                ),
                "# TYPE grapharrow_rest_write_conflicts_total counter",
                f"grapharrow_rest_write_conflicts_total {self._write_conflicts}",
            ]
        if self.ana_resources is not None:
            sessions, budget = self.ana_resources
            samples.extend(
                (
                    "# TYPE grapharrow_ana_sessions_open gauge",
                    f"grapharrow_ana_sessions_open {sessions.session_count}",
                    "# TYPE grapharrow_ana_session_bytes gauge",
                    f"grapharrow_ana_session_bytes {budget.used_bytes}",
                    "# TYPE grapharrow_ana_session_limit_bytes gauge",
                    f"grapharrow_ana_session_limit_bytes {budget.max_bytes}",
                )
            )
        return "\n".join([*samples, ""])


def install_request_metrics(
    app: FastAPI, *, prefix: str, ana_enabled: bool, token: str | None
) -> RequestMetrics:
    """Install process-local instrumentation on the two supported HTTP operation routes."""
    routes = {f"{prefix}/query": "rest_cypher"}
    if ana_enabled:
        routes["/tools/call"] = "ana"
    metrics = RequestMetrics(routes.values())

    def middleware(downstream: ASGIApp) -> ASGIApp:
        async def count_request(scope: Scope, receive: Receive, send: Send) -> None:
            interface = None
            if scope["type"] == "http" and scope["method"] == "POST":
                path = scope["path"]
                root = scope.get("root_path", "").rstrip("/")
                if root and path.startswith(root + "/"):
                    path = path[len(root) :]
                interface = routes.get(path)

            if interface is None:
                await downstream(scope, receive, send)
                return

            status = "aborted"

            async def observe_response(message: Message) -> None:
                nonlocal status
                if message["type"] == "http.response.start":
                    status = str(message["status"])
                await send(message)

            start = perf_counter()
            metrics.start(interface)
            try:
                await downstream(scope, receive, observe_response)
            except Exception:
                if status == "aborted":
                    status = "500"
                raise
            finally:
                metrics.finish(interface, status, perf_counter() - start)

        return count_request

    @app.get(f"{prefix}/metrics", include_in_schema=False)
    async def scrape_metrics(authorization: str | None = Header(default=None)) -> Response:
        if token is not None and not compare_digest(
            (authorization or "").encode(), f"Bearer {token}".encode()
        ):
            raise HTTPException(status_code=401, detail="Invalid or missing authorization token.")
        return Response(
            metrics.render(),
            media_type="text/plain; version=0.0.4",
            headers={"Cache-Control": "no-store"},
        )

    app.add_middleware(middleware)
    return metrics
