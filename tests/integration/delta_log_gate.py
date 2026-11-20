"""Pause a local object-store proxy at the HTTP publication of one Delta version."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Event, Thread
from urllib.parse import unquote, urlsplit

_HOP_HEADERS = {"connection", "keep-alive", "transfer-encoding"}


@dataclass(slots=True)
class DeltaLogGate:
    reached: Event = field(default_factory=Event)
    staged_partitions: set[str] = field(default_factory=set)
    authority: str = ""


@contextmanager
def pause_delta_log_put(
    upstream: str, *, version: int, after_publish: bool
) -> Iterator[DeltaLogGate]:
    """Forward object I/O, holding the selected log PUT or its successful response."""
    target = urlsplit(f"http://{upstream}")
    suffix = f"/_delta_log/{version:020d}.json"
    gate = DeltaLogGate()
    closed = Event()

    class Handler(BaseHTTPRequestHandler):
        def forward(self) -> None:
            path = unquote(urlsplit(self.path).path)
            log_put = self.command == "PUT" and path.endswith(suffix)
            if log_put and not after_publish:
                gate.reached.set()
                closed.wait()
                return

            headers = {
                name: value
                for name, value in self.headers.items()
                if name.lower() not in _HOP_HEADERS
            }
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            connection = HTTPConnection(target.hostname or "127.0.0.1", target.port, timeout=20)
            try:
                connection.request(self.command, self.path, body=body or None, headers=headers)
                response = connection.getresponse()
                data = response.read()
                if self.command == "PUT" and 200 <= response.status < 300:
                    if log_put:
                        gate.reached.set()
                        closed.wait()
                        return
                    if path.endswith(".parquet"):
                        partition = path.rsplit("/", 2)[-2].removeprefix("kind=")
                        gate.staged_partitions.add(partition)
                self.send_response_only(response.status, response.reason)
                for name, value in response.getheaders():
                    if name.lower() not in _HOP_HEADERS | {"content-length"}:
                        self.send_header(name, value)
                size = (
                    response.getheader("Content-Length", "0")
                    if self.command == "HEAD"
                    else str(len(data))
                )
                self.send_header("Content-Length", size)
                self.end_headers()
                self.wfile.write(data)
            finally:
                connection.close()

        do_DELETE = do_GET = do_HEAD = do_POST = do_PUT = forward

        def log_message(self, format: str, *args: object) -> None:
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        gate.authority = f"127.0.0.1:{server.server_port}"
        thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
        thread.start()
        try:
            yield gate
        finally:
            closed.set()
            server.shutdown()
            thread.join()
