"""Minimal Bolt 4.4 server for direct driver access."""

from __future__ import annotations

import argparse
import socket
import socketserver
import struct
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from grapharrow.adapters import FileSystemAdapter
from grapharrow.app import GraphArrowApplication, build_application
from grapharrow.config import GraphArrowSettings
from grapharrow.core.errors import GraphArrowError
from grapharrow.interfaces.cypher_guard import query_requires_write
from grapharrow.use_cases import CypherQueryUseCase, GraphTransactionUseCase
from grapharrow.use_cases.graph import GraphState

BOLT_MAGIC = bytes.fromhex("6060B017")
SUPPORTED_BOLT_MAJOR = 4
SUPPORTED_BOLT_MINOR = 4
SUPPORTED_BOLT_VERSION_BYTES = bytes([0, 0, SUPPORTED_BOLT_MINOR, SUPPORTED_BOLT_MAJOR])

HELLO = 0x01
GOODBYE = 0x02
RESET = 0x0F
RUN = 0x10
BEGIN = 0x11
COMMIT = 0x12
ROLLBACK = 0x13
DISCARD = 0x2F
PULL = 0x3F

SUCCESS = 0x70
RECORD = 0x71
IGNORED = 0x7E
FAILURE = 0x7F


@dataclass(frozen=True, slots=True)
class BoltInterfaceApplication:
    """Wired application container plus Bolt runtime settings."""

    grapharrow: GraphArrowApplication
    host: str
    port: int
    auth_token: str | None
    server_agent: str = "GraphArrow/0.1.0"
    database_name: str = "grapharrow"


@dataclass(slots=True)
class PackstreamStructure:
    """Simple representation of a PackStream structure."""

    signature: int
    fields: list[Any] = field(default_factory=list)


@dataclass(slots=True)
class _PendingResult:
    """Result stream buffered between RUN and PULL/DISCARD."""

    columns: tuple[str, ...]
    rows: tuple[dict[str, Any], ...]
    index: int = 0
    statement_type: str = "r"
    summary_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class _ConnectionState:
    """Mutable state for a single Bolt connection."""

    ready: bool = False
    failed: bool = False
    closed: bool = False
    pending: _PendingResult | None = None
    transaction: GraphTransactionUseCase | None = None
    transaction_cypher: CypherQueryUseCase | None = None
    next_query_id: int = 0


class _BoltPackstreamCodec:
    """Subset PackStream encoder/decoder needed by the Bolt bootstrap server."""

    def encode(self, value: Any) -> bytes:
        """Encode Python values into the limited PackStream subset used by this server."""
        if value is None:
            return b"\xc0"
        if value is True:
            return b"\xc3"
        if value is False:
            return b"\xc2"
        if isinstance(value, int):
            return self._encode_int(value)
        if isinstance(value, float):
            return b"\xc1" + struct.pack(">d", value)
        if isinstance(value, str):
            return self._encode_string(value)
        if isinstance(value, (list, tuple)):
            payload = b"".join(self.encode(item) for item in value)
            return self._encode_sized(0x90, 0xD4, 0xD5, 0xD6, len(value), payload)
        if isinstance(value, dict):
            map_payload = bytearray()
            for key, item in value.items():
                map_payload.extend(self._encode_string(str(key)))
                map_payload.extend(self.encode(item))
            return self._encode_sized(
                0xA0,
                0xD8,
                0xD9,
                0xDA,
                len(value),
                bytes(map_payload),
            )
        if isinstance(value, PackstreamStructure):
            if not 0 <= len(value.fields) <= 15:
                raise ValueError("Only tiny structures are supported.")
            structure_payload = bytearray([0xB0 | len(value.fields), value.signature])
            for field in value.fields:
                structure_payload.extend(self.encode(field))
            return bytes(structure_payload)
        raise TypeError(f"Unsupported PackStream value: {type(value)!r}")

    def decode(self, payload: bytes) -> Any:
        """Decode one PackStream payload into Python values or structures."""
        value, offset = self._decode_value(memoryview(payload), 0)
        if offset != len(payload):
            raise ValueError("Unexpected trailing bytes in PackStream payload.")
        return value

    def _decode_value(self, payload: memoryview, offset: int) -> tuple[Any, int]:
        """Decode this stored value into the Python representation used here."""
        marker = payload[offset]
        offset += 1

        if marker <= 0x7F:
            return marker, offset
        if marker >= 0xF0:
            return marker - 0x100, offset
        if marker == 0xC0:
            return None, offset
        if marker == 0xC2:
            return False, offset
        if marker == 0xC3:
            return True, offset
        if marker == 0xC1:
            return struct.unpack(">d", payload[offset : offset + 8])[0], offset + 8
        if marker in {0xC8, 0xC9, 0xCA, 0xCB}:
            size = {0xC8: 1, 0xC9: 2, 0xCA: 4, 0xCB: 8}[marker]
            formats = {1: ">b", 2: ">h", 4: ">i", 8: ">q"}
            return struct.unpack(formats[size], payload[offset : offset + size])[0], offset + size
        if 0x80 <= marker <= 0x8F:
            size = marker & 0x0F
            return payload[offset : offset + size].tobytes().decode("utf-8"), offset + size
        if marker in {0xD0, 0xD1, 0xD2}:
            size, offset = self._decode_size(payload, offset, marker)
            return payload[offset : offset + size].tobytes().decode("utf-8"), offset + size
        if 0x90 <= marker <= 0x9F:
            size = marker & 0x0F
            items: list[Any] = []
            for _ in range(size):
                item, offset = self._decode_value(payload, offset)
                items.append(item)
            return items, offset
        if marker in {0xD4, 0xD5, 0xD6}:
            size, offset = self._decode_size(payload, offset, marker)
            list_items: list[Any] = []
            for _ in range(size):
                item, offset = self._decode_value(payload, offset)
                list_items.append(item)
            return list_items, offset
        if 0xA0 <= marker <= 0xAF:
            size = marker & 0x0F
            result: dict[str, Any] = {}
            for _ in range(size):
                key, offset = self._decode_value(payload, offset)
                value, offset = self._decode_value(payload, offset)
                result[str(key)] = value
            return result, offset
        if marker in {0xD8, 0xD9, 0xDA}:
            size, offset = self._decode_size(payload, offset, marker)
            result_map: dict[str, Any] = {}
            for _ in range(size):
                key, offset = self._decode_value(payload, offset)
                value, offset = self._decode_value(payload, offset)
                result_map[str(key)] = value
            return result_map, offset
        if 0xB0 <= marker <= 0xBF:
            size = marker & 0x0F
            signature = payload[offset]
            offset += 1
            fields: list[Any] = []
            for _ in range(size):
                value, offset = self._decode_value(payload, offset)
                fields.append(value)
            return PackstreamStructure(signature=signature, fields=fields), offset
        raise ValueError(f"Unsupported PackStream marker: 0x{marker:02X}")

    def _decode_size(
        self,
        payload: memoryview,
        offset: int,
        marker: int,
    ) -> tuple[int, int]:
        """Decode the length prefix used by string, list, and map PackStream markers."""
        if marker in {0xD0, 0xD4, 0xD8}:
            return payload[offset], offset + 1
        if marker in {0xD1, 0xD5, 0xD9}:
            return struct.unpack(">H", payload[offset : offset + 2])[0], offset + 2
        if marker in {0xD2, 0xD6, 0xDA}:
            return struct.unpack(">I", payload[offset : offset + 4])[0], offset + 4
        raise ValueError(f"Unsupported size marker: 0x{marker:02X}")

    def _encode_int(self, value: int) -> bytes:
        """Encode an integer using the smallest PackStream integer marker that fits."""
        if -16 <= value <= 127:
            return struct.pack(">b", value)
        if -128 <= value <= 127:
            return b"\xc8" + struct.pack(">b", value)
        if -32768 <= value <= 32767:
            return b"\xc9" + struct.pack(">h", value)
        if -(2**31) <= value <= (2**31 - 1):
            return b"\xca" + struct.pack(">i", value)
        return b"\xcb" + struct.pack(">q", value)

    def _encode_string(self, value: str) -> bytes:
        """Encode a UTF-8 string with the correct PackStream string header."""
        payload = value.encode("utf-8")
        return self._encode_sized(0x80, 0xD0, 0xD1, 0xD2, len(payload), payload)

    def _encode_sized(
        self,
        tiny_base: int,
        marker_8: int,
        marker_16: int,
        marker_32: int,
        size: int,
        payload: bytes,
    ) -> bytes:
        """Encode a sized PackStream value by choosing the tightest available header."""
        if size < 16:
            return bytes([tiny_base | size]) + payload
        if size <= 0xFF:
            return bytes([marker_8, size]) + payload
        if size <= 0xFFFF:
            return bytes([marker_16]) + struct.pack(">H", size) + payload
        return bytes([marker_32]) + struct.pack(">I", size) + payload


class _BoltRequestHandler(socketserver.BaseRequestHandler):
    """Single connection Bolt handler implementing a minimal direct protocol path."""

    codec = _BoltPackstreamCodec()

    def handle(self) -> None:
        """Drive the per-connection Bolt loop from handshake through message dispatch."""
        server = cast(GraphArrowBoltServer, self.server)
        state = _ConnectionState()
        try:
            if not self._negotiate_protocol():
                return
            while not state.closed:
                payload = _recv_message(self.request)
                if payload is None:
                    return
                message = self.codec.decode(payload)
                if not isinstance(message, PackstreamStructure):
                    self._fail(state, "Bolt messages must be PackStream structures.")
                    return
                self._dispatch(server, state, message)
        except (ConnectionError, OSError):
            return

    def _negotiate_protocol(self) -> bool:
        """Accept the handshake only when the client offers a Bolt version we support."""
        handshake = _recv_exact(self.request, 20)
        if handshake is None or handshake[:4] != BOLT_MAGIC:
            return False
        offered = [handshake[index : index + 4] for index in range(4, 20, 4)]
        # The bootstrap server speaks a single Bolt version and accepts it if the
        # driver proposes either the exact version or a compatible range.
        if any(_version_matches(candidate) for candidate in offered):
            self.request.sendall(SUPPORTED_BOLT_VERSION_BYTES)
            return True
        self.request.sendall(b"\x00\x00\x00\x00")
        return False

    def _dispatch(
        self,
        server: GraphArrowBoltServer,
        state: _ConnectionState,
        message: PackstreamStructure,
    ) -> None:
        """Route one decoded Bolt message through the current connection state machine."""
        if state.failed and message.signature not in {RESET, ROLLBACK, GOODBYE}:
            _send_message(self.request, self.codec, PackstreamStructure(IGNORED, []))
            return

        if message.signature == HELLO:
            self._handle_hello(server, state, message)
            return
        if message.signature == GOODBYE:
            state.closed = True
            return
        if message.signature == RESET:
            state.failed = False
            state.pending = None
            if state.transaction is not None:
                state.transaction.rollback()
                state.transaction = None
                state.transaction_cypher = None
            _send_success(self.request, self.codec, {})
            return
        if not state.ready:
            self._fail(state, "HELLO must be completed before other messages.")
            return
        if message.signature == BEGIN:
            self._handle_begin(server, state, message)
            return
        if message.signature == RUN:
            self._handle_run(server, state, message)
            return
        if message.signature == COMMIT:
            self._handle_commit(server, state, message)
            return
        if message.signature == ROLLBACK:
            self._handle_rollback(state, message)
            return
        if message.signature == PULL:
            self._handle_pull(server, state, message)
            return
        if message.signature == DISCARD:
            self._handle_discard(server, state, message)
            return

        self._fail(state, f"Unsupported Bolt message signature: 0x{message.signature:02X}")

    def _handle_hello(
        self,
        server: GraphArrowBoltServer,
        state: _ConnectionState,
        message: PackstreamStructure,
    ) -> None:
        """Authenticate the client and move the connection into the ready state."""
        if state.ready:
            self._fail(state, "HELLO may only be sent once per connection.")
            return
        if len(message.fields) != 1 or not isinstance(message.fields[0], dict):
            self._fail(state, "HELLO requires one metadata dictionary.")
            return

        metadata = cast(dict[str, Any], message.fields[0])
        auth_error = _validate_auth(metadata, expected_token=server.bolt_app.auth_token)
        if auth_error is not None:
            _send_failure(
                self.request,
                self.codec,
                code="Neo.ClientError.Security.Unauthorized",
                message=auth_error,
            )
            state.closed = True
            return

        state.ready = True
        _send_success(
            self.request,
            self.codec,
            {
                "server": server.bolt_app.server_agent,
                "connection_id": f"grapharrow-bolt-{threading.get_ident()}",
            },
        )

    def _handle_begin(
        self,
        server: GraphArrowBoltServer,
        state: _ConnectionState,
        message: PackstreamStructure,
    ) -> None:
        """Open a GraphArrow transaction and a transaction-scoped Cypher executor."""
        if state.pending is not None:
            self._fail(state, "Cannot BEGIN while a result stream is active.")
            return
        if state.transaction is not None:
            self._fail(state, "A transaction is already active.")
            return
        if len(message.fields) != 1 or not isinstance(message.fields[0], dict):
            self._fail(state, "BEGIN requires one metadata dictionary.")
            return

        transaction = GraphTransactionUseCase(server.bolt_app.grapharrow.use_cases.graph)
        state.transaction = transaction
        state.transaction_cypher = CypherQueryUseCase(
            graph_service=cast(Any, transaction),
            tabular_adapter=server.bolt_app.grapharrow.adapters.tabular,
        )
        _send_success(self.request, self.codec, {})

    def _handle_run(
        self,
        server: GraphArrowBoltServer,
        state: _ConnectionState,
        message: PackstreamStructure,
    ) -> None:
        """Execute a Cypher statement and buffer its rows until PULL or DISCARD arrives."""
        if state.pending is not None:
            self._fail(state, "A result stream is already active.")
            return
        if len(message.fields) != 3:
            self._fail(state, "RUN requires query, parameters, and extra metadata.")
            return

        query = message.fields[0]
        parameters = message.fields[1]
        if not isinstance(query, str) or not isinstance(parameters, dict):
            self._fail(state, "RUN query must be a string and parameters a dictionary.")
            return

        try:
            executor = state.transaction_cypher or server.bolt_app.grapharrow.use_cases.cypher
            statement_type = _statement_type_for_query(query)
            before_state = (
                executor._graph_service.load_state() if statement_type != "r" else None
            )
            result = executor.execute(query, parameters)
            after_state = (
                executor._graph_service.load_state() if before_state is not None else None
            )
        except GraphArrowError as exc:
            state.failed = True
            _send_failure(
                self.request,
                self.codec,
                code="GraphArrow.QueryError",
                message=str(exc),
            )
            return

        # RUN buffers the logical result stream; actual records are emitted only
        # when the client later issues PULL, matching Bolt's pull-based flow.
        query_id = state.next_query_id
        state.next_query_id += 1
        summary_metadata: dict[str, Any] = {
            "db": server.bolt_app.database_name,
            "qid": query_id,
        }
        if before_state is not None and after_state is not None:
            stats = _summary_stats(before_state, after_state)
            if stats:
                summary_metadata["stats"] = stats
        state.pending = _PendingResult(
            columns=result.columns,
            rows=result.rows,
            statement_type=statement_type,
            summary_metadata=summary_metadata,
        )
        _send_success(
            self.request,
            self.codec,
            {
                "fields": list(result.columns),
                "t_first": 0,
                "db": server.bolt_app.database_name,
                "qid": query_id,
            },
        )

    def _handle_commit(
        self,
        server: GraphArrowBoltServer,
        state: _ConnectionState,
        message: PackstreamStructure,
    ) -> None:
        """Commit the active transaction and reply with completion metadata and bookmark."""
        if state.transaction is None:
            self._fail(state, "COMMIT requires an active transaction.")
            return
        if state.pending is not None:
            self._fail(state, "COMMIT cannot be sent while a result stream is active.")
            return
        if message.fields:
            self._fail(state, "COMMIT does not accept fields.")
            return

        try:
            state.transaction.commit()
        except GraphArrowError as exc:
            state.failed = True
            _send_failure(
                self.request,
                self.codec,
                code="GraphArrow.QueryError",
                message=str(exc),
            )
            return
        finally:
            state.transaction = None
            state.transaction_cypher = None

        _send_success(self.request, self.codec, _completion_metadata(server, "w"))

    def _handle_rollback(
        self,
        state: _ConnectionState,
        message: PackstreamStructure,
    ) -> None:
        """Abort the active transaction and clear failure state for the connection."""
        if state.transaction is None:
            self._fail(state, "ROLLBACK requires an active transaction.")
            return
        if message.fields:
            self._fail(state, "ROLLBACK does not accept fields.")
            return

        state.pending = None
        state.transaction.rollback()
        state.failed = False
        state.transaction = None
        state.transaction_cypher = None
        _send_success(self.request, self.codec, {})

    def _handle_pull(
        self,
        server: GraphArrowBoltServer,
        state: _ConnectionState,
        message: PackstreamStructure,
    ) -> None:
        """Stream buffered rows to the client in Bolt PULL batches and finish the result."""
        if state.pending is None:
            self._fail(state, "PULL requires an active result stream.")
            return
        if len(message.fields) != 1 or not isinstance(message.fields[0], dict):
            self._fail(state, "PULL requires an options dictionary.")
            return

        options = cast(dict[str, Any], message.fields[0])
        requested = int(options.get("n", -1))
        if requested == 0:
            _send_success(self.request, self.codec, {"has_more": True})
            return

        pending = state.pending
        remaining = pending.rows[pending.index :]
        if requested < 0:
            limit = len(remaining)
        else:
            limit = min(requested, len(remaining))

        batch = remaining[:limit]
        for row in batch:
            values = [row.get(column) for column in pending.columns]
            _send_message(self.request, self.codec, PackstreamStructure(RECORD, [values]))

        pending.index += len(batch)
        has_more = pending.index < len(pending.rows)
        if has_more:
            _send_success(self.request, self.codec, {"has_more": True})
            return

        state.pending = None
        _send_success(
            self.request,
            self.codec,
            _completion_metadata(
                server,
                pending.statement_type,
                extra_metadata=pending.summary_metadata,
            ),
        )

    def _handle_discard(
        self,
        server: GraphArrowBoltServer,
        state: _ConnectionState,
        message: PackstreamStructure,
    ) -> None:
        """Drop the pending result stream without sending records and finish the statement."""
        if state.pending is None:
            self._fail(state, "DISCARD requires an active result stream.")
            return
        if len(message.fields) != 1 or not isinstance(message.fields[0], dict):
            self._fail(state, "DISCARD requires an options dictionary.")
            return

        pending = state.pending
        state.pending = None
        _send_success(
            self.request,
            self.codec,
            _completion_metadata(
                server,
                pending.statement_type,
                extra_metadata=pending.summary_metadata,
            ),
        )

    def _fail(self, state: _ConnectionState, message: str) -> None:
        """Mark the connection as failed and send a Bolt FAILURE frame to the client."""
        state.failed = True
        _send_failure(
            self.request,
            self.codec,
            code="GraphArrow.ProtocolError",
            message=message,
        )


class GraphArrowBoltServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """Threaded TCP server exposing the minimal Bolt interface."""

    allow_reuse_address = True
    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        bolt_app: BoltInterfaceApplication,
    ) -> None:
        """Bind the threaded TCP server to a prebuilt Bolt application container."""
        self.bolt_app = bolt_app
        super().__init__(server_address, _BoltRequestHandler)


@dataclass(slots=True)
class BoltServerHandle:
    """Running Bolt server plus its background thread."""

    server: GraphArrowBoltServer
    thread: threading.Thread

    @property
    def host(self) -> str:
        """Expose the bound host chosen for the running Bolt server."""
        return cast(str, self.server.server_address[0])

    @property
    def port(self) -> int:
        """Expose the bound port chosen for the running Bolt server."""
        return self.server.server_address[1]

    def stop(self) -> None:
        """Shut down the running Bolt server."""

        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def build_bolt_application(
    path: str | Path,
    *,
    settings: GraphArrowSettings | None = None,
    filesystem: FileSystemAdapter | None = None,
) -> BoltInterfaceApplication:
    """Build the Bolt interface container."""

    resolved_settings = settings or GraphArrowSettings(database_path=str(path))
    grapharrow = build_application(path, settings=resolved_settings, filesystem=filesystem)
    grapharrow.use_cases.admin.initialize()
    return BoltInterfaceApplication(
        grapharrow=grapharrow,
        host=resolved_settings.bolt_host,
        port=resolved_settings.bolt_port,
        auth_token=resolved_settings.bolt_auth_token,
        database_name=_database_name_from_root(grapharrow.layout.root),
    )


def start_bolt_server(application: BoltInterfaceApplication) -> BoltServerHandle:
    """Start the Bolt server in a background thread."""

    server = GraphArrowBoltServer((application.host, application.port), application)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return BoltServerHandle(server=server, thread=thread)


@contextmanager
def run_bolt_server(
    path: str | Path,
    *,
    settings: GraphArrowSettings | None = None,
    filesystem: FileSystemAdapter | None = None,
) -> Any:
    """Context manager that builds and runs a Bolt server."""

    handle = start_bolt_server(
        build_bolt_application(path, settings=settings, filesystem=filesystem)
    )
    try:
        yield handle
    finally:
        handle.stop()


def main() -> int:
    """Run the Bolt server entrypoint."""

    parser = argparse.ArgumentParser(prog="grapharrow-bolt")
    parser.add_argument("--path", default=None, help="Database directory served by Bolt.")
    parser.add_argument("--host", default=None, help="Bolt host override.")
    parser.add_argument("--port", type=int, default=None, help="Bolt port override.")
    args = parser.parse_args()

    settings = GraphArrowSettings()
    database_path = args.path if args.path is not None else settings.database_path
    if database_path is None:
        parser.error("A database path is required via --path or GRAPHARROW_DATABASE_PATH.")

    resolved_settings = settings.model_copy(
        update={
            "database_path": database_path,
            "bolt_host": args.host or settings.bolt_host,
            "bolt_port": args.port or settings.bolt_port,
        }
    )
    handle = start_bolt_server(build_bolt_application(database_path, settings=resolved_settings))
    try:
        handle.thread.join()
    except KeyboardInterrupt:
        handle.stop()
    return 0


def _version_matches(candidate: bytes) -> bool:
    """Check whether one advertised Bolt version tuple is compatible with our server."""
    if len(candidate) != 4 or candidate == b"\x00\x00\x00\x00":
        return False
    range_marker = candidate[1]
    minor = candidate[2]
    major = candidate[3]
    if major != SUPPORTED_BOLT_MAJOR:
        return False
    minimum = max(0, minor - range_marker)
    return minimum <= SUPPORTED_BOLT_MINOR <= minor


def _recv_exact(sock: socket.socket, size: int) -> bytes | None:
    """Read exactly ``size`` bytes from the socket unless the peer closes first."""
    chunks = bytearray()
    while len(chunks) < size:
        chunk = sock.recv(size - len(chunks))
        if not chunk:
            return None
        chunks.extend(chunk)
    return bytes(chunks)


def _recv_message(sock: socket.socket) -> bytes | None:
    """Read one chunked Bolt message and return the assembled PackStream payload."""
    chunks = bytearray()
    while True:
        header = _recv_exact(sock, 2)
        if header is None:
            return None
        chunk_size = struct.unpack(">H", header)[0]
        if chunk_size == 0:
            return bytes(chunks)
        chunk = _recv_exact(sock, chunk_size)
        if chunk is None:
            return None
        chunks.extend(chunk)


def _send_message(
    sock: socket.socket,
    codec: _BoltPackstreamCodec,
    message: PackstreamStructure,
) -> None:
    """Encode and send one Bolt message using the protocol chunking rules."""
    payload = codec.encode(message)
    view = memoryview(payload)
    # Bolt frames logical messages as chunked payloads terminated by an empty
    # chunk, so large records can stream without a separate length prefix.
    while view:
        chunk = view[:65535]
        sock.sendall(struct.pack(">H", len(chunk)))
        sock.sendall(chunk)
        view = view[len(chunk) :]
    sock.sendall(b"\x00\x00")


def _send_success(
    sock: socket.socket,
    codec: _BoltPackstreamCodec,
    metadata: dict[str, Any],
) -> None:
    """Send a Bolt SUCCESS response with the supplied metadata payload."""
    _send_message(sock, codec, PackstreamStructure(SUCCESS, [metadata]))


def _send_failure(
    sock: socket.socket,
    codec: _BoltPackstreamCodec,
    *,
    code: str,
    message: str,
) -> None:
    """Send a Bolt FAILURE response using the error envelope drivers expect."""
    _send_message(
        sock,
        codec,
        PackstreamStructure(
            FAILURE,
            [{"code": code, "message": message}],
        ),
    )


def _validate_auth(metadata: dict[str, Any], *, expected_token: str | None) -> str | None:
    """Validate HELLO auth metadata against the optional server token configuration."""
    scheme = str(metadata.get("scheme", "none"))
    credentials = metadata.get("credentials")
    if expected_token is None:
        if scheme in {"none", "basic"}:
            return None
        return f"Unsupported authentication scheme: {scheme}"
    if scheme != "basic":
        return "Bolt authentication requires the basic scheme."
    if credentials != expected_token:
        return "Invalid Bolt credentials."
    return None


def _statement_type_for_query(query: str) -> str:
    """Classify a statement as read or write for Bolt completion metadata."""
    return "w" if query_requires_write(query) else "r"


def _bookmark_for_snapshot(server: GraphArrowBoltServer) -> str | None:
    """Build the bookmark token for the latest published snapshot, when available."""
    try:
        pointer = server.bolt_app.grapharrow.repositories.snapshot.load_current_pointer()
    except Exception:
        return None
    return f"grapharrow:snapshot:{pointer.snapshot_id.value}"


def _completion_metadata(
    server: GraphArrowBoltServer,
    statement_type: str,
    *,
    extra_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the terminal SUCCESS metadata sent after PULL, DISCARD, or COMMIT."""
    metadata: dict[str, Any] = {
        "has_more": False,
        "type": statement_type,
        "t_last": 0,
        "db": server.bolt_app.database_name,
    }
    bookmark = _bookmark_for_snapshot(server)
    if bookmark is not None:
        metadata["bookmark"] = bookmark
    if extra_metadata:
        metadata.update(extra_metadata)
    return metadata


def _summary_stats(before: GraphState, after: GraphState) -> dict[str, int]:
    """Summarize basic graph-update counters by diffing state before and after one statement."""
    stats: dict[str, int] = {}
    before_nodes = {node.node_id: node for node in before.nodes}
    after_nodes = {node.node_id: node for node in after.nodes}
    before_edges = {edge.edge_id: edge for edge in before.edges}
    after_edges = {edge.edge_id: edge for edge in after.edges}

    nodes_created = len(set(after_nodes) - set(before_nodes))
    nodes_deleted = len(set(before_nodes) - set(after_nodes))
    relationships_created = len(set(after_edges) - set(before_edges))
    relationships_deleted = len(set(before_edges) - set(after_edges))

    labels_added = 0
    labels_removed = 0
    properties_set = 0

    for node_id, node in after_nodes.items():
        previous = before_nodes.get(node_id)
        if previous is None:
            labels_added += len(node.labels)
            properties_set += len(node.properties)
            continue
        labels_added += len(set(node.labels) - set(previous.labels))
        labels_removed += len(set(previous.labels) - set(node.labels))
        properties_set += sum(
            1
            for key, value in node.properties.items()
            if key not in previous.properties or previous.properties[key] != value
        )

    for edge_id, edge in after_edges.items():
        previous_edge = before_edges.get(edge_id)
        if previous_edge is None:
            properties_set += len(edge.properties)
            continue
        properties_set += sum(
            1
            for key, value in edge.properties.items()
            if key not in previous_edge.properties or previous_edge.properties[key] != value
        )

    counter_values = {
        "nodes-created": nodes_created,
        "nodes-deleted": nodes_deleted,
        "relationships-created": relationships_created,
        "relationships-deleted": relationships_deleted,
        "properties-set": properties_set,
        "labels-added": labels_added,
        "labels-removed": labels_removed,
    }
    for key, value in counter_values.items():
        if value > 0:
            stats[key] = value
    return stats


def _database_name_from_root(root: str | Path) -> str:
    """Derive a stable logical database name from the configured root location."""
    if isinstance(root, Path):
        return root.name or "grapharrow"
    cleaned = str(root).rstrip("/")
    if not cleaned:
        return "grapharrow"
    return cleaned.rsplit("/", 1)[-1] or "grapharrow"


__all__ = [
    "BoltInterfaceApplication",
    "BoltServerHandle",
    "GraphArrowBoltServer",
    "build_bolt_application",
    "main",
    "run_bolt_server",
    "start_bolt_server",
]
