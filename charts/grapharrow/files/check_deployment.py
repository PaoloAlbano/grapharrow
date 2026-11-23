"""Read-only checks for the GraphArrow Service created by the chart."""

import json
import os
import socket
from http.client import HTTPConnection, HTTPException
from pathlib import Path

from grapharrow.interfaces.bolt import (
    BOLT_MAGIC,
    GOODBYE,
    HELLO,
    PULL,
    RECORD,
    RUN,
    SUCCESS,
    SUPPORTED_BOLT_VERSION_BYTES,
    PackstreamStructure,
    _BoltPackstreamCodec,
    _recv_exact,
    _recv_message,
    _send_message,
)

SERVER = os.environ["GRAPHARROW_SMOKE_HOST"]
AUTH = Path("/run/grapharrow-smoke-auth")
QUERY = "RETURN 1 AS ok"


def request(method, path, status, *, token=None, session=None, body=None):
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    if session is not None:
        headers["X-ANA-Session"] = session
    if body is not None:
        headers["Content-Type"] = "application/json"
    client = HTTPConnection(SERVER, 8000, timeout=8)
    try:
        client.request(method, path, json.dumps(body) if body is not None else None, headers)
        response = client.getresponse()
        payload = response.read()
        if response.status != status:
            raise RuntimeError(
                f"{method} {path} returned HTTP {response.status}, expected {status}"
            )
        return json.loads(payload) if payload else None, response.getheader("X-ANA-Session")
    finally:
        client.close()


def credential(name):
    value = (AUTH / name).read_text()
    if not value or any(char.isspace() for char in value):
        raise RuntimeError(f"The authentication Secret needs a nonempty {name} without whitespace")
    return value


def check_rest():
    token = credential("rest-token")
    request("POST", "/v1/query", 401, body={"query": QUERY})
    result, _ = request("POST", "/v1/query", 200, token=token, body={"query": QUERY})
    if result["data"]["rows"] != [{"ok": 1}]:
        raise RuntimeError("Cypher query returned an unexpected result")


def check_ana():
    data = json.loads((AUTH / "clients.json").read_bytes())
    if not data["clients"]:
        raise RuntimeError("The authentication Secret needs at least one ANA client")
    token = data["clients"][0]["api_key"]
    response, session_header = request("POST", "/sessions", 201, token=token)
    session = response["session_id"]
    try:
        if session_header != session:
            raise RuntimeError("Session ID and X-ANA-Session header differ")
        result, _ = request(
            "POST",
            "/tools/call",
            200,
            token=token,
            session=session,
            body={"tool": "count_nodes", "args": {"label": "GraphArrowSmokeTest"}},
        )
        handle = result["result"]["handle"]
        result, _ = request(
            "POST",
            "/tools/call",
            200,
            token=token,
            session=session,
            body={"tool": "fetch", "args": {"from": handle}},
        )
        [[count]] = result["result"]["rows"]
        if not isinstance(count, int) or count < 0:
            raise RuntimeError("ANA returned an invalid node count")
    finally:
        request("DELETE", "/sessions/current", 204, token=token, session=session)


def expect_bolt(socket_, codec, signature, phase):
    payload = _recv_message(socket_)
    reply = codec.decode(payload) if payload else None
    if not isinstance(reply, PackstreamStructure) or reply.signature != signature:
        raise RuntimeError(f"Bolt {phase} did not receive the expected response")
    return reply.fields


def check_bolt():
    token = credential("bolt-token")
    codec = _BoltPackstreamCodec()
    with socket.create_connection((SERVER, 7687), timeout=8) as connection:
        connection.sendall(BOLT_MAGIC + SUPPORTED_BOLT_VERSION_BYTES + bytes(12))
        if _recv_exact(connection, 4) != SUPPORTED_BOLT_VERSION_BYTES:
            raise RuntimeError("Bolt protocol negotiation failed")
        auth = {"scheme": "basic", "principal": "helm-test", "credentials": token}
        _send_message(connection, codec, PackstreamStructure(HELLO, [auth]))
        expect_bolt(connection, codec, SUCCESS, "authentication")
        _send_message(connection, codec, PackstreamStructure(RUN, [QUERY, {}, {}]))
        expect_bolt(connection, codec, SUCCESS, "RUN")
        _send_message(connection, codec, PackstreamStructure(PULL, [{"n": -1}]))
        if expect_bolt(connection, codec, RECORD, "PULL") != [[1]]:
            raise RuntimeError("Cypher query returned an unexpected Bolt record")
        expect_bolt(connection, codec, SUCCESS, "result completion")
        _send_message(connection, codec, PackstreamStructure(GOODBYE))


def main():
    for name, check in (("REST", check_rest), ("ANA", check_ana), ("Bolt", check_bolt)):
        try:
            check()
        except RuntimeError as error:
            raise SystemExit(f"{name} failed: {error}") from None
        except (HTTPException, OSError, KeyError, TypeError, ValueError) as error:
            raise SystemExit(f"{name} failed ({type(error).__name__})") from None
        print(f"{name} passed", flush=True)


if __name__ == "__main__":
    main()
