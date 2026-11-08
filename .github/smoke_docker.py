"""Exercise a local image without publishing it or using user database paths."""

from __future__ import annotations

import argparse
import json
import secrets
import subprocess
import tempfile
import time
from importlib.metadata import version
from pathlib import Path
from uuid import uuid4

import httpx
from neo4j import GraphDatabase
from neo4j.exceptions import AuthError, ServiceUnavailable, SessionExpired


def docker(*args: str) -> str:
    return subprocess.check_output(["docker", *args], text=True).strip()


def wait_ready(url: str) -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            response = httpx.get(url + "/health", timeout=2)
            if response.status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.5)
    raise RuntimeError("Container HTTP server did not become ready")


def count(client: httpx.Client) -> None:
    response = client.post("/v1/query", json={"query": "MATCH (n:Device) RETURN count(n) AS n"})
    assert response.status_code == 200, response.text
    assert response.json()["data"]["rows"] == [{"n": 2}], response.text


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("image")
    args = parser.parse_args()
    expected = version("grapharrow")
    installed = docker(
        "run",
        "--rm",
        "--entrypoint",
        "/app/.venv/bin/python",
        args.image,
        "-c",
        "from importlib.metadata import version; from importlib.util import find_spec; "
        "assert find_spec('agent_native_graph') is None; assert find_spec('neo4j') is None; "
        "print(version('grapharrow'))",
    )
    assert installed == expected, (installed, expected)
    identity = "grapharrow-smoke-" + uuid4().hex[:12]
    volume = identity + "-data"
    rest_key, ana_key, bolt_key = (secrets.token_urlsafe(32) for _ in range(3))
    docker("volume", "create", volume)
    container = None
    try:
        with tempfile.TemporaryDirectory(prefix="grapharrow-docker-auth-") as temporary:
            root = Path(temporary)
            auth = root / "clients.json"
            auth.write_text(json.dumps({"clients": [{"id": "smoke", "api_key": ana_key}]}))
            auth.chmod(0o644)  # Disposable file read by the container, never a host credential.
            environment = root / "server.env"
            environment.write_text(
                f"GRAPHARROW_SERVER_AUTH_TOKEN={rest_key}\n"
                f"GRAPHARROW_BOLT_AUTH_TOKEN={bolt_key}\n"
                "ANA_AUTH_FILE=/run/grapharrow/clients.json\n"
            )
            environment.chmod(0o600)
            container = docker(
                "run",
                "-d",
                "--name",
                identity,
                "--env-file",
                str(environment),
                "--mount",
                f"type=bind,source={auth},target=/run/grapharrow/clients.json,readonly",
                "--mount",
                f"type=volume,source={volume},target=/data",
                "-p",
                "127.0.0.1::8000",
                "-p",
                "127.0.0.1::7687",
                args.image,
            )
            http_port = docker("port", container, "8000/tcp").rsplit(":", 1)[1]
            bolt_port = docker("port", container, "7687/tcp").rsplit(":", 1)[1]
            url = "http://127.0.0.1:" + http_port
            wait_ready(url)
            with httpx.Client(base_url=url, timeout=15) as public:
                assert public.post("/v1/query", json={"query": "RETURN 1"}).status_code == 401
                assert public.post("/sessions").status_code == 401
                assert public.get("/openapi.json").json()["info"]["version"] == expected
            with httpx.Client(
                base_url=url, headers={"Authorization": "Bearer " + rest_key}, timeout=15
            ) as rest:
                ids = []
                for name in ("alpha", "beta"):
                    response = rest.post(
                        "/v1/nodes",
                        json={
                            "labels": ["Device"] + (["Target"] if name == "beta" else []),
                            "properties": {"name": name},
                        },
                    )
                    assert response.status_code == 201, response.text
                    ids.append(response.json()["data"]["id"])
                response = rest.post(
                    "/v1/edges", json={"src_id": ids[0], "dst_id": ids[1], "rel_type": "LINK"}
                )
                assert response.status_code == 201, response.text
                count(rest)
                sessions = []
                for label in ("Device", "Target"):
                    headers = {"Authorization": "Bearer " + ana_key}
                    response = rest.post("/sessions", headers=headers)
                    assert response.status_code == 201, response.text
                    sid = response.json()["session_id"]
                    assert response.headers["X-ANA-Session"] == sid
                    headers["X-ANA-Session"] = sid
                    response = rest.post(
                        "/tools/call",
                        headers=headers,
                        json={"tool": "count_nodes", "args": {"label": label}},
                    )
                    assert response.status_code == 200, response.text
                    assert response.headers["X-ANA-Session"] == sid
                    assert response.json()["result"]["handle"] == "h1"
                    sessions.append(headers)
                for headers, expected_count in zip(sessions, (2, 1), strict=True):
                    response = rest.post(
                        "/tools/call",
                        headers=headers,
                        json={"tool": "fetch", "args": {"from": "h1"}},
                    )
                    assert response.json()["result"]["rows"] == [[expected_count]], response.text
                    assert rest.delete("/sessions/current", headers=headers).status_code == 204
                docker(
                    "exec",
                    "-d",
                    container,
                    "/app/.venv/bin/grapharrow-bolt",
                    "--host",
                    "0.0.0.0",
                    "--port",
                    "7687",
                    "--no-query-log",
                )
                bolt_url = "bolt://127.0.0.1:" + bolt_port
                deadline = time.monotonic() + 30
                while True:
                    try:
                        with GraphDatabase.driver(
                            bolt_url, auth=("smoke", bolt_key), connection_timeout=2
                        ) as driver:
                            driver.verify_connectivity()
                            with driver.session() as session:
                                assert (
                                    session.run("MATCH (n:Device) RETURN count(n) AS n").single()[
                                        "n"
                                    ]
                                    == 2
                                )
                        break
                    except (ServiceUnavailable, SessionExpired):
                        # Retry connection setup only; an assertion is a semantic failure.
                        if time.monotonic() >= deadline:
                            raise
                        time.sleep(0.5)
                try:
                    with GraphDatabase.driver(
                        bolt_url, auth=("smoke", "wrong"), connection_timeout=2
                    ) as driver:
                        driver.verify_connectivity()
                except AuthError:
                    pass
                else:
                    raise AssertionError("Bolt accepted an invalid password")
                docker("restart", container)
                http_port = docker("port", container, "8000/tcp").rsplit(":", 1)[1]
                restarted_url = "http://127.0.0.1:" + http_port
                wait_ready(restarted_url)
                with httpx.Client(
                    base_url=restarted_url, headers={"Authorization": "Bearer " + rest_key}
                ) as reopened:
                    count(reopened)
            print(
                f"Docker smoke passed: {args.image}, GraphArrow {expected}; "
                "REST/ANA/Bolt and persistence"
            )
    finally:
        if container:
            try:
                print(docker("logs", "--tail", "25", container))
            finally:
                docker("rm", "-f", container)
        docker("volume", "rm", volume)


if __name__ == "__main__":
    main()
