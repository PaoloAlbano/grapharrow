"""Verify GraphArrow deployment on the disposable kind-grapharrow-helm cluster."""

from __future__ import annotations

import argparse
import csv
import json
import re
import secrets
import ssl
import subprocess
import sys
import tarfile
import time
from base64 import b64encode
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event
from typing import TypeVar

import httpx
from azure.storage.blob import BlobServiceClient, ContainerSasPermissions, generate_container_sas
from neo4j import GraphDatabase, TrustCustomCAs
from neo4j.exceptions import WriteServiceUnavailable

CONTEXT = "kind-grapharrow-helm"
CHART = Path("charts/grapharrow")
TLS_EDGE = Path("examples/kubernetes/tls-edge")
AZURITE = (
    "mcr.microsoft.com/azure-storage/azurite:3.37.0"
    "@sha256:830430c1da1a2d537e08f3e6764dd1f5ae00cf0346bcaf625b968ec3f0971fd5"
)
T = TypeVar("T")


def run(*args: str, input_text: str | None = None) -> str:
    return subprocess.check_output(args, text=True, input=input_text).strip()


def kubectl(*args: str, input_text: str | None = None) -> str:
    return run("kubectl", "--context", CONTEXT, *args, input_text=input_text)


def eventually(read: Callable[[], T], expected: T, *, timeout: float = 45) -> None:
    deadline = time.monotonic() + timeout
    result: T | httpx.HTTPError | None = None
    while time.monotonic() < deadline:
        try:
            result = read()
        except httpx.TransportError as exc:
            result = exc
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code not in (502, 503, 504):
                raise
            result = exc
        if result == expected:
            return
        time.sleep(0.5)
    raise AssertionError((result, expected))


@contextmanager
def port_forward(
    namespace: str, target: str, *remote: int, target_ports: tuple[int, ...] = ()
) -> Iterator[dict[int, str]]:
    # kubectl reports the target ports when forwarding through a Service.
    target_ports = target_ports or remote
    with TemporaryDirectory() as temporary:
        output_path = Path(temporary) / "forward.log"
        with (
            output_path.open("w") as output,
            subprocess.Popen(
                [
                    "kubectl",
                    "--context",
                    CONTEXT,
                    "-n",
                    namespace,
                    "port-forward",
                    "--address",
                    "127.0.0.1",
                    target,
                    *(f":{port}" for port in remote),
                ],
                stdout=output,
                stderr=subprocess.STDOUT,
                text=True,
            ) as process,
        ):
            try:
                deadline = time.monotonic() + 15
                while process.poll() is None and time.monotonic() < deadline:
                    ports = {
                        int(port): local
                        for local, port in re.findall(
                            r"Forwarding from 127\.0\.0\.1:(\d+) -> (\d+)",
                            output_path.read_text(),
                        )
                    }
                    if ports.keys() >= set(target_ports):
                        yield {
                            port: ports[container_port]
                            for port, container_port in zip(remote, target_ports, strict=True)
                        }
                        return
                    time.sleep(0.1)
                raise RuntimeError(output_path.read_text())
            finally:
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=5)


@contextmanager
def forward(namespace: str, target: str) -> Iterator[tuple[str, str]]:
    with port_forward(namespace, target, 8000, 7687) as ports:
        yield (f"http://127.0.0.1:{ports[8000]}", f"bolt://127.0.0.1:{ports[7687]}")


class Deployment:
    def __init__(self, namespace: str, image: str, temporary: Path, chart: Path = CHART) -> None:
        repository, tag = image.rsplit(":", 1)
        self.namespace = namespace
        self.chart = chart
        self.options = [
            "--kube-context",
            CONTEXT,
            "--namespace",
            namespace,
            "--wait",
            "--timeout",
            "150s",
        ]
        self.values = [
            "--set-string",
            "auth.existingSecret=grapharrow-auth",
            "--set-string",
            "requestMetrics.existingSecret=grapharrow-metrics",
            "--set-string",
            f"image.repository={repository}",
            "--set-string",
            f"image.tag={tag}",
            "--set",
            "image.pullPolicy=Never",
        ]
        self.rest_key, self.bolt_key, self.ana_key, self.metrics_key = [
            secrets.token_urlsafe(32) for _ in range(4)
        ]
        self.temporary = temporary

    def secret(self, name: str, files: dict[str, str]) -> None:
        folder = self.temporary / name
        folder.mkdir(mode=0o700, exist_ok=True)
        args = []
        for key, value in files.items():
            path = folder / key
            path.write_text(value)
            args.append(f"--from-file={key}={path}")
        kubectl("-n", self.namespace, "create", "secret", "generic", name, *args)

    def setup_auth(self) -> None:
        self.secret(
            "grapharrow-auth",
            {
                "rest-token": self.rest_key,
                "bolt-token": self.bolt_key,
                "clients.json": json.dumps({"clients": [{"id": "smoke", "api_key": self.ana_key}]}),
            },
        )
        self.secret("grapharrow-metrics", {"metrics-token": self.metrics_key})

    def helm(self, action: str, *args: str) -> None:
        print(run("helm", action, *self.options, *args), flush=True)

    def profile(self, name: str) -> list[str]:
        return ["--values", str(self.chart_file(f"profiles/{name}"))]

    def chart_file(self, name: str) -> Path:
        if self.chart.is_dir():
            return self.chart / name
        path = self.temporary / Path(name).name
        with tarfile.open(self.chart, "r:gz") as archive:
            member = archive.extractfile(f"grapharrow/{name}")
            if member is None:
                raise ValueError(f"Chart file is missing: {name}")
            with member:
                path.write_bytes(member.read())
        return path

    def helm_test(self, release: str) -> None:
        subprocess.run(
            [
                "helm",
                "test",
                release,
                "--kube-context",
                CONTEXT,
                "--namespace",
                self.namespace,
                "--timeout",
                "90s",
                "--logs",
            ],
            check=True,
        )

    def count(self, url: str) -> int:
        return self.query(url, "MATCH (n:Smoke) RETURN count(n) AS n")[0]["n"]

    def query(self, url: str, query: str, *, idempotency_key: str | None = None) -> list[dict]:
        headers = {"Authorization": f"Bearer {self.rest_key}"}
        if idempotency_key is not None:
            headers["Idempotency-Key"] = idempotency_key
        response = httpx.post(
            url + "/v1/query",
            headers=headers,
            json={"query": query},
            timeout=25,
        )
        response.raise_for_status()
        return response.json()["data"]["rows"]

    def session(self, url: str) -> dict[str, str]:
        headers = {"Authorization": f"Bearer {self.ana_key}"}
        response = httpx.post(url + "/sessions", headers=headers, timeout=10)
        response.raise_for_status()
        sid = response.json()["session_id"]
        assert response.headers["X-ANA-Session"] == sid
        return {**headers, "X-ANA-Session": sid}

    def ana(self, url: str, headers: dict[str, str], tool: str, args: dict) -> httpx.Response:
        return httpx.post(
            url + "/tools/call", headers=headers, json={"tool": tool, "args": args}, timeout=10
        )

    def verify_ana(self, url: str, headers: dict[str, str], count: int) -> None:
        response = self.ana(url, headers, "count_nodes", {"label": "Smoke"})
        response.raise_for_status()
        assert response.json()["result"]["handle"] == "h1"
        response = self.ana(url, headers, "fetch", {"from": "h1"})
        response.raise_for_status()
        assert response.json()["result"]["rows"] == [[count]], response.text

    def verify_bolt(self, url: str, count: int, *, certificate: Path | None = None) -> None:
        tls = (
            {"encrypted": True, "trusted_certificates": TrustCustomCAs(str(certificate))}
            if certificate
            else {}
        )
        with (
            GraphDatabase.driver(url, auth=("smoke", self.bolt_key), **tls) as driver,
            driver.session() as s,
        ):
            assert s.run("MATCH (n:Smoke) RETURN count(n) AS n").single()["n"] == count

    def verify_metrics(self, url: str) -> None:
        path = url + "/v1/metrics"
        rest_headers = {"Authorization": f"Bearer {self.rest_key}"}
        metrics_headers = {"Authorization": f"Bearer {self.metrics_key}"}
        assert httpx.get(path, headers=rest_headers, timeout=10).status_code == 401
        response = httpx.get(path, headers=metrics_headers, timeout=10)
        response.raise_for_status()
        assert response.headers["Content-Type"].startswith("text/plain; version=0.0.4")
        for interface in ("rest_cypher", "ana"):
            label = f'{{interface="{interface}"}}'
            sample = re.search(
                rf"^grapharrow_http_operation_execution_seconds_count{re.escape(label)} (\d+)$",
                response.text,
                re.MULTILINE,
            )
            assert sample is not None and int(sample[1]) > 0, response.text
        unauthorized = httpx.post(
            url + "/v1/query", json={"query": "RETURN 1"}, headers=metrics_headers, timeout=10
        )
        assert unauthorized.status_code == 401

    def native(self) -> None:
        options = [*self.profile("native.yaml"), *self.values]
        self.helm("install", "native", str(self.chart), *options)
        self.helm_test("native")
        with forward(self.namespace, "service/native") as (http, bolt):
            self.query(http, "CREATE (:Smoke {id: 1})")
            self.verify_ana(http, self.session(http), 1)
            self.verify_bolt(bolt, 1)
            self.verify_metrics(http)
        self.helm(
            "upgrade",
            "native",
            str(self.chart),
            *options,
            "--set",
            "resources.http.requests.cpu=25m,requestMetrics.enabled=false",
        )
        with forward(self.namespace, "service/native") as (http, _):
            headers = {"Authorization": f"Bearer {self.metrics_key}"}
            assert httpx.get(http + "/v1/metrics", headers=headers, timeout=10).status_code == 404
        self.helm("rollback", "native", "1")
        with forward(self.namespace, "service/native") as (http, bolt):
            assert self.count(http) == 1
            self.verify_bolt(bolt, 1)
        print("Native PVC, read-only containers, upgrade and rollback passed", flush=True)

    def setup_azurite(self) -> str:
        node = "grapharrow-helm-worker"
        account = "grapharrowtest"
        key = b64encode(secrets.token_bytes(64)).decode()
        self.secret("azurite-auth", {"AZURITE_ACCOUNTS": f"{account}:{key}"})
        endpoint = {
            "GRAPHARROW_AZURE_ACCOUNT_NAME": account,
            **{
                f"GRAPHARROW_AZURE_{service}_STORAGE_{setting}": value
                for service in ("BLOB", "DFS")
                for setting, value in (
                    ("SCHEME", "http"),
                    ("AUTHORITY", f"azurite:10000/{account}"),
                )
            },
        }
        self.secret("grapharrow-azure-storage", {**endpoint, "GRAPHARROW_AZURE_ACCOUNT_KEY": key})
        sas = generate_container_sas(
            account,
            "graphs",
            account_key=key,
            permission=ContainerSasPermissions(read=True, list=True),
            expiry=datetime.now(UTC) + timedelta(hours=1),
        )
        self.secret("grapharrow-reader-storage", {**endpoint, "GRAPHARROW_AZURE_SAS_TOKEN": sas})
        azurite_args = [
            "azurite-blob",
            "--blobHost",
            "0.0.0.0",
            "--inMemoryPersistence",
            "--disableProductStyleUrl",
            "--skipApiVersionCheck",
            "--disableTelemetry",
        ]
        kubectl(
            "-n",
            self.namespace,
            "apply",
            "-f",
            "-",
            input_text=f"""
apiVersion: v1
kind: Service
metadata:
  name: azurite
spec:
  selector:
    app: azurite
  ports:
    - port: 10000
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: azurite
spec:
  selector:
    matchLabels:
      app: azurite
  template:
    metadata:
      labels:
        app: azurite
    spec:
      nodeSelector:
        kubernetes.io/hostname: {node}
      containers:
        - name: azurite
          image: {AZURITE}
          args: {json.dumps(azurite_args)}
          envFrom:
            - secretRef:
                name: azurite-auth
          readinessProbe:
            tcpSocket:
              port: 10000
""",
        )
        kubectl("-n", self.namespace, "rollout", "status", "deployment/azurite", "--timeout=90s")
        with port_forward(self.namespace, "service/azurite", 10000) as ports:
            with BlobServiceClient(
                account_url=f"http://127.0.0.1:{ports[10000]}/{account}",
                credential={"account_name": account, "account_key": key},
            ) as client:
                client.create_container("graphs")
        return node

    def pods(self, component: str, *, release: str = "grapharrow") -> dict[str, str]:
        pods = json.loads(
            kubectl(
                "-n",
                self.namespace,
                "get",
                "pod",
                "-o",
                "json",
                "-l",
                f"app.kubernetes.io/instance={release},app.kubernetes.io/component={component}",
            )
        )
        return {
            pod["metadata"]["name"]: pod["spec"]["nodeName"]
            for pod in pods["items"]
            if "deletionTimestamp" not in pod["metadata"]
        }

    def tls_edge(self, servers: set[str], count: int) -> None:
        certificate = self.temporary / "edge.crt"
        key = self.temporary / "edge.key"
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=localhost",
                "-addext",
                "subjectAltName=IP:127.0.0.1",
                "-keyout",
                str(key),
                "-out",
                str(certificate),
            ],
            check=True,
            capture_output=True,
        )
        kubectl(
            "-n",
            self.namespace,
            "create",
            "secret",
            "tls",
            "grapharrow-edge-tls",
            f"--cert={certificate}",
            f"--key={key}",
        )
        verify = ssl.create_default_context(cafile=str(certificate))
        for variant in ("https", "bolt-tls"):
            kubectl("-n", self.namespace, "apply", "-k", str(TLS_EDGE / variant))
            kubectl(
                "-n",
                self.namespace,
                "rollout",
                "status",
                "deployment/grapharrow-tls-edge",
                "--timeout=90s",
            )
            with port_forward(
                self.namespace, "service/grapharrow-https", 443, target_ports=(8443,)
            ) as ports:
                url = f"https://127.0.0.1:{ports[443]}/v1/query"
                with httpx.Client(verify=verify, timeout=10) as client:
                    payload = {"query": "MATCH (n:Smoke) RETURN count(n) AS n"}
                    assert client.post(url, json=payload).status_code == 401
                    response = client.post(
                        url, json=payload, headers={"Authorization": f"Bearer {self.rest_key}"}
                    )
                    response.raise_for_status()
                    assert response.json()["data"]["rows"] == [{"n": count}]
            if variant == "https":
                assert not kubectl(
                    "-n", self.namespace, "get", "service/grapharrow-bolt-tls", "--ignore-not-found"
                )

        pods = json.loads(
            kubectl(
                "-n",
                self.namespace,
                "get",
                "pods",
                "-l",
                "app.kubernetes.io/name=grapharrow-tls-edge",
                "--field-selector=status.phase=Running",
                "-o",
                "json",
            )
        )["items"]
        first, second = (
            pod["metadata"]["name"] for pod in pods if "deletionTimestamp" not in pod["metadata"]
        )
        with (
            port_forward(self.namespace, f"pod/{first}", 8443) as first_ports,
            port_forward(self.namespace, f"pod/{second}", 8443) as second_ports,
            httpx.Client(verify=verify, timeout=10) as client,
        ):
            source = f"https://127.0.0.1:{first_ports[8443]}"
            destination = f"https://127.0.0.1:{second_ports[8443]}"
            auth = {"Authorization": f"Bearer {self.ana_key}"}
            seen = set()
            for _ in range(16):
                response = client.post(source + "/sessions", headers=auth)
                response.raise_for_status()
                session = response.json()["session_id"]
                assert response.headers["X-ANA-Session"] == session
                headers = {**auth, "X-ANA-Session": session}
                response = client.post(
                    source + "/tools/call",
                    headers=headers,
                    json={"tool": "count_nodes", "args": {"label": "Smoke"}},
                )
                response.raise_for_status()
                handle = response.json()["result"]["handle"]
                assert handle == "h1"
                response = client.post(
                    destination + "/tools/call",
                    headers=headers,
                    json={"tool": "fetch", "args": {"from": handle}},
                )
                response.raise_for_status()
                assert response.json()["result"]["rows"] == [[count]]
                client.delete(destination + "/sessions/current", headers=headers).raise_for_status()
                seen.add(session.split(".", 1)[0])
                if seen == servers:
                    break
            assert seen == servers, seen

        with port_forward(
            self.namespace, "service/grapharrow-bolt-tls", 7687, target_ports=(8767,)
        ) as ports:
            self.verify_bolt(f"bolt://127.0.0.1:{ports[7687]}", count, certificate=certificate)
        print("REST HTTPS, ANA affinity across TLS proxies and Bolt TLS passed", flush=True)

    def check_autoscaling(self, options: list[str], count: int) -> None:
        self.helm(
            "upgrade",
            "grapharrow",
            str(self.chart),
            *options,
            "--set",
            "autoscaling.enabled=true,autoscaling.minReplicas=2,autoscaling.maxReplicas=4",
        )
        workload = "statefulset/grapharrow"
        kubectl(
            "-n",
            self.namespace,
            "wait",
            "--for=jsonpath={.spec.replicas}=2",
            workload,
            "--timeout=60s",
        )
        kubectl("-n", self.namespace, "rollout", "status", workload, "--timeout=60s")
        kubectl(
            "-n",
            self.namespace,
            "rollout",
            "status",
            "deployment/grapharrow-gateway",
            "--timeout=60s",
        )
        gateways = self.pods("gateway")
        with forward(self.namespace, "service/grapharrow") as (http, bolt):
            session = self.session(http)
            self.verify_ana(http, session, count)

            # Exercise the scale subresource used by the HPA without depending on CPU metrics.
            kubectl("-n", self.namespace, "scale", workload, "--replicas=4")
            newer_pods = {"grapharrow-2", "grapharrow-3"}
            seen = set()

            def new_workers() -> set[str]:
                headers = self.session(http)
                owner = headers["X-ANA-Session"].split(".", 1)[0]
                self.verify_ana(http, headers, count)
                if owner in newer_pods:
                    seen.add(owner)
                return seen

            # Exercise the session route as each new pod first enters the HTTP pool.
            eventually(new_workers, newer_pods, timeout=60)
            response = self.ana(http, session, "fetch", {"from": "h1"})
            response.raise_for_status()
            assert response.json()["result"]["rows"] == [[count]]
            assert self.count(http) == count
            self.verify_bolt(bolt, count)
        assert gateways == self.pods("gateway")
        print(
            "Autoscaling routes new pods without a gateway rollout or losing ANA sessions",
            flush=True,
        )

    def verify_worker_drain(
        self, release: str, replicas: int, count: int, *, writable: bool = False
    ) -> None:
        gateways = self.pods("gateway", release=release)
        gateway = next(iter(gateways))
        ordinal = replicas - 1
        target = f"{release}-{ordinal}"

        def bolt_stats() -> dict[str, str]:
            stats = kubectl(
                "-n",
                self.namespace,
                "exec",
                gateway,
                "-c",
                "haproxy",
                "-i",
                "--",
                "socat",
                "-",
                "UNIX-CONNECT:/run/haproxy/admin.sock",
                input_text="show stat\n",
            )
            return next(
                row
                for row in csv.DictReader(stats.removeprefix("# ").splitlines())
                if row["pxname"] == "bolt" and row["svname"] == f"app_{ordinal}"
            )

        with (
            forward(self.namespace, f"pod/{target}") as (direct, _),
            forward(self.namespace, f"pod/{gateway}") as (http, bolt),
            GraphDatabase.driver(bolt, auth=("smoke", self.bolt_key)) as driver,
        ):
            ana_session = self.session(direct)
            assert ana_session["X-ANA-Session"].startswith(target + ".")
            self.verify_ana(http, ana_session, count)
            eventually(lambda: bolt_stats()["status"], "UP")
            with ExitStack() as active:
                query = "MATCH (n:Smoke) RETURN count(n) AS n"
                transactions = []
                departing = None
                for _ in range(replicas):
                    before = int(bolt_stats()["scur"])
                    session = active.enter_context(driver.session())
                    tx = active.enter_context(session.begin_transaction())
                    assert tx.run(query).single()["n"] == count
                    if int(bolt_stats()["scur"]) > before:
                        departing = tx
                    transactions.append(tx)
                assert departing is not None, "No open Bolt transaction on the departing pod"
                if writable:
                    probe_id = departing.run(
                        "CREATE (n:DrainProbe {value: 0}) RETURN id(n) AS id"
                    ).single()["id"]
                log_path = self.temporary / f"{release}-drain.log"
                command = [
                    sys.executable,
                    str(self.chart_file("files/scale_down.py")),
                    release,
                    "--namespace",
                    self.namespace,
                    "--context",
                    CONTEXT,
                    "--grace-seconds",
                    "24" if writable else "16",
                ]
                with (
                    log_path.open("w") as output,
                    subprocess.Popen(
                        command,
                        stdout=output,
                        stderr=subprocess.STDOUT,
                        text=True,
                    ) as drain,
                ):
                    eventually(lambda: f"Draining {target}" in log_path.read_text(), True)
                    competing = subprocess.run(command, capture_output=True, text=True, timeout=8)
                    assert competing.returncode != 0 and "AlreadyExists" in competing.stderr
                    before = bolt_stats()["stot"]
                    auth = {"Authorization": f"Bearer {self.ana_key}"}
                    for pod in gateways:
                        with port_forward(self.namespace, f"pod/{pod}", 8000) as ports:
                            endpoint = f"http://127.0.0.1:{ports[8000]}"
                            with httpx.Client(timeout=5, headers=auth) as client:
                                for _ in range(4):
                                    response = client.post(endpoint + "/sessions")
                                    response.raise_for_status()
                                    token = response.headers["X-ANA-Session"]
                                    assert not token.startswith(target + "."), token
                                    client.delete(
                                        endpoint + "/sessions/current",
                                        headers={"X-ANA-Session": token},
                                    ).raise_for_status()
                    for _ in range(3):
                        self.verify_bolt(bolt, count)
                    assert bolt_stats()["stot"] == before
                    for tx in transactions:
                        assert tx.run(query).single()["n"] == count
                        tx.commit()
                    response = self.ana(http, ana_session, "fetch", {"from": "h1"})
                    response.raise_for_status()
                    assert response.json()["result"]["rows"] == [[count]]
                    if writable:
                        stopped = Event()
                        wrote, read = Event(), Event()
                        executor = active.enter_context(ThreadPoolExecutor(max_workers=2))
                        active.callback(stopped.set)
                        writer = executor.submit(
                            self.delta_drain_writes, http, probe_id, stopped, wrote
                        )
                        reader = executor.submit(
                            self.delta_drain_reads, http, target, count, stopped, read
                        )
                        assert wrote.wait(timeout=12) and read.wait(timeout=2), (
                            "No mixed traffic completed during the drain"
                        )
                    assert drain.wait(timeout=110) == 0, log_path.read_text()
                    if writable:
                        stopped.set()
                        value = writer.result(timeout=30)
                        workflows = reader.result(timeout=15)
                        assert self.query(http, "MATCH (n:DrainProbe) RETURN n.value AS value") == [
                            {"value": value}
                        ]
                        self.verify_ana(http, self.session(http), count)
                        print(
                            f"Delta drain completed {value} writes and {workflows} ANA workflows",
                            flush=True,
                        )
        assert target not in self.pods("server", release=release)
        assert len(self.pods("server", release=release)) == replicas - 1
        assert not kubectl(
            "-n", self.namespace, "get", f"lease/{release}-drain", "--ignore-not-found"
        )
        print(
            f"{release} {replicas}→{replicas - 1} drain kept existing ANA and Bolt traffic"
            + (" and completed mixed reads and writes" if writable else ""),
            flush=True,
        )

    def delta_drain_writes(self, http: str, node_id: int, stop: Event, wrote: Event) -> int:
        value = 0
        while not stop.is_set():
            value += 1
            update = (
                f"MATCH (n:DrainProbe) WHERE id(n)={node_id} "
                f"SET n.value={value} RETURN n.value AS value"
            )
            assert self.query(http, update, idempotency_key=f"drain-{value}") == [{"value": value}]
            wrote.set()
            stop.wait(0.15)
        return value

    def delta_drain_reads(
        self, http: str, target: str, count: int, stop: Event, read: Event
    ) -> int:
        workflows = 0
        while not stop.is_set():
            session = self.session(http)
            assert not session["X-ANA-Session"].startswith(target + ".")
            self.verify_ana(http, session, count)
            httpx.delete(http + "/sessions/current", headers=session, timeout=10).raise_for_status()
            workflows += 1
            read.set()
            stop.wait(0.15)
        return workflows

    def native_readers(self) -> None:
        uri = ["--set-string", "storage.uri=az://graphs/native-readers"]
        self.helm(
            "install",
            "native-writer",
            str(self.chart),
            *self.profile("native-writer.yaml"),
            *self.values,
            *uri,
            "--set-string",
            "storage.existingSecret=grapharrow-azure-storage",
        )
        with forward(self.namespace, "service/native-writer") as (writer, writer_bolt):
            self.query(writer, "CREATE (:Smoke {id: 1})")
            with GraphDatabase.driver(writer_bolt, auth=("smoke", self.bolt_key)) as driver:

                def write(index: int) -> None:
                    statement = f"CREATE (:ConcurrentSmoke {{id: {index}}})"
                    if index % 2 == 0:
                        self.query(writer, statement)
                    else:
                        with driver.session() as session:
                            session.run(statement).consume()

                with ThreadPoolExecutor(max_workers=4) as executor:
                    list(executor.map(write, range(4)))
            assert self.query(writer, "MATCH (n:ConcurrentSmoke) RETURN count(n) AS n") == [
                {"n": 4}
            ]
            self.helm(
                "install",
                "native-readers",
                str(self.chart),
                *self.profile("native-readers.yaml"),
                *self.values,
                *uri,
                "--set",
                "autoscaling.enabled=true,autoscaling.maxReplicas=3",
            )
            readers = "statefulset/native-readers"
            kubectl(
                "-n",
                self.namespace,
                "wait",
                "--for=jsonpath={.status.readyReplicas}=2",
                readers,
                "--timeout=75s",
            )
            self.helm_test("native-readers")
            with forward(self.namespace, "service/native-readers") as (reader, bolt):
                old_session = self.session(reader)
                self.verify_ana(reader, old_session, 1)
                auth = {"Authorization": f"Bearer {self.rest_key}"}
                response = httpx.post(
                    reader + "/v1/query",
                    json={"query": "CREATE (:Smoke)"},
                    headers=auth,
                    timeout=10,
                )
                assert (
                    response.status_code == 403 and response.json()["error"]["code"] == "READ_ONLY"
                )
                with GraphDatabase.driver(bolt, auth=("smoke", self.bolt_key)) as driver:
                    with driver.session() as session, session.begin_transaction() as tx:
                        assert tx.run("MATCH (n:Smoke) RETURN count(n) AS n").single()["n"] == 1
                        self.query(writer, "CREATE (:Smoke {id: 2})")
                        assert tx.run("MATCH (n:Smoke) RETURN count(n) AS n").single()["n"] == 1
                        tx.commit()
                    try:
                        with driver.session() as session, session.begin_transaction() as tx:
                            tx.run("CREATE (:Smoke)").consume()
                    except WriteServiceUnavailable:
                        # The Neo4j driver translates ForbiddenOnReadOnlyDatabase for direct Bolt.
                        pass
                    else:
                        raise AssertionError("The Native reader accepted a Bolt write")
                assert self.count(reader) == 2
                old_count = self.ana(reader, old_session, "count_nodes", {"label": "Smoke"})
                old_count.raise_for_status()
                fetch = self.ana(
                    reader, old_session, "fetch", {"from": old_count.json()["result"]["handle"]}
                )
                fetch.raise_for_status()
                assert fetch.json()["result"]["rows"] == [[1]]
                self.verify_ana(reader, self.session(reader), 2)
                kubectl("-n", self.namespace, "scale", readers, "--replicas=3")
                kubectl("-n", self.namespace, "rollout", "status", readers, "--timeout=60s")
                eventually(
                    lambda: self.session(reader)["X-ANA-Session"].startswith("native-readers-2."),
                    True,
                )
                self.verify_worker_drain("native-readers", 3, 2)
                kubectl("-n", self.namespace, "scale", "statefulset/native-writer", "--replicas=0")
                kubectl(
                    "-n",
                    self.namespace,
                    "wait",
                    "--for=delete",
                    "pod/native-writer-0",
                    "--timeout=75s",
                )
                assert self.count(reader) == 2
                self.verify_bolt(bolt, 2)
        self.helm("uninstall", "native-readers", "native-writer")
        print(
            "Native object storage writer, read-only replicas and snapshot isolation passed",
            flush=True,
        )

    def delta(self) -> None:
        storage_node = self.setup_azurite()
        self.native_readers()
        options = [
            *self.profile("delta-azure.yaml"),
            *self.values,
            "--set-string",
            "storage.uri=az://graphs/smoke",
        ]
        for replicas in (1, 2, 4):
            action = "install" if replicas == 1 else "upgrade"
            self.helm(
                action, "grapharrow", str(self.chart), *options, "--set", f"replicaCount={replicas}"
            )
            kubectl(
                "-n",
                self.namespace,
                "rollout",
                "status",
                "deployment/grapharrow-gateway",
                "--timeout=60s",
            )
            with forward(self.namespace, "service/grapharrow") as (http, _):
                eventually(lambda: self.count(http), 0)
        self.helm_test("grapharrow")
        servers = self.pods("server")
        assert len(servers) == 4 and len(set(servers.values())) >= 2, servers
        gateways = self.pods("gateway")
        assert len(set(gateways.values())) >= 2, gateways
        lost_node = next(node for node in servers.values() if node != storage_node)
        gateway = next(name for name, node in gateways.items() if node != lost_node)
        with forward(self.namespace, f"pod/{gateway}") as (http, bolt):

            def write(i: int) -> None:
                for attempt in range(12):
                    try:
                        self.query(
                            http, f"CREATE (:Smoke {{id: {i}}})", idempotency_key=f"smoke-{i}"
                        )
                        return
                    except httpx.HTTPStatusError as exc:
                        if (
                            exc.response.status_code != 409
                            or exc.response.json()["error"]["code"] != "WRITE_CONFLICT"
                            or attempt == 11
                        ):
                            raise
                        time.sleep(0.025 * (attempt + 1))

            with ThreadPoolExecutor(max_workers=8) as executor:
                list(executor.map(write, range(8)))
            write(0)
            for pod in servers:
                with forward(self.namespace, f"pod/{pod}") as (server_http, server_bolt):
                    eventually(lambda: self.count(server_http), 8)
                    assert self.query(
                        server_http, "MATCH (n:Smoke) RETURN n.id AS id ORDER BY id"
                    ) == [{"id": i} for i in range(8)]
                    self.verify_bolt(server_bolt, 8)
                    self.verify_ana(server_http, self.session(server_http), 8)
                    self.verify_metrics(server_http)
            sessions = {}
            for _ in range(16):
                headers = self.session(http)
                route = headers["X-ANA-Session"].split(".", 1)[0]
                sessions[route] = headers
                if sessions.keys() == servers.keys():
                    break
            assert sessions.keys() == servers.keys(), sessions.keys()
            for headers in sessions.values():
                self.verify_ana(http, headers, 8)
            self.verify_bolt(bolt, 8)
            self.tls_edge(set(servers), 8)
            lost_session = next(sessions[pod] for pod, node in servers.items() if node == lost_node)
            run("docker", "stop", lost_node)
            try:
                eventually(
                    lambda: self.ana(http, lost_session, "fetch", {"from": "h1"}).status_code,
                    404,
                )
                self.verify_ana(http, self.session(http), 8)
                write(8)
            finally:
                run("docker", "start", lost_node)
            eventually(lambda: self.count(http), 9)
        self.helm("rollback", "grapharrow", "2")
        with forward(self.namespace, "service/grapharrow") as (http, bolt):
            assert self.count(http) == 9
            self.verify_bolt(bolt, 9)
        print(
            "Delta scale 1→2→4, concurrent writes, ANA affinity, node loss and rollback passed",
            flush=True,
        )
        self.check_autoscaling(options, 9)
        self.verify_worker_drain("grapharrow", 4, 9, writable=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("image", help="Locally loaded GraphArrow image with an explicit tag")
    parser.add_argument("--chart", type=Path, default=CHART, help="Chart directory or package")
    args = parser.parse_args()
    namespace = "smoke-" + secrets.token_hex(4)
    kubectl("create", "namespace", namespace)
    kubectl("label", "namespace", namespace, "pod-security.kubernetes.io/enforce=restricted")
    # Azurite is a test dependency; its disposable namespace uses its upstream security policy.
    delta_namespace = namespace + "-delta"
    kubectl("create", "namespace", delta_namespace)
    try:
        with TemporaryDirectory(prefix="grapharrow-helm-") as temporary:
            for name, scenario in (
                (namespace, Deployment.native),
                (delta_namespace, Deployment.delta),
            ):
                test = Deployment(name, args.image, Path(temporary), args.chart)
                test.setup_auth()
                scenario(test)
    except BaseException:
        for name in (namespace, delta_namespace):
            print(kubectl("-n", name, "get", "pods,pvc,services", "-o", "wide"), flush=True)
            print(kubectl("-n", name, "get", "events", "--sort-by=.lastTimestamp"), flush=True)
        raise
    finally:
        kubectl("delete", "namespace", namespace, delta_namespace, "--wait=false")


if __name__ == "__main__":
    main()
