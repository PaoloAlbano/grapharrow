"""Drain and remove the highest-ordinal GraphArrow pod through its Helm gateway."""

from __future__ import annotations

import argparse
import json
import signal
import subprocess
import time
from contextlib import ExitStack
from typing import Any

_SOCKET = "UNIX-CONNECT:/run/haproxy/admin.sock"


class ScaleDown:
    def __init__(self, release: str, namespace: str, context: str | None) -> None:
        self.release = release
        self.kubectl_prefix = ["kubectl", "--namespace", namespace]
        if context:
            self.kubectl_prefix.extend(("--context", context))

    def kubectl(self, *args: str, stdin: str | None = None, timeout: int = 30) -> str:
        return subprocess.check_output(
            [*self.kubectl_prefix, *args], text=True, input=stdin, timeout=timeout
        ).strip()

    def resource(self, kind: str, name: str) -> dict[str, Any]:
        result = self.kubectl("get", kind, name, "--ignore-not-found", "-o", "json")
        return json.loads(result) if result else {}

    def gateways(self) -> dict[str, tuple[str, str]]:
        name = f"{self.release}-gateway"
        spec = self.resource("deployment", name)
        selector = f"app.kubernetes.io/instance={self.release},app.kubernetes.io/component=gateway"
        pods = json.loads(self.kubectl("get", "pods", "-l", selector, "-o", "json"))["items"]
        ready = {
            pod["metadata"]["name"]: (pod["metadata"]["uid"], container["containerID"])
            for pod in pods
            if "deletionTimestamp" not in pod["metadata"]
            for container in pod["status"].get("containerStatuses", ())
            if container["name"] == "haproxy" and container["ready"]
        }
        if not ready or len(ready) != len(pods) or len(ready) != spec["spec"]["replicas"]:
            raise RuntimeError("Wait for the gateway rollout to finish before scaling down.")
        return ready

    def runtime(self, pod: str, command: str) -> str:
        return self.kubectl(
            "exec",
            pod,
            "-c",
            "haproxy",
            "-i",
            "--",
            "socat",
            "-",
            _SOCKET,
            stdin=command + "\n",
        )

    def get_weight(self, pod: str, server: str) -> int:
        return int(self.runtime(pod, f"get weight {server}").split()[0])

    def set_weight(self, pod: str, server: str, weight: int) -> None:
        error = self.runtime(pod, f"set server {server} weight {weight}")
        if error:
            raise RuntimeError(f"{pod}: {error}")

    def restore_weight(self, pod: str, server: str, weight: int) -> None:
        # A removed gateway has already discarded the runtime change.
        if self.resource("pod", pod):
            self.set_weight(pod, server, weight)

    def run(self, grace_seconds: int) -> None:
        lease = f"{self.release}-drain"
        claim = json.dumps(
            {"apiVersion": "coordination.k8s.io/v1", "kind": "Lease", "metadata": {"name": lease}}
        )
        with ExitStack() as undo:
            self.kubectl("create", "-f", "-", stdin=claim)
            undo.callback(self.kubectl, "delete", "lease", lease, "--ignore-not-found")
            target, replicas = self.drain(grace_seconds, undo)
        print(f"Removed {target}; {replicas} replicas requested.", flush=True)

    def drain(self, grace_seconds: int, undo: ExitStack) -> tuple[str, int]:
        workload = f"statefulset/{self.release}"
        spec = self.resource("statefulset", self.release).get("spec")
        if spec is None:
            raise RuntimeError(f"StatefulSet {self.release} does not exist.")
        replicas = spec["replicas"]
        hpa = self.resource("hpa", self.release).get("spec", {})
        minimum = hpa.get("minReplicas", 1)
        if replicas <= minimum:
            raise RuntimeError(f"Scaling down would go below the minimum of {minimum} replicas.")
        ordinal = spec.get("ordinals", {}).get("start", 0) + replicas - 1
        target = f"{self.release}-{ordinal}"
        uid = self.resource("pod", target).get("metadata", {}).get("uid")
        if uid is None:
            raise RuntimeError(f"Wait for {target} to exist before scaling down.")
        gateways = self.gateways()
        for pod in gateways:
            for pool in ("http_pool", "bolt"):
                server = f"{pool}/app_{ordinal}"
                undo.callback(self.restore_weight, pod, server, self.get_weight(pod, server))
                self.set_weight(pod, server, 0)
        print(f"Draining {target} for {grace_seconds}s", flush=True)
        deadline = time.monotonic() + grace_seconds
        while True:
            if self.gateways() != gateways:
                raise RuntimeError("A gateway changed during the drain. Retry after it is ready.")
            if self.resource("pod", target).get("metadata", {}).get("uid") != uid:
                raise RuntimeError("The target pod was replaced during the drain.")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(remaining, 1))
        self.kubectl(
            "scale", workload, f"--current-replicas={replicas}", f"--replicas={replicas - 1}"
        )
        self.kubectl("wait", "--for=delete", f"pod/{target}", "--timeout=90s", timeout=95)
        return target, replicas - 1


def stop(signum: int, _: object) -> None:
    raise SystemExit(128 + signum)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("release", help="Helm release with a GraphArrow gateway")
    parser.add_argument("--namespace", default="grapharrow")
    parser.add_argument("--context")
    parser.add_argument("--grace-seconds", type=int, default=60)
    args = parser.parse_args()
    if args.grace_seconds < 0:
        parser.error("--grace-seconds must be nonnegative")
    signal.signal(signal.SIGTERM, stop)
    ScaleDown(args.release, args.namespace, args.context).run(args.grace_seconds)
