"""Measure ANA session latency, current RSS and retained resource accounting."""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import os
import platform
import statistics
import subprocess
import sys
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pyarrow as pa

from grapharrow.app import build_application
from grapharrow.use_cases.ana import GraphArrowAgentBackend
from grapharrow.use_cases.ana_resources import AnaResourceBudget


def rss_bytes() -> int:
    """Sample current resident memory, independently of Python's allocator."""
    statm = Path("/proc/self/statm")
    if statm.exists():
        return int(statm.read_text().split()[1]) * os.sysconf("SC_PAGE_SIZE")
    return int(subprocess.check_output(["ps", "-o", "rss=", "-p", str(os.getpid())])) * 1024


def worker(database: Path, sessions: int) -> dict[str, Any]:
    application = build_application(database)
    backends = []
    budget = AnaResourceBudget(application.settings.ana_max_total_bytes)
    creates = []
    calls: dict[str, list[float]] = {}
    gc.collect()
    before = rss_bytes()
    for _ in range(sessions):
        start = time.perf_counter()
        backends.append(GraphArrowAgentBackend(application, resource_budget=budget))
        creates.append((time.perf_counter() - start) * 1000)
    opened = rss_bytes()
    for backend in backends:

        def call(tool: str, args: dict[str, Any], backend: Any = backend) -> dict[str, Any]:
            start = time.perf_counter()
            result = backend.call(tool, args)
            calls.setdefault(tool, []).append((time.perf_counter() - start) * 1000)
            return result

        scanned = call("node_scan", {"label": "Device", "limit": 1000})
        selected = call("node_search", {"label": "Device", "value": "device-000042"})
        expanded = call(
            "expand",
            {
                "from": selected["handle"],
                "source": "device",
                "relationship_type": "LINK",
                "direction": "out",
                "target_label": "Device",
                "as": "neighbor",
            },
        )
        projected = call(
            "project",
            {
                "from": scanned["handle"],
                "select": [{"var": "device", "property": "name", "alias": "name"}],
                "order_by": [{"field": "name"}],
            },
        )
        page = call("fetch", {"from": projected["handle"], "limit": 100})
        count = call(
            "count_handle", {"from": expanded["handle"], "var": "neighbor", "distinct": True}
        )
        assert call("fetch", {"from": count["handle"]})["rows"] == [[3]]
        assert page["returned_count"] == 100
    used = rss_bytes()
    accounting = [getattr(backend, "resource_usage", None) for backend in backends]
    for backend in backends:
        backend.close()
    gc.collect()
    closed = rss_bytes()
    return {
        "sessions": sessions,
        "create_ms": creates,
        "call_ms": calls,
        "rss_bytes": {"before": before, "open": opened, "used": used, "closed": closed},
        "accounting": accounting,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodes", type=int, default=5000)
    parser.add_argument("--sessions", type=int, nargs="+", default=[1, 4])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--label", default="bounded")
    parser.add_argument("--output", type=Path, default=Path("docs/benchmarks/ana-sessions.json"))
    parser.add_argument("--worker-db", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--worker-sessions", type=int, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker_db:
        print(json.dumps(worker(args.worker_db, args.worker_sessions)))
        return
    if args.nodes < 1000 or args.repeats < 1 or any(count < 1 for count in args.sessions):
        parser.error("Use at least 1000 nodes and positive session/repeat counts")
    runs = []
    with TemporaryDirectory(prefix="grapharrow-ana-benchmark-") as temporary:
        database = Path(temporary) / "db"
        application = build_application(database)
        _, nodes = application.use_cases.graph.create_nodes_bulk(
            table=pa.Table.from_pylist(
                [
                    {"name": f"device-{index:06d}", "value": index, "description": "x" * 160}
                    for index in range(args.nodes)
                ]
            ),
            default_labels=("Device",),
        )
        application.use_cases.graph.create_edges_bulk(
            table=pa.Table.from_pylist(
                [
                    {
                        "src_id": node.node_id,
                        "dst_id": nodes[(index + offset) % len(nodes)].node_id,
                        "rel_type": "LINK",
                    }
                    for index, node in enumerate(nodes)
                    for offset in (1, 2, 3)
                ]
            )
        )
        for sessions in args.sessions:
            for _ in range(args.repeats):
                result = subprocess.run(
                    [
                        sys.executable,
                        str(Path(__file__).resolve()),
                        "--worker-db",
                        str(database),
                        "--worker-sessions",
                        str(sessions),
                    ],
                    capture_output=True,
                    text=True,
                )
                if result.returncode:
                    raise RuntimeError(result.stderr)
                runs.append(json.loads(result.stdout))
    sources = sorted(
        (Path(__file__).resolve().parents[1] / "src/grapharrow/use_cases").glob("ana*.py")
    )
    digest = hashlib.sha256(b"".join(path.read_bytes() for path in sources)).hexdigest()
    result = {
        "label": args.label,
        "python": platform.python_version(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "ana_source_sha256": digest,
        "workload": {
            "nodes": args.nodes,
            "edges": args.nodes * 3,
            "repeats": args.repeats,
            "session_counts": args.sessions,
        },
        "runs": runs,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    for count in args.sessions:
        group = [run for run in runs if run["sessions"] == count]
        times = [ms for run in group for ms in run["create_ms"]]
        retained = [run["rss_bytes"]["used"] - run["rss_bytes"]["before"] for run in group]
        print(
            f"{count} sessions: create median {statistics.median(times):.2f} ms; "
            f"RSS delta median {statistics.median(retained) / 2**20:.2f} MiB"
        )
    print(f"Results: {args.output}")


if __name__ == "__main__":
    main()
