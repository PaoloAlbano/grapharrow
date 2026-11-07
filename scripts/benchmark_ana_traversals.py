"""Measure concurrent ANA traversals and sample worker RSS after session setup."""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import math
import os
import platform
import resource
import statistics
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from pathlib import Path
from threading import Barrier
from typing import Any

import pyarrow as pa

from grapharrow.app import build_application
from grapharrow.config import GraphArrowSettings
from grapharrow.use_cases.ana import GraphArrowAgentBackend
from grapharrow.use_cases.ana_resources import AnaResourceBudget

CASES = {
    "hub_one_limit_1": ("Hub", 1, 1, "expand"),
    "hub_eight_limit_10": ("Hub", 8, 10, "expand"),
    "hub_eight_limit_100": ("Hub", 8, 100, "expand"),
    "low_degree_250": ("Seed", 250, 1000, "expand"),
    "hub_pattern_limit_10": ("Hub", 1, 10, "multi_hop_query"),
    "hub_two_hop_limit_10": ("Hub", 1, 10, "two_hop"),
}


def build_fixture(path: Path, degree: int) -> None:
    """Create hubs with parallel edges, plus a low-degree control on the same snapshot."""
    app = build_application(path)
    if app.layout.current_snapshot_path.exists():
        raise ValueError("Use a new fixture path to avoid overwriting an existing database")
    _, hubs = app.use_cases.graph.create_nodes_bulk(
        table=pa.table({"name": [f"hub-{i}" for i in range(8)]}), default_labels=("Hub",)
    )
    _, targets = app.use_cases.graph.create_nodes_bulk(
        table=pa.table({"name": [f"target-{i:05d}" for i in range(degree // 2)]}),
        default_labels=("Target",),
    )
    _, seeds = app.use_cases.graph.create_nodes_bulk(
        table=pa.table({"name": [f"seed-{i:03d}" for i in range(250)]}),
        default_labels=("Seed",),
    )
    sources = [hub.node_id for hub in hubs for _ in range(degree)]
    destinations = [targets[i % len(targets)].node_id for _ in hubs for i in range(degree)]
    for i, seed in enumerate(seeds):
        sources.extend([seed.node_id] * 3)
        destinations.extend(targets[(i + offset) % len(targets)].node_id for offset in range(3))
    app.use_cases.graph.create_edges_bulk(
        table=pa.table(
            {"src_id": sources, "dst_id": destinations, "rel_type": ["LINK"] * len(sources)}
        )
    )


def max_rss_bytes() -> int:
    """Return process lifetime high-water RSS in bytes on macOS or Linux."""
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(value if sys.platform == "darwin" else value * 1024)


def rss_bytes(pid: int) -> int:
    """Sample current RSS in a separate process from the timed workload."""
    statm = Path(f"/proc/{pid}/statm")
    try:
        if statm.exists():
            return int(statm.read_text().split()[1]) * os.sysconf("SC_PAGE_SIZE")
        return (
            int(
                subprocess.check_output(
                    ["ps", "-o", "rss=", "-p", str(pid)], stderr=subprocess.DEVNULL, text=True
                )
            )
            * 1024
        )
    except (OSError, subprocess.CalledProcessError, ValueError):
        return 0


def worker(args: argparse.Namespace) -> None:
    """Open independent sessions, then synchronize calls after the parent's RSS sample."""
    app = build_application(args.database, settings=GraphArrowSettings(_env_file=None))
    budget = AnaResourceBudget(app.settings.ana_max_total_bytes)
    label, seed_count, limit, kind = CASES[args.worker_case]
    tool = "multi_hop_query" if kind == "two_hop" else kind
    barrier = Barrier(args.worker_sessions)
    with ExitStack() as stack:
        backends = [
            stack.enter_context(GraphArrowAgentBackend(app, resource_budget=budget))
            for _ in range(args.worker_sessions)
        ]
        handles = [
            backend.call("node_scan", {"label": label, "limit": seed_count})["handle"]
            for backend in backends
        ]
        assert len({backend._owner for backend in backends}) == len(backends)
        gc.collect()
        setup_high_water = max_rss_bytes()
        print(
            json.dumps({"ready": True, "usage": [b.resource_usage for b in backends]}), flush=True
        )
        if sys.stdin.readline().strip() != "go":
            raise RuntimeError("Expected the parent to start the measurement")

        def execute(index: int) -> dict[str, Any]:
            backend = backends[index]
            durations = []
            signatures = set()
            expected_rows = min(limit, seed_count * (args.degree if label == "Hub" else 3))
            expected_distinct = args.degree // 2 if label == "Hub" else min(args.degree // 2, 252)
            if kind == "two_hop":
                expected_distinct = 8
            for iteration in range(args.warmups + args.repeats):
                call_args: dict[str, Any] = {
                    "from": handles[index],
                    "limit": limit,
                }
                if tool == "expand":
                    call_args.update(
                        source=label.lower(),
                        relationship_type="LINK",
                        direction="out",
                        target_label="Target",
                        **{"as": "target"},
                    )
                else:
                    call_args.update(
                        start_label=label,
                        start_as=label.lower(),
                        hops=[
                            {
                                "relationship_type": "LINK",
                                "target_label": "Target",
                                "as": "target",
                            }
                        ],
                    )
                    if kind == "two_hop":
                        call_args["hops"][0]["as"] = "middle"
                        call_args["hops"].append(
                            {
                                "relationship_type": "LINK",
                                "target_label": "Hub",
                                "as": "target",
                                "direction": "in",
                            }
                        )
                barrier.wait(timeout=60)
                start = time.perf_counter_ns()
                result = backend.call(tool, call_args)
                elapsed = (time.perf_counter_ns() - start) / 1e6
                if iteration >= args.warmups:
                    durations.append(elapsed)
                assert result["matched_count"] == expected_rows
                handle = backend._handle_store.handles[result["handle"]]
                assert len(handle.metadata["native_entity_query"]["node_ids"]) == expected_distinct
                assert all(row["target"].reference.owner == backend._owner for row in handle.rows)
                page = backend.call("fetch", {"from": result["handle"], "limit": 1000})
                signature = [
                    result["matched_count"],
                    page["rows"],
                    {
                        key: result[key]
                        for key in ("pages", "limited", "input_count")
                        if key in result
                    },
                ]
                signatures.add(
                    hashlib.sha256(json.dumps(signature, sort_keys=True).encode()).hexdigest()
                )
            assert len(signatures) == 1
            return {
                "latency_ms": durations,
                "result_sha256": signatures.pop(),
                "usage": backend.resource_usage,
            }

        with ThreadPoolExecutor(max_workers=args.worker_sessions) as pool:
            started = time.perf_counter()
            results = list(pool.map(execute, range(args.worker_sessions)))
            wall_ms = (time.perf_counter() - started) * 1000
        high_water = max_rss_bytes()
    assert budget.used_bytes == 0
    print(
        json.dumps(
            {
                "sessions": results,
                "wall_ms": wall_ms,
                "setup_high_water_rss_bytes": setup_high_water,
                "high_water_rss_bytes": high_water,
                "budget_after_close_bytes": budget.used_bytes,
            }
        ),
        flush=True,
    )


def measure(args: argparse.Namespace, case: str, sessions: int) -> dict[str, Any]:
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--database",
        str(args.database),
        "--degree",
        str(args.degree),
        "--repeats",
        str(args.repeats),
        "--warmups",
        str(args.warmups),
        "--worker-case",
        case,
        "--worker-sessions",
        str(sessions),
    ]
    with subprocess.Popen(
        command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    ) as process:
        assert process.stdout is not None and process.stdin is not None
        ready = process.stdout.readline()
        if not ready:
            _, errors = process.communicate()
            raise RuntimeError(errors)
        setup = json.loads(ready)
        baseline = rss_bytes(process.pid)
        if not baseline:
            process.terminate()
            raise RuntimeError("Cannot sample worker RSS. Check process-inspection permissions.")
        samples = [baseline]
        sampled_at = []
        process.stdin.write("go\n")
        process.stdin.flush()
        with ThreadPoolExecutor(max_workers=1) as reader:
            output = reader.submit(process.communicate)
            while not output.done():
                sampled_at.append(time.perf_counter())
                samples.append(rss_bytes(process.pid))
                time.sleep(0.01)
            stdout, stderr = output.result()
        if process.returncode:
            raise RuntimeError(stderr)
    result = json.loads(stdout)
    values = sorted(value for session in result["sessions"] for value in session["latency_ms"])
    intervals = [
        (right - left) * 1000 for left, right in zip(sampled_at, sampled_at[1:], strict=False)
    ]
    result.update(
        case=case,
        concurrency=sessions,
        setup_usage=setup["usage"],
        rss_baseline_bytes=baseline,
        sampled_peak_rss_bytes=max(samples),
        sampled_rss_growth_bytes=max(samples) - baseline,
        rss_sample_count=len(samples),
        rss_sample_interval_median_ms=statistics.median(intervals) if intervals else None,
        latency_median_ms=statistics.median(values),
        latency_p95_ms=values[math.ceil(len(values) * 0.95) - 1],
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--degree", type=int, default=20000)
    parser.add_argument("--sessions", nargs="+", type=int, default=[1, 4])
    parser.add_argument("--cases", nargs="+", choices=list(CASES), default=list(CASES))
    parser.add_argument("--repeats", type=int, default=21)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--build-fixture", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--worker-case", choices=list(CASES), help=argparse.SUPPRESS)
    parser.add_argument("--worker-sessions", type=int, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.degree < 1000 or args.degree % 2 or args.repeats < 1 or args.warmups < 0:
        parser.error("Use an even degree of at least 1000 and positive repeats")
    if any(count < 1 for count in args.sessions):
        parser.error("Session counts must be positive")
    if args.worker_case:
        worker(args)
        return
    if args.output is None:
        parser.error("--output is required for benchmark runs")
    if args.build_fixture:
        build_fixture(args.database, args.degree)
    report = {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "pyarrow": pa.__version__,
        "degree": args.degree,
        "repeats": args.repeats,
        "warmups": args.warmups,
        "runs": [],
    }
    module = sys.modules[GraphArrowAgentBackend.__module__]
    sources = sorted(Path(module.__file__).resolve().parent.glob("ana*.py"))
    report["ana_source_sha256"] = hashlib.sha256(
        b"".join(path.read_bytes() for path in sources)
    ).hexdigest()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    for case in args.cases:
        for sessions in args.sessions:
            result = measure(args, case, sessions)
            report["runs"].append(result)
            args.output.write_text(json.dumps(report, indent=2) + "\n")
            print(
                f"{case}, {sessions} sessions: median {result['latency_median_ms']:.2f} ms, "
                f"p95 {result['latency_p95_ms']:.2f} ms, "
                f"sampled RSS growth {result['sampled_rss_growth_bytes'] / 1048576:.1f} MiB",
                flush=True,
            )


if __name__ == "__main__":
    main()
