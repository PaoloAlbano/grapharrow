"""Compare composable Cypher scans in fresh processes on increasing graph sizes."""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import os
import platform
import resource
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

QUERIES = {
    "selective_projection": (
        "MATCH (p:Person) WHERE p.age>=$low AND p.age<$high "
        "RETURN coalesce(p.name,'?') AS name,coalesce(p.age,0) AS age ORDER BY name LIMIT 100"
    ),
    "with_filter": (
        "MATCH (p:Person) WITH p.name AS name,p.age AS age "
        "WITH name,coalesce(age,0) AS age WHERE age>=$low AND age<$high "
        "RETURN name,age ORDER BY name LIMIT 100"
    ),
    "case_group": (
        "MATCH (p:Person) WHERE p.age>=$low "
        "WITH coalesce(p.country,'unknown') AS country,"
        "CASE WHEN p.age<995 THEN 'lower' ELSE 'upper' END AS band "
        "WITH country,band,count(*) AS n RETURN country,band,n ORDER BY country,band"
    ),
    "broad_projection": (
        "MATCH (p:Person) RETURN coalesce(p.name,'?') AS name,"
        "CASE WHEN p.age>=500 THEN 'older' ELSE 'younger' END AS band "
        "ORDER BY name LIMIT 100"
    ),
    "correlated_control": (
        "MATCH (p:Person) WHERE p.age>=999 CALL (p) { "
        "MATCH (p)-[:KNOWS]->(q) RETURN count(*) AS n } "
        "RETURN p.name AS name,n ORDER BY name"
    ),
}
LEGACY = (
    "MATCH (p:Person) WHERE p.age>=$low AND p.age<$high "
    "RETURN p.name AS name,p.age AS age ORDER BY name LIMIT 100"
)

LIMIT_QUERIES = {
    "unordered_head": "MATCH (p:Person) RETURN coalesce(p.payload,'?') AS payload LIMIT 10",
    "filtered_head": (
        "MATCH (p:Person) WHERE p.age>=500 AND p.country IS NOT NULL "
        "RETURN coalesce(p.name,'?') AS name SKIP 50 LIMIT 10"
    ),
    "payload_top_k": (
        "MATCH (p:Person) RETURN coalesce(p.payload,'?') AS payload "
        "ORDER BY p.age DESC,p.name ASC SKIP 10 LIMIT 10"
    ),
    "mixed_order_with": (
        "MATCH (p:Person) WITH coalesce(p.country,'?') AS country,p.name AS name,p.age AS age "
        "WITH country,name,age ORDER BY country DESC,age ASC,name DESC SKIP 10 LIMIT 10 "
        "RETURN country,name,age"
    ),
    "native_top_k": (
        "MATCH (p:Person) RETURN p.name AS name,p.age AS age "
        "ORDER BY age DESC,name ASC SKIP 10 LIMIT 10"
    ),
    "large_window_control": (
        "MATCH (p:Person) RETURN coalesce(p.name,'?') AS name ORDER BY name LIMIT 50000"
    ),
}


def child(args: argparse.Namespace) -> None:
    if args.source:
        sys.path.insert(0, str(args.source / "src"))
    import pyarrow as pa

    from grapharrow.app import build_application
    from grapharrow.config import GraphArrowSettings

    os.environ["GRAPHARROW_DISABLE_CYPHER_COLUMNAR"] = (
        "1" if args.mode in {"row", "row-limits-off", "row-limits-on"} else "0"
    )
    os.environ["GRAPHARROW_DISABLE_CYPHER_LIMIT_OPTIMIZATIONS"] = (
        "1" if args.mode in {"limits-off", "row-limits-off"} else "0"
    )
    app = build_application(
        args.database,
        settings=GraphArrowSettings(
            parquet_filter_pushdown_enabled=args.mode != "columnar-no-reader-filter"
        ),
    )
    queries = LIMIT_QUERIES if args.suite == "limits" else QUERIES
    query = LEGACY if args.mode == "legacy" else queries[args.workload]
    scans: list[dict[str, Any]] = []
    load = app.use_cases.graph.load_node_property_table

    def scan(**kwargs: Any) -> Any:
        result = load(**kwargs)
        if not scans:
            scans.append(
                {
                    "columns": kwargs.get("columns"),
                    "predicates": [
                        {"column": p.column, "operator": p.operator, "value": p.value}
                        for p in kwargs.get("predicates", ())
                    ],
                    "returned_rows": result.num_rows,
                }
            )
        return result

    app.use_cases.graph.load_node_property_table = scan
    batches = getattr(app.use_cases.graph, "iter_node_property_batches", None)
    streamed = {"batches": 0, "rows": 0}
    if batches is not None:

        def stream(**kwargs: Any) -> Any:
            iterator = batches(**kwargs)
            try:
                for batch in iterator:
                    streamed["batches"] += 1
                    streamed["rows"] += batch.num_rows
                    yield batch
            finally:
                iterator.close()

        app.use_cases.graph.iter_node_property_batches = stream
    rss_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    samples, digests = [], []
    returned = 0
    for _ in range(args.repeats + 1):
        gc.collect()
        started = time.perf_counter()
        result = app.use_cases.cypher.execute(query, {"low": 990, "high": 995})
        samples.append(time.perf_counter() - started)
        encoded = json.dumps({"columns": result.columns, "rows": result.rows}, sort_keys=True)
        digests.append(hashlib.sha256(encoded.encode()).hexdigest())
        returned = len(result.rows)
        del result
    assert len(set(digests)) == 1
    divisor = 1024**2 if sys.platform == "darwin" else 1024
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(
        json.dumps(
            {
                "mode": args.mode,
                "workload": args.workload,
                "first_seconds": samples[0],
                "warm_seconds": samples[1:],
                "median_seconds": statistics.median(samples[1:]),
                "peak_rss_mib": peak / divisor,
                "rss_increase_mib": (peak - rss_before) / divisor,
                "arrow_peak_mib": pa.default_memory_pool().max_memory() / 1024**2,
                "returned_rows": returned,
                "digest": digests[0],
                "first_scan": scans,
                "streamed": streamed,
                "python": platform.python_version(),
                "pyarrow": pa.__version__,
            }
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", default="1000,10000,100000")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--baseline-source", type=Path)
    parser.add_argument("--suite", choices=("columnar", "limits"), default="columnar")
    parser.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--source", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--database", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--mode", help=argparse.SUPPRESS)
    parser.add_argument("--workload", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("--repeats must be positive")
    if args.child:
        child(args)
        return
    import pyarrow as pa

    from grapharrow.app import build_application

    sizes = [int(size) for size in args.sizes.split(",")]
    if not sizes or any(size < 1000 for size in sizes):
        parser.error("Sizes must be at least 1000 nodes")
    args.directory.mkdir(parents=True, exist_ok=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {
        "platform": platform.platform(),
        "repeats": args.repeats,
        "fixture": {"edges_per_node": 2, "payload_characters": 256, "age_modulus": 1000},
        "queries": LIMIT_QUERIES if args.suite == "limits" else QUERIES,
        "results": [],
        "method": (
            "Fresh process per mode/query/size; first call and warm repetitions; "
            "OS caches uncontrolled. RSS is process high-water mark. No network transport."
        ),
    }
    for size in sizes:
        database = args.directory / f"nodes-{size}"
        if database.exists():
            parser.error(f"Refusing to overwrite an existing database: {database}")
        app = build_application(database)
        _, nodes = app.use_cases.graph.create_nodes_bulk(
            table=pa.table(
                {
                    "name": [f"person-{i:09d}" for i in range(size)],
                    "age": [i % 1000 for i in range(size)],
                    "country": [
                        None if i % 7 == 0 else ("IT", "FR", "DE")[i % 3] for i in range(size)
                    ],
                    "payload": ["x" * 248 + f"{i % 100:08d}" for i in range(size)],
                }
            ),
            default_labels=("Person",),
        )
        ids = [node.node_id for node in nodes]
        app.use_cases.graph.create_edges_bulk(
            table=pa.table(
                {
                    "src_id": ids * 2,
                    "dst_id": ids[1:] + ids[:1] + ids[2:] + ids[:2],
                    "rel_type": ["KNOWS"] * (size * 2),
                }
            )
        )
        del app, nodes, ids
        gc.collect()
        for workload in report["queries"]:
            modes = (
                ["limits-off", "limits-on"]
                if args.suite == "limits"
                else ["row", "columnar", "columnar-no-reader-filter"]
            )
            if args.baseline_source:
                modes.insert(0, "before")
            if workload == "selective_projection":
                modes.append("legacy")
            digest = None
            for mode in modes:
                command = [
                    sys.executable,
                    str(Path(__file__).absolute()),
                    "--child",
                    "--suite",
                    args.suite,
                    "--directory",
                    str(args.directory),
                    "--output",
                    str(args.output),
                    "--database",
                    str(database),
                    "--workload",
                    workload,
                    "--mode",
                    mode,
                    "--repeats",
                    str(args.repeats),
                ]
                if mode == "before":
                    command.extend(["--source", str(args.baseline_source.absolute())])
                result = subprocess.run(command, text=True, capture_output=True, check=True)
                measured = json.loads(result.stdout)
                measured["nodes"] = size
                if digest is None:
                    digest = measured["digest"]
                assert measured["digest"] == digest, (size, workload, mode)
                report["results"].append(measured)
                args.output.write_text(json.dumps(report, indent=2) + "\n")
                print(
                    f"{size:>7} {workload:>22} {mode:>26}: "
                    f"{measured['median_seconds'] * 1000:8.2f} ms, "
                    f"{measured['peak_rss_mib']:7.1f} MiB",
                    flush=True,
                )


if __name__ == "__main__":
    main()
