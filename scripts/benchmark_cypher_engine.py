"""Benchmark representative GraphArrow Cypher query paths."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import resource
import shutil
import statistics
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import numpy as np
import pyarrow as pa  # type: ignore[import-untyped]

from grapharrow import Database

DISABLE_PLANNER_ENV = "GRAPHARROW_DISABLE_CYPHER_PLANNER"
COUNTRIES = np.array(["IT", "DE", "FR", "US", "UK", "ES"], dtype=object)


@dataclass(frozen=True, slots=True)
class BenchmarkResult:
    query: str
    median_seconds: float
    best_seconds: float
    worst_seconds: float
    runs: tuple[float, ...]
    row_count: int
    checksum: str
    median_rss_delta_mb: float
    best_rss_delta_mb: float
    worst_rss_delta_mb: float
    rss_delta_runs: tuple[float, ...]
    median_arrow_peak_mb: float
    best_arrow_peak_mb: float
    worst_arrow_peak_mb: float
    arrow_peak_runs: tuple[float, ...]

def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark GraphArrow Cypher execution.")
    parser.add_argument("--nodes", type=int, default=20_000, help="Number of synthetic nodes.")
    parser.add_argument("--edges", type=int, default=120_000, help="Number of synthetic edges.")
    parser.add_argument("--repeats", type=int, default=5, help="Measured runs per query.")
    parser.add_argument("--warmups", type=int, default=1, help="Warmup runs per query.")
    parser.add_argument(
        "--memory-repeats",
        type=int,
        default=3,
        help="Dedicated memory probe runs per query.",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed for synthetic data.")
    parser.add_argument(
        "--workspace",
        type=Path,
        default=Path(".benchmarks") / "cypher_engine",
        help="Workspace directory for the generated benchmark database.",
    )
    parser.add_argument(
        "--json-out",
        type=Path,
        default=Path("docs") / "benchmarks" / "cypher-engine-benchmark.json",
        help="JSON output path for benchmark results.",
    )
    parser.add_argument(
        "--markdown-out",
        type=Path,
        default=Path("docs") / "benchmarks" / "cypher-engine-benchmark.md",
        help="Markdown output path for benchmark results.",
    )
    parser.add_argument(
        "--force-rebuild",
        action="store_true",
        help="Rebuild the synthetic benchmark database even if it already exists.",
    )
    parser.add_argument(
        "--disable-planner",
        action="store_true",
        help="Disable the Cypher logical planner and force legacy execution paths.",
    )
    parser.add_argument(
        "--child-query",
        type=str,
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--database-path",
        type=Path,
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--child-params-json",
        type=str,
        default=None,
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args()
    if args.disable_planner:
        os.environ[DISABLE_PLANNER_ENV] = "1"
    else:
        os.environ.pop(DISABLE_PLANNER_ENV, None)
    queries = build_queries()
    if args.child_query is not None:
        if args.database_path is None:
            raise SystemExit("--database-path is required in child mode.")
        child_params = (
            cast_params(json.loads(args.child_params_json))
            if args.child_params_json is not None
            else {}
        )
        payload = run_memory_probe(
            database_path=args.database_path,
            query=queries[args.child_query],
            params=child_params,
        )
        print(json.dumps(payload))
        return

    _log(
        "Starting Cypher engine benchmark "
        f"(nodes={args.nodes}, edges={args.edges}, repeats={args.repeats}, "
        f"warmups={args.warmups}, memory_repeats={args.memory_repeats})."
    )

    database = prepare_database(
        workspace=args.workspace,
        node_count=args.nodes,
        edge_count=args.edges,
        seed=args.seed,
        force_rebuild=args.force_rebuild,
    )

    query_params: dict[str, dict[str, object]] = {
        "unwind_parameter_pipeline": {
            "values": [
                f"Tag-{index:04d}"
                for index in range(500)
            ]
        }
    }

    results: list[BenchmarkResult] = []
    for name, query in queries.items():
        _log(f"Benchmarking query '{name}'.")
        result = measure_query(
            database=database,
            database_path=Path(database.path),
            disable_planner=args.disable_planner,
            name=name,
            query=query,
            params=query_params.get(name, {}),
            repeats=args.repeats,
            warmups=args.warmups,
            memory_repeats=args.memory_repeats,
        )
        results.append(result)
        _log(
            f"Completed query '{name}' "
            f"(median={result.median_seconds:.3f}s, "
            f"rss_delta={result.median_rss_delta_mb:.2f}MB, "
            f"arrow_peak={result.median_arrow_peak_mb:.2f}MB)."
        )

    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.markdown_out.parent.mkdir(parents=True, exist_ok=True)
    write_json_report(args.json_out, args, results)
    write_markdown_report(args.markdown_out, args, results)

    _log(f"Wrote JSON report to {args.json_out}")
    _log(f"Wrote Markdown report to {args.markdown_out}")
    print(f"Wrote JSON report to {args.json_out}")
    print(f"Wrote Markdown report to {args.markdown_out}")


def build_queries() -> dict[str, str]:
    return {
        "scalar_filter_sort_limit": (
            "MATCH (n:Person) "
            "WHERE n.country = 'IT' AND n.active = true "
            "RETURN n.external_id, n.score "
            "ORDER BY n.score DESC, n.external_id ASC "
            "LIMIT 100"
        ),
        "relationship_property_filter_sort_limit": (
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "WHERE r.weight >= 0.8 "
            "RETURN a.external_id, b.external_id, r.weight "
            "ORDER BY r.weight DESC, a.external_id ASC, b.external_id ASC "
            "LIMIT 500"
        ),
        "grouped_with_count": (
            "MATCH (n:Person) "
            "WITH n.country AS country, COUNT(*) AS total "
            "RETURN country, total "
            "ORDER BY total DESC, country ASC "
            "LIMIT 10"
        ),
        "return_distinct_projection_order_limit": (
            "MATCH (n:Person) "
            "RETURN DISTINCT n.score "
            "ORDER BY n.score DESC "
            "LIMIT 1000"
        ),
        "optional_match_projection": (
            "MATCH (a:Person) "
            "OPTIONAL MATCH (a)-[r:KNOWS]->(b:Person) "
            "RETURN a.external_id, type(r), b.external_id "
            "ORDER BY a.external_id ASC "
            "LIMIT 1000"
        ),
        "optional_match_return_distinct_projection": (
            "MATCH (a:Person) "
            "OPTIONAL MATCH (a)-[r:KNOWS]->(b:Person) "
            "RETURN DISTINCT a.external_id, type(r), b.external_id "
            "ORDER BY a.external_id ASC, type(r) ASC, b.external_id ASC "
            "LIMIT 1000"
        ),
        "three_hop_traversal": (
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person) "
            "WHERE a.anchor = true "
            "RETURN COUNT(*) AS total"
        ),
        "three_hop_projection_order_limit": (
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person) "
            "WHERE a.anchor = true "
            "RETURN a.external_id, d.external_id "
            "ORDER BY a.external_id ASC, d.external_id ASC "
            "LIMIT 500"
        ),
        "three_hop_traversal_dense_where": (
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person) "
            "WHERE a.anchor = true "
            "AND b.active = true "
            "AND c.country IN ['IT', 'DE', 'FR'] "
            "AND d.score >= 0.5 "
            "RETURN COUNT(*) AS total"
        ),
        "three_hop_traversal_mixed_where": (
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person) "
            "WHERE a.anchor = true "
            "AND b.active = true "
            "AND c.country IN ['IT', 'DE', 'FR'] "
            "AND d.score >= 0.5 "
            "AND d.external_id STARTS WITH 'person-0' "
            "RETURN COUNT(*) AS total"
        ),
        "multi_match_with_where": (
            "MATCH (a:Person) "
            "WHERE a.anchor = true "
            "MATCH (a)-[:KNOWS]->(b:Person) "
            "WHERE b.active = true "
            "MATCH (b)-[:KNOWS]->(c:Person) "
            "WHERE c.country IN ['IT', 'DE', 'FR'] "
            "MATCH (c)-[:KNOWS]->(d:Person) "
            "WHERE d.score >= 0.5 "
            "RETURN COUNT(*) AS total"
        ),
        "three_hop_wildcard_projection_order_limit": (
            "MATCH (a:Person)-[*3]->(d:Person) "
            "WHERE a.anchor = true "
            "RETURN a.external_id, d.external_id "
            "ORDER BY a.external_id ASC, d.external_id ASC "
            "LIMIT 500"
        ),
        "range_hop_wildcard_projection_order_limit": (
            "MATCH (a:Person)-[*1..3]->(d:Person) "
            "WHERE a.anchor = true "
            "RETURN DISTINCT a.external_id, d.external_id "
            "ORDER BY a.external_id ASC, d.external_id ASC "
            "LIMIT 500"
        ),
        "unwind_parameter_pipeline": (
            "UNWIND $values AS value "
            "RETURN toLower(value) "
            "ORDER BY toLower(value) ASC"
        ),
    }


def prepare_database(
    *,
    workspace: Path,
    node_count: int,
    edge_count: int,
    seed: int,
    force_rebuild: bool,
) -> Database:
    workspace.mkdir(parents=True, exist_ok=True)
    database_path = workspace / f"grapharrow-{node_count}-nodes-{edge_count}-edges-seed-{seed}"
    if force_rebuild and database_path.exists():
        _log(f"Force rebuild requested, removing existing benchmark database at {database_path}.")
        shutil.rmtree(database_path)

    database = Database(database_path)
    if database_path.exists() and (database_path / "current_snapshot.json").exists():
        if _database_matches_expected_shape(
            database,
            expected_nodes=node_count,
            expected_edges=edge_count,
        ):
            _log(f"Reusing existing benchmark database at {database_path}.")
            return database
        _log(f"Existing benchmark database at {database_path} has stale shape, rebuilding it.")
        shutil.rmtree(database_path)
        database = Database(database_path)

    _log(f"Generating synthetic dataset in {database_path}.")
    rng = np.random.default_rng(seed)
    node_ids = np.arange(1, node_count + 1, dtype=np.int64)
    anchor_count = min(500, node_count)

    node_table = pa.table(
        {
            "external_id": pa.array([f"person-{node_id:06d}" for node_id in node_ids]),
            "name": pa.array([f"Person {node_id:06d}" for node_id in node_ids]),
            "country": pa.array(rng.choice(COUNTRIES, size=node_count).tolist(), type=pa.string()),
            "active": pa.array((rng.random(node_count) > 0.25).tolist(), type=pa.bool_()),
            "score": pa.array(rng.random(node_count).tolist(), type=pa.float64()),
            "anchor": pa.array((node_ids <= anchor_count).tolist(), type=pa.bool_()),
        }
    )

    random_edges = edge_count - (anchor_count * 3)
    src_random = rng.integers(1, node_count + 1, size=max(random_edges, 0), dtype=np.int64)
    dst_random = rng.integers(1, node_count + 1, size=max(random_edges, 0), dtype=np.int64)

    controlled_sources = np.repeat(np.arange(1, anchor_count + 1, dtype=np.int64), 3)
    controlled_targets = np.concatenate(
        [
            controlled_sources + 1,
            controlled_sources + 2,
            controlled_sources + 3,
        ]
    )[: anchor_count * 3]
    controlled_targets = ((controlled_targets - 1) % node_count) + 1

    src_ids = np.concatenate([controlled_sources, src_random])
    dst_ids = np.concatenate([controlled_targets, dst_random])
    rel_types = np.array(["KNOWS"] * len(src_ids), dtype=object)
    weights = rng.random(len(src_ids)).tolist()
    edge_table = pa.table(
        {
            "src_id": pa.array(src_ids.tolist(), type=pa.int64()),
            "dst_id": pa.array(dst_ids.tolist(), type=pa.int64()),
            "rel_type": pa.array(rel_types.tolist(), type=pa.string()),
            "weight": pa.array(weights, type=pa.float64()),
        }
    )

    graph_use_case = database._app.use_cases.graph
    _log(f"Bulk ingesting {node_count} nodes.")
    graph_use_case.create_nodes_bulk(
        table=node_table,
        default_labels=("Person",),
    )
    _log(f"Bulk ingesting {edge_count} edges.")
    graph_use_case.create_edges_bulk(table=edge_table)

    _log(f"Finished building benchmark database at {database_path}.")
    return database


def _database_matches_expected_shape(
    database: Database,
    *,
    expected_nodes: int,
    expected_edges: int,
) -> bool:
    state = database._app.use_cases.graph.load_state()
    return len(state.nodes) == expected_nodes and len(state.edges) == expected_edges


def measure_query(
    *,
    database: Database,
    database_path: Path,
    disable_planner: bool,
    name: str,
    query: str,
    params: dict[str, object],
    repeats: int,
    warmups: int,
    memory_repeats: int,
) -> BenchmarkResult:
    _log(
        f"Running warmups for '{name}' "
        f"({warmups} warmups, {repeats} measured runs, {memory_repeats} memory probes)."
    )
    for _ in range(warmups):
        run_query(database, query, params)

    runs: list[float] = []
    final_row_count = -1
    final_checksum = ""
    for _ in range(repeats):
        started = time.perf_counter()
        row_count, checksum = run_query(database, query, params)
        ended = time.perf_counter()
        final_row_count = row_count
        final_checksum = checksum
        runs.append(ended - started)

    memory_runs = [
        run_memory_probe_subprocess(
            script_path=Path(__file__),
            database_path=database_path,
            disable_planner=disable_planner,
            query_name=name,
            params=params,
        )
        for _ in range(memory_repeats)
    ]
    rss_delta_runs = tuple(run["rss_delta_mb"] for run in memory_runs)
    arrow_peak_runs = tuple(run["arrow_peak_mb"] for run in memory_runs)

    return BenchmarkResult(
        query=name,
        median_seconds=statistics.median(runs),
        best_seconds=min(runs),
        worst_seconds=max(runs),
        runs=tuple(runs),
        row_count=final_row_count,
        checksum=final_checksum,
        median_rss_delta_mb=statistics.median(rss_delta_runs),
        best_rss_delta_mb=min(rss_delta_runs),
        worst_rss_delta_mb=max(rss_delta_runs),
        rss_delta_runs=rss_delta_runs,
        median_arrow_peak_mb=statistics.median(arrow_peak_runs),
        best_arrow_peak_mb=min(arrow_peak_runs),
        worst_arrow_peak_mb=max(arrow_peak_runs),
        arrow_peak_runs=arrow_peak_runs,
    )


def _log(message: str) -> None:
    timestamp = datetime.now(UTC).astimezone().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", file=sys.stderr, flush=True)


def run_query(
    database: Database,
    query: str,
    params: dict[str, object],
) -> tuple[int, str]:
    with database.read_transaction() as session:
        result = session.query(query, params)
    payload = {
        "columns": list(result.columns),
        "rows": list(result.rows),
    }
    checksum = hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()
    return len(result.rows), checksum


def run_memory_probe_subprocess(
    *,
    script_path: Path,
    database_path: Path,
    disable_planner: bool,
    query_name: str,
    params: dict[str, object],
) -> dict[str, float]:
    env = os.environ.copy()
    if disable_planner:
        env[DISABLE_PLANNER_ENV] = "1"
    else:
        env.pop(DISABLE_PLANNER_ENV, None)
    output = subprocess.check_output(
        [
            sys.executable,
            str(script_path),
            "--child-query",
            query_name,
            "--database-path",
            str(database_path),
            "--child-params-json",
            json.dumps(params, sort_keys=True),
        ],
        text=True,
        stderr=subprocess.DEVNULL,
        env=env,
    )
    return cast(dict[str, float], json.loads(output))


def run_memory_probe(
    *,
    database_path: Path,
    query: str,
    params: dict[str, object],
) -> dict[str, float]:
    database = Database(database_path)
    pool = pa.default_memory_pool()
    before_rss_mb = current_rss_mb()
    baseline_arrow_peak = float(pool.max_memory())
    with database.read_transaction() as session:
        result = session.query(query, params)
    _ = result.rows
    after_rss_mb = current_rss_mb()
    arrow_peak_mb = max(0.0, (float(pool.max_memory()) - baseline_arrow_peak) / (1024 * 1024))
    return {
        "rss_delta_mb": max(0.0, after_rss_mb - before_rss_mb),
        "peak_rss_mb": peak_rss_mb(),
        "arrow_peak_mb": arrow_peak_mb,
    }


def current_rss_mb() -> float:
    rss_kb = int(
        subprocess.check_output(
            ["ps", "-o", "rss=", "-p", str(os.getpid())],
            text=True,
        ).strip()
    )
    return rss_kb / 1024.0


def peak_rss_mb() -> float:
    value = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    if sys.platform == "darwin":
        return value / (1024 * 1024)
    return value / 1024.0


def cast_params(payload: object) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise SystemExit("Child params payload must be a JSON object.")
    return {str(key): value for key, value in payload.items()}


def write_json_report(
    path: Path,
    args: argparse.Namespace,
    results: list[BenchmarkResult],
) -> None:
    payload = {
        "config": {
            "nodes": args.nodes,
            "edges": args.edges,
            "repeats": args.repeats,
            "warmups": args.warmups,
            "memory_repeats": args.memory_repeats,
            "seed": args.seed,
            "planner_enabled": not args.disable_planner,
            "workspace": str(args.workspace),
        },
        "results": [asdict(result) for result in results],
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_markdown_report(
    path: Path,
    args: argparse.Namespace,
    results: list[BenchmarkResult],
) -> None:
    lines = [
        "# Cypher Engine Benchmark",
        "",
        (
            "This benchmark measures representative GraphArrow query paths "
            "on a synthetic embedded database."
        ),
        "",
        "## Configuration",
        "",
        f"- Nodes: `{args.nodes}`",
        f"- Edges: `{args.edges}`",
        f"- Repeats: `{args.repeats}`",
        f"- Warmups: `{args.warmups}`",
        f"- Memory probe repeats: `{args.memory_repeats}`",
        f"- Seed: `{args.seed}`",
        f"- Planner enabled: `{'yes' if not args.disable_planner else 'no'}`",
        "",
        "## Results",
        "",
        "| Query | Median (s) | Peak RSS Δ (MB) | Arrow peak (MB) | Rows | Checksum |",
        "| --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for result in results:
        lines.append(
            "| "
            f"{result.query} | "
            f"{result.median_seconds:.6f} | "
            f"{result.median_rss_delta_mb:.2f} | "
            f"{result.median_arrow_peak_mb:.2f} | "
            f"{result.row_count} | "
            f"`{result.checksum[:12]}` |"
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- The benchmark includes parse, compile, execution, and public result shaping.",
            "- The database is generated once per configuration and reused across measured runs.",
            "- Results should be compared on the same machine and similar thermal/load conditions.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
