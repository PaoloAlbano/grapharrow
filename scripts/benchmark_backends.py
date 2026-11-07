"""Benchmark candidate data access backends on Parquet datasets."""

from __future__ import annotations

import argparse
import json
import statistics
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

LABELS = np.array(["Person", "Company", "Employee", "Investor"], dtype=object)
COUNTRIES = np.array(["IT", "DE", "FR", "US", "UK", "ES"], dtype=object)
REL_TYPES = np.array(["KNOWS", "WORKS_AT", "INVESTED_IN"], dtype=object)
WORKSPACE = Path(".benchmarks") / "backend_evaluation"


@dataclass(frozen=True, slots=True)
class BenchmarkResult:
    backend: str
    query: str
    median_seconds: float
    best_seconds: float
    worst_seconds: float
    runs: tuple[float, ...]
    row_count: int
    checksum: float


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark Parquet access backends.")
    parser.add_argument("--nodes", type=int, default=300_000, help="Number of synthetic nodes.")
    parser.add_argument("--edges", type=int, default=1_200_000, help="Number of synthetic edges.")
    parser.add_argument("--repeats", type=int, default=5, help="Measured runs per query/backend.")
    parser.add_argument("--warmups", type=int, default=1, help="Warmup runs per query/backend.")
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for synthetic data generation.",
    )
    parser.add_argument(
        "--workspace",
        type=Path,
        default=WORKSPACE,
        help="Workspace directory for generated benchmark data.",
    )
    parser.add_argument(
        "--json-out",
        type=Path,
        default=WORKSPACE / "data-access-backend-benchmark.json",
        help="JSON output path for benchmark results.",
    )
    parser.add_argument(
        "--markdown-out",
        type=Path,
        default=WORKSPACE / "data-access-backend-benchmark.md",
        help="Markdown output path for benchmark results.",
    )
    args = parser.parse_args()

    nodes_path, edges_path = prepare_dataset(
        workspace=args.workspace,
        node_count=args.nodes,
        edge_count=args.edges,
        seed=args.seed,
    )

    benchmark_fns = {
        "pyarrow": lambda: run_pyarrow(nodes_path, edges_path),
        "pandas": lambda: run_pandas(nodes_path, edges_path),
        "polars": lambda: run_polars(nodes_path, edges_path),
        "duckdb": lambda: run_duckdb(nodes_path, edges_path),
    }

    all_results: list[BenchmarkResult] = []
    expected_outputs: dict[str, tuple[int, float]] = {}

    for backend, runner in benchmark_fns.items():
        outputs = runner()
        for query_name, callable_ in outputs.items():
            result = measure_callable(
                backend=backend,
                query_name=query_name,
                callable_=callable_,
                warmups=args.warmups,
                repeats=args.repeats,
            )
            signature = (result.row_count, result.checksum)
            if query_name in expected_outputs and expected_outputs[query_name] != signature:
                raise RuntimeError(
                    f"Backend output mismatch for {query_name}: "
                    f"{backend} returned {signature}, expected {expected_outputs[query_name]}"
                )
            expected_outputs.setdefault(query_name, signature)
            all_results.append(result)

    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.markdown_out.parent.mkdir(parents=True, exist_ok=True)
    write_json_report(args.json_out, args, all_results)
    write_markdown_report(args.markdown_out, args, all_results)

    print(f"Wrote JSON report to {args.json_out}")
    print(f"Wrote Markdown report to {args.markdown_out}")


def prepare_dataset(
    *,
    workspace: Path,
    node_count: int,
    edge_count: int,
    seed: int,
) -> tuple[Path, Path]:
    workspace.mkdir(parents=True, exist_ok=True)
    nodes_path = workspace / f"nodes-{node_count}-seed-{seed}.parquet"
    edges_path = workspace / f"edges-{edge_count}-nodes-{node_count}-seed-{seed}.parquet"

    if nodes_path.exists() and edges_path.exists():
        return nodes_path, edges_path

    rng = np.random.default_rng(seed)

    node_ids = np.arange(1, node_count + 1, dtype=np.int64)
    nodes_table = pa.table(
        {
            "node_id": node_ids,
            "primary_label": rng.choice(LABELS, size=node_count, p=[0.6, 0.15, 0.15, 0.1]),
            "country": rng.choice(COUNTRIES, size=node_count),
            "age": rng.integers(18, 80, size=node_count, dtype=np.int32),
            "active": rng.random(node_count) > 0.2,
            "score": rng.random(node_count).astype(np.float64),
        }
    )
    pq.write_table(nodes_table, nodes_path, compression="zstd")

    edge_ids = np.arange(1, edge_count + 1, dtype=np.int64)
    edges_table = pa.table(
        {
            "edge_id": edge_ids,
            "rel_type": rng.choice(REL_TYPES, size=edge_count, p=[0.65, 0.2, 0.15]),
            "src_id": rng.integers(1, node_count + 1, size=edge_count, dtype=np.int64),
            "dst_id": rng.integers(1, node_count + 1, size=edge_count, dtype=np.int64),
            "weight": rng.random(edge_count).astype(np.float64),
        }
    )
    pq.write_table(edges_table, edges_path, compression="zstd")

    return nodes_path, edges_path


def measure_callable(
    *,
    backend: str,
    query_name: str,
    callable_: Any,
    warmups: int,
    repeats: int,
) -> BenchmarkResult:
    for _ in range(warmups):
        callable_()

    runs: list[float] = []
    final_row_count = -1
    final_checksum = -1.0
    for _ in range(repeats):
        started = time.perf_counter()
        row_count, checksum = callable_()
        ended = time.perf_counter()
        final_row_count = int(row_count)
        final_checksum = float(checksum)
        runs.append(ended - started)

    return BenchmarkResult(
        backend=backend,
        query=query_name,
        median_seconds=statistics.median(runs),
        best_seconds=min(runs),
        worst_seconds=max(runs),
        runs=tuple(runs),
        row_count=final_row_count,
        checksum=final_checksum,
    )


def run_pyarrow(nodes_path: Path, edges_path: Path) -> dict[str, Any]:
    nodes_ds = ds.dataset(nodes_path, format="parquet")
    edges_ds = ds.dataset(edges_path, format="parquet")

    def q1() -> tuple[int, float]:
        table = nodes_ds.to_table(
            columns=["node_id", "age"],
            filter=(
                (ds.field("primary_label") == "Person")
                & (ds.field("country") == "IT")
                & (ds.field("age") >= 40)
            ),
        )
        limited = table.sort_by([("age", "descending"), ("node_id", "ascending")]).slice(0, 100)
        return limited.num_rows, float(pc.sum(limited["node_id"]).as_py() or 0.0)

    def q2() -> tuple[int, float]:
        filtered_edges = edges_ds.to_table(
            columns=["src_id", "dst_id", "weight"],
            filter=(ds.field("rel_type") == "KNOWS") & (ds.field("weight") > 0.5),
        )
        active_sources = nodes_ds.to_table(
            columns=["node_id", "active"],
            filter=ds.field("active"),
        )
        european_targets = nodes_ds.to_table(
            columns=["node_id", "country"],
            filter=(ds.field("country") == "IT") | (ds.field("country") == "DE"),
        )
        joined = filtered_edges.join(
            active_sources,
            keys="src_id",
            right_keys="node_id",
            join_type="inner",
        )
        joined = joined.join(
            european_targets,
            keys="dst_id",
            right_keys="node_id",
            join_type="inner",
        )
        return joined.num_rows, float(pc.sum(joined["dst_id"]).as_py() or 0.0)

    def hop_join(hops: int) -> tuple[int, float]:
        knows_edges = edges_ds.to_table(
            columns=["src_id", "dst_id"],
            filter=ds.field("rel_type") == "KNOWS",
        )
        nodes_it = nodes_ds.to_table(
            columns=["node_id", "country"],
            filter=ds.field("country") == "IT",
        )
        nodes_active = nodes_ds.to_table(
            columns=["node_id", "active"],
            filter=ds.field("active"),
        )

        joined = knows_edges.rename_columns(["src_id_1", "dst_id_1"])
        for hop in range(2, hops + 1):
            next_edges = knows_edges.rename_columns([f"src_id_{hop}", f"dst_id_{hop}"])
            joined = joined.join(
                next_edges,
                keys=f"dst_id_{hop - 1}",
                right_keys=f"src_id_{hop}",
                join_type="inner",
            )

        joined = joined.join(
            nodes_it,
            keys="src_id_1",
            right_keys="node_id",
            join_type="inner",
        )
        joined = joined.join(
            nodes_active,
            keys=f"dst_id_{hops}",
            right_keys="node_id",
            join_type="inner",
        )
        return joined.num_rows, float(pc.sum(joined[f"dst_id_{hops}"]).as_py() or 0.0)

    return {
        "node_filter_sort_limit": q1,
        "one_hop_join": q2,
        "two_hop_join": lambda: hop_join(2),
        "three_hop_join": lambda: hop_join(3),
        "four_hop_join": lambda: hop_join(4),
    }


def run_pandas(nodes_path: Path, edges_path: Path) -> dict[str, Any]:
    def q1() -> tuple[int, float]:
        nodes = pd.read_parquet(nodes_path)
        filtered = nodes[
            (nodes["primary_label"] == "Person") & (nodes["country"] == "IT") & (nodes["age"] >= 40)
        ][["node_id", "age"]]
        limited = filtered.sort_values(
            by=["age", "node_id"],
            ascending=[False, True],
        ).head(100)
        return int(len(limited)), float(limited["node_id"].sum())

    def q2() -> tuple[int, float]:
        nodes = pd.read_parquet(nodes_path)
        edges = pd.read_parquet(edges_path)
        filtered_edges = edges[(edges["rel_type"] == "KNOWS") & (edges["weight"] > 0.5)][
            ["src_id", "dst_id", "weight"]
        ]
        active_sources = nodes[nodes["active"]][["node_id", "active"]]
        european_targets = nodes[nodes["country"].isin(["IT", "DE"])][["node_id", "country"]]
        joined = filtered_edges.merge(active_sources, left_on="src_id", right_on="node_id")
        joined = joined.merge(
            european_targets,
            left_on="dst_id",
            right_on="node_id",
            suffixes=("", "_dst"),
        )
        return int(len(joined)), float(joined["dst_id"].sum())

    def hop_join(hops: int) -> tuple[int, float]:
        nodes = pd.read_parquet(nodes_path)
        edges = pd.read_parquet(edges_path)
        knows_edges = edges[edges["rel_type"] == "KNOWS"][["src_id", "dst_id"]]
        nodes_it = nodes[nodes["country"] == "IT"][["node_id", "country"]]
        nodes_active = nodes[nodes["active"]][["node_id", "active"]]

        joined = knows_edges.rename(columns={"src_id": "src_id_1", "dst_id": "dst_id_1"})
        for hop in range(2, hops + 1):
            next_edges = knows_edges.rename(
                columns={"src_id": f"src_id_{hop}", "dst_id": f"dst_id_{hop}"}
            )
            joined = joined.merge(
                next_edges,
                left_on=f"dst_id_{hop - 1}",
                right_on=f"src_id_{hop}",
            )

        joined = joined.merge(nodes_it, left_on="src_id_1", right_on="node_id")
        joined = joined.merge(nodes_active, left_on=f"dst_id_{hops}", right_on="node_id")
        return int(len(joined)), float(joined[f"dst_id_{hops}"].sum())

    return {
        "node_filter_sort_limit": q1,
        "one_hop_join": q2,
        "two_hop_join": lambda: hop_join(2),
        "three_hop_join": lambda: hop_join(3),
        "four_hop_join": lambda: hop_join(4),
    }


def run_polars(nodes_path: Path, edges_path: Path) -> dict[str, Any]:
    def q1() -> tuple[int, float]:
        result = (
            pl.scan_parquet(nodes_path)
            .filter(
                (pl.col("primary_label") == "Person")
                & (pl.col("country") == "IT")
                & (pl.col("age") >= 40)
            )
            .select(["node_id", "age"])
            .sort(by=["age", "node_id"], descending=[True, False])
            .limit(100)
            .collect()
        )
        return result.height, float(result["node_id"].sum() or 0.0)

    def q2() -> tuple[int, float]:
        edges = (
            pl.scan_parquet(edges_path)
            .filter((pl.col("rel_type") == "KNOWS") & (pl.col("weight") > 0.5))
            .select(["src_id", "dst_id", "weight"])
        )
        active_sources = (
            pl.scan_parquet(nodes_path).filter(pl.col("active")).select(["node_id", "active"])
        )
        european_targets = (
            pl.scan_parquet(nodes_path)
            .filter(pl.col("country").is_in(["IT", "DE"]))
            .select(["node_id", "country"])
        )
        result = (
            edges.join(active_sources, left_on="src_id", right_on="node_id", how="inner")
            .join(european_targets, left_on="dst_id", right_on="node_id", how="inner")
            .collect()
        )
        return result.height, float(result["dst_id"].sum() or 0.0)

    def hop_join(hops: int) -> tuple[int, float]:
        knows_edges = (
            pl.scan_parquet(edges_path)
            .filter(pl.col("rel_type") == "KNOWS")
            .select(["src_id", "dst_id"])
        )
        nodes_it = (
            pl.scan_parquet(nodes_path)
            .filter(pl.col("country") == "IT")
            .select(["node_id", "country"])
        )
        nodes_active = (
            pl.scan_parquet(nodes_path).filter(pl.col("active")).select(["node_id", "active"])
        )
        joined = knows_edges.rename({"src_id": "src_id_1", "dst_id": "dst_id_1"})
        for hop in range(2, hops + 1):
            next_edges = knows_edges.rename({"src_id": f"src_id_{hop}", "dst_id": f"dst_id_{hop}"})
            joined = joined.join(
                next_edges,
                left_on=f"dst_id_{hop - 1}",
                right_on=f"src_id_{hop}",
                how="inner",
            )

        result = (
            joined.join(nodes_it, left_on="src_id_1", right_on="node_id", how="inner")
            .join(nodes_active, left_on=f"dst_id_{hops}", right_on="node_id", how="inner")
            .collect()
        )
        return result.height, float(result[f"dst_id_{hops}"].sum() or 0.0)

    return {
        "node_filter_sort_limit": q1,
        "one_hop_join": q2,
        "two_hop_join": lambda: hop_join(2),
        "three_hop_join": lambda: hop_join(3),
        "four_hop_join": lambda: hop_join(4),
    }


def run_duckdb(nodes_path: Path, edges_path: Path) -> dict[str, Any]:
    nodes_sql = str(nodes_path)
    edges_sql = str(edges_path)

    def q1() -> tuple[int, float]:
        connection = duckdb.connect()
        try:
            row_count, checksum = connection.execute(
                f"""
                WITH filtered AS (
                    SELECT node_id, age
                    FROM parquet_scan('{nodes_sql}')
                    WHERE primary_label = 'Person'
                      AND country = 'IT'
                      AND age >= 40
                    ORDER BY age DESC, node_id ASC
                    LIMIT 100
                )
                SELECT COUNT(*), COALESCE(SUM(node_id), 0)
                FROM filtered
                """
            ).fetchone()
        finally:
            connection.close()
        return int(row_count), float(checksum)

    def q2() -> tuple[int, float]:
        connection = duckdb.connect()
        try:
            row_count, checksum = connection.execute(
                f"""
                SELECT COUNT(*), COALESCE(SUM(e.dst_id), 0)
                FROM parquet_scan('{edges_sql}') AS e
                JOIN parquet_scan('{nodes_sql}') AS src
                  ON e.src_id = src.node_id
                JOIN parquet_scan('{nodes_sql}') AS dst
                  ON e.dst_id = dst.node_id
                WHERE e.rel_type = 'KNOWS'
                  AND e.weight > 0.5
                  AND src.active = TRUE
                  AND dst.country IN ('IT', 'DE')
                """
            ).fetchone()
        finally:
            connection.close()
        return int(row_count), float(checksum)

    def hop_join(hops: int) -> tuple[int, float]:
        edge_aliases = [f"e{i}" for i in range(1, hops + 1)]
        joins = []
        for index in range(1, hops):
            joins.append(
                f"JOIN parquet_scan('{edges_sql}') AS {edge_aliases[index]} "
                f"ON {edge_aliases[index - 1]}.dst_id = {edge_aliases[index]}.src_id"
            )
        joins_sql = "\n                ".join(joins)
        last_alias = edge_aliases[-1]
        rel_type_predicate = " AND ".join(f"{alias}.rel_type = 'KNOWS'" for alias in edge_aliases)

        connection = duckdb.connect()
        try:
            row_count, checksum = connection.execute(
                f"""
                SELECT COUNT(*), COALESCE(SUM({last_alias}.dst_id), 0)
                FROM parquet_scan('{edges_sql}') AS e1
                {joins_sql}
                JOIN parquet_scan('{nodes_sql}') AS start_node
                  ON e1.src_id = start_node.node_id
                JOIN parquet_scan('{nodes_sql}') AS end_node
                  ON {last_alias}.dst_id = end_node.node_id
                WHERE {rel_type_predicate}
                  AND start_node.country = 'IT'
                  AND end_node.active = TRUE
                """
            ).fetchone()
        finally:
            connection.close()
        return int(row_count), float(checksum)

    return {
        "node_filter_sort_limit": q1,
        "one_hop_join": q2,
        "two_hop_join": lambda: hop_join(2),
        "three_hop_join": lambda: hop_join(3),
        "four_hop_join": lambda: hop_join(4),
    }


def write_json_report(
    output_path: Path,
    args: argparse.Namespace,
    results: list[BenchmarkResult],
) -> None:
    payload = {
        "parameters": {
            "nodes": args.nodes,
            "edges": args.edges,
            "repeats": args.repeats,
            "warmups": args.warmups,
            "seed": args.seed,
        },
        "results": [asdict(result) for result in results],
    }
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_markdown_report(
    output_path: Path,
    args: argparse.Namespace,
    results: list[BenchmarkResult],
) -> None:
    grouped: dict[str, list[BenchmarkResult]] = {}
    for result in results:
        grouped.setdefault(result.query, []).append(result)

    lines = [
        "# Data Access Backend Benchmark",
        "",
        "This report was generated by `scripts/benchmark_backends.py`.",
        "",
        "## Parameters",
        "",
        f"- Nodes: `{args.nodes}`",
        f"- Edges: `{args.edges}`",
        f"- Warmups: `{args.warmups}`",
        f"- Measured runs: `{args.repeats}`",
        f"- Seed: `{args.seed}`",
        "",
    ]

    for query_name, query_results in grouped.items():
        ordered = sorted(query_results, key=lambda item: item.median_seconds)
        lines.extend(
            [
                f"## {query_name}",
                "",
                "| Backend | Median (s) | Best (s) | Worst (s) | Row count | Checksum |",
                "| --- | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for result in ordered:
            lines.append(
                "| "
                f"{result.backend} | "
                f"{result.median_seconds:.6f} | "
                f"{result.best_seconds:.6f} | "
                f"{result.worst_seconds:.6f} | "
                f"{result.row_count} | "
                f"{result.checksum:.2f} |"
            )
        lines.append("")

    output_path.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    main()
