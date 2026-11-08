"""Reproduce engine read, aggregate, index, parser, and publication measurements."""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import platform
import statistics
import time
import tracemalloc
from collections.abc import Callable, Iterator
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import patch

import pyarrow as pa

from grapharrow.adapters.opencypher import parse_cypher
from grapharrow.app import build_application
from grapharrow.config import GraphArrowSettings
from grapharrow.use_cases.ana import GraphArrowAgentBackend
from grapharrow.use_cases.ana_patterns import aggregate_rows, finish_pattern


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def measure(operation: Callable[[], Any], repeats: int, *, memory: bool = False) -> dict[str, Any]:
    samples, signatures = [], set()
    for _ in range(repeats + 2):
        start = time.perf_counter_ns()
        result = operation()
        samples.append((time.perf_counter_ns() - start) / 1e6)
        signatures.add(digest(result))
    assert len(signatures) == 1
    out = {
        "first_ms": samples[0],
        "warm_ms": samples[2:],
        "median_ms": statistics.median(samples[2:]),
        "result_sha256": signatures.pop(),
    }
    if memory:
        gc.collect()
        tracemalloc.start()
        operation()
        _, out["python_peak_bytes"] = tracemalloc.get_traced_memory()
        tracemalloc.stop()
    return out


class Capture:
    def _store(self, rows: Any, **kwargs: Any) -> dict[str, Any]:
        return {"rows": rows}


def bindings(size: int) -> Iterator[dict[str, Any]]:
    for i in range(size):
        yield {"x": {"id": i, "bucket": i % 10, "value": (i * 7919) % size}}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodes", type=int, default=25000)
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert args.nodes > 42 and args.rows > 0 and args.repeats > 0
    results: dict[str, Any] = {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "nodes": args.nodes,
        "edges": args.nodes * 3,
        "binding_rows": args.rows,
        "repeats": args.repeats,
    }
    with TemporaryDirectory(prefix="grapharrow-engine-") as temporary:
        app = build_application(
            Path(temporary) / "database", settings=GraphArrowSettings(ana_read_cache_enabled=False)
        )
        _, nodes = app.use_cases.graph.create_nodes_bulk(
            table=pa.table(
                {
                    "name": [f"device-{i:06d}" for i in range(args.nodes)],
                    "value": list(range(args.nodes)),
                    "description": ["x" * 160] * args.nodes,
                }
            ),
            default_labels=("Device",),
        )
        app.use_cases.graph.create_edges_bulk(
            table=pa.table(
                {
                    "src_id": [node.node_id for node in nodes for _ in range(3)],
                    "dst_id": [
                        nodes[(i + delta) % args.nodes].node_id
                        for i in range(args.nodes)
                        for delta in (1, 2, 3)
                    ],
                    "rel_type": ["LINK"] * (args.nodes * 3),
                }
            )
        )
        with GraphArrowAgentBackend(app) as backend:

            def discovery(tool: str, parameters: dict[str, Any]) -> Any:
                result = backend.call(tool, parameters)
                page = backend.call("fetch", {"from": result["handle"]})
                return [{key: value for key, value in result.items() if key != "handle"}, page]

            cases = {
                "entity_resolve": ("entity_resolve", {"label": "Device", "text": "device-000042"}),
                "entity_context": (
                    "entity_resolve",
                    {"label": "Device", "text": "device-000042", "context_terms": ["x"]},
                ),
                "value_exact": (
                    "value_search",
                    {
                        "label": "Device",
                        "property": "name",
                        "text": "device-000042",
                        "match_mode": "exact",
                    },
                ),
                "value_prefix": (
                    "value_search",
                    {
                        "label": "Device",
                        "property": "name",
                        "text": "device-000",
                        "match_mode": "prefix",
                        "limit": 100,
                    },
                ),
            }
            results["discovery"] = {
                name: measure(
                    lambda tool=tool, params=params: discovery(tool, params), args.repeats
                )
                for name, (tool, params) in cases.items()
            }
        query = "MATCH (d:Device {name: $name}) RETURN d.value AS value"
        params = {"name": "device-000042"}
        results["cypher_scan"] = measure(
            lambda: app.use_cases.cypher.execute(query, params).rows, args.repeats
        )
        app.use_cases.indexes.create_index(
            name="device_name",
            kind="range",
            owner_kind="label",
            owner_name="Device",
            property_names=("name",),
        )
        results["cypher_index"] = measure(
            lambda: app.use_cases.cypher.execute(query, params).rows, args.repeats
        )
        with GraphArrowAgentBackend(app) as backend:
            results["ana_index"] = measure(
                lambda: backend.call(
                    "node_search", {"label": "Device", "property": "name", "value": "device-000042"}
                )["preview"],
                args.repeats,
            )
        index_store = getattr(app.use_cases.graph, "property_indexes", None)
        if index_store is not None:
            results["index_state"] = {
                "builds": index_store.builds,
                "hits": index_store.hits,
                "retained_bytes": index_store.used_bytes,
                "max_bytes": index_store.max_bytes,
            }
        cached_parser = getattr(app.use_cases.cypher, "_parse_cache", None)

        def syntax(cached: bool) -> str:
            tree = (
                cached_parser.parse(query)
                if cached and cached_parser is not None
                else parse_cypher(query)
            )
            return tree.getText()

        results["parse_raw"] = measure(lambda: syntax(False), args.repeats)
        results["parse_reused"] = measure(lambda: syntax(True), args.repeats)
        aggregate_args = {
            "group_by": [{"var": "x", "property": "bucket"}],
            "metrics": [
                {"op": "count", "var": "x", "alias": "count"},
                {"op": "sum", "var": "x", "property": "value", "alias": "total"},
            ],
        }
        results["aggregate"] = measure(
            lambda: aggregate_rows(bindings(args.rows), aggregate_args), args.repeats, memory=True
        )
        top_args = {
            "select": [{"var": "x", "property": "id"}],
            "order_by": [{"var": "x", "property": "value", "direction": "desc"}],
            "limit": 10,
        }
        results["top_k"] = measure(
            lambda: finish_pattern(Capture(), bindings(args.rows), "x", top_args),
            args.repeats,
            memory=True,
        )
        writes, bytes_written, latencies = [], [], []
        original = app.adapters.parquet.write_table

        def record(table: Any, path: Any, **kwargs: Any) -> None:
            original(table, path, **kwargs)
            writes[-1] += 1
            bytes_written[-1] += Path(path).stat().st_size

        with patch.object(app.adapters.parquet, "write_table", record):
            for i in range(args.repeats + 2):
                writes.append(0)
                bytes_written.append(0)
                started = time.perf_counter_ns()
                app.use_cases.graph.create_node(
                    labels=("Other",), properties={"name": f"other-{i}"}
                )
                latencies.append((time.perf_counter_ns() - started) / 1e6)
        results["publication"] = {
            "warm_ms": latencies[2:],
            "median_ms": statistics.median(latencies[2:]),
            "parquet_writes": writes[2:],
            "parquet_bytes_written": bytes_written[2:],
        }
        assert app.use_cases.admin.verify(verify_checksums=True).ok
        results["publication"]["result_sha256"] = digest(
            app.use_cases.cypher.execute("MATCH (d:Device) RETURN count(d) AS count").rows
        )
        app.use_cases.admin.vacuum()
        assert app.use_cases.admin.verify(verify_checksums=True).ok
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(results, indent=2) + "\n")
    print(f"Engine benchmark saved to {args.output}", flush=True)


if __name__ == "__main__":
    main()
