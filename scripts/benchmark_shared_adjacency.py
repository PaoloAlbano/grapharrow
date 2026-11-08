"""Compare adjacency workloads through native Cypher, REST, Bolt and new ANA workflows."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import statistics
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pyarrow as pa
from fastapi.testclient import TestClient
from neo4j import GraphDatabase

from grapharrow.app import build_application
from grapharrow.config import GraphArrowSettings
from grapharrow.interfaces.bolt import run_bolt_server
from grapharrow.interfaces.rest import create_rest_app
from grapharrow.use_cases.ana import GraphArrowAgentBackend

QUERIES = {
    "outgoing": "MATCH (a:Device {name:$name})-[:LINK]->(b:Device) "
    "RETURN b.name AS name ORDER BY b.name",
    "incoming": "MATCH (a:Device {name:$name})<-[:LINK]-(b:Device) "
    "RETURN b.name AS name ORDER BY b.name",
    "two_hop": "MATCH (a:Device {name:$name})-[:LINK]->(b:Device)-[:LINK]->(c:Device) "
    "RETURN c.name AS name ORDER BY c.name",
    "property": "MATCH (a:Device {name:$name})-[r:LINK]->(b:Device) WHERE r.weight >= 1 "
    "RETURN b.name AS name, r.weight AS weight ORDER BY b.name, r.weight",
    "broad_count": "MATCH (a:Device)-[:LINK]->(b:Device) RETURN count(*) AS count",
}


def build_fixture(path: Path, nodes: int) -> None:
    app = build_application(path)
    if app.layout.current_snapshot_path.exists():
        raise ValueError("Use a new fixture directory")
    _, records = app.use_cases.graph.create_nodes_bulk(
        table=pa.table({"name": [f"device-{i:05d}" for i in range(nodes)]}),
        default_labels=("Device",),
    )
    app.use_cases.graph.create_edges_bulk(
        table=pa.table(
            {
                "src_id": [node.node_id for node in records for _ in range(3)],
                "dst_id": [
                    records[(i + offset + 1) % nodes].node_id
                    for i in range(nodes)
                    for offset in range(3)
                ],
                "rel_type": ["LINK"] * (nodes * 3),
                "weight": [offset for _ in range(nodes) for offset in range(3)],
            }
        )
    )
    app.use_cases.indexes.create_index(
        name="device_name",
        kind="range",
        owner_kind="label",
        owner_name="Device",
        property_names=("name",),
    )


def settings(path: Path) -> GraphArrowSettings:
    return GraphArrowSettings(
        _env_file=None,
        database_path=path,
        server_auth_token="adjacency-benchmark",
        bolt_host="127.0.0.1",
        bolt_port=0,
        bolt_auth_token="adjacency-benchmark",
    )


@contextmanager
def executor(path: Path, interface: str) -> Any:
    if interface == "native":
        app = build_application(path, settings=settings(path))

        def execute(query: str, params: dict[str, Any]) -> dict[str, Any]:
            result = app.use_cases.cypher.execute(query, params)
            return {"columns": list(result.columns), "rows": list(result.rows)}

        yield execute
    elif interface == "rest_testclient":
        with TestClient(create_rest_app(path, settings=settings(path))) as client:

            def execute(query: str, params: dict[str, Any]) -> dict[str, Any]:
                response = client.post(
                    "/v1/query",
                    headers={"Authorization": "Bearer adjacency-benchmark"},
                    json={"query": query, "params": params},
                )
                response.raise_for_status()
                data = response.json()["data"]
                return {"columns": data["columns"], "rows": data["rows"]}

            yield execute
    else:
        with run_bolt_server(path, settings=settings(path)) as handle:
            with (
                GraphDatabase.driver(
                    f"bolt://127.0.0.1:{handle.port}", auth=("neo4j", "adjacency-benchmark")
                ) as driver,
                driver.session() as session,
            ):
                driver.verify_connectivity()
                transaction = (
                    session.begin_transaction() if interface == "bolt_transaction" else None
                )

                def execute(query: str, params: dict[str, Any]) -> dict[str, Any]:
                    result = (transaction if transaction is not None else session).run(
                        query, params
                    )
                    columns = list(result.keys())
                    return {"columns": columns, "rows": result.data()}

                try:
                    yield execute
                finally:
                    if transaction is not None:
                        transaction.rollback()


def measure(call: Any, repeats: int, warmups: int) -> dict[str, Any]:
    samples = []
    digests = set()
    first_ms = None
    result = None
    for iteration in range(1 + warmups + repeats):
        start = time.perf_counter_ns()
        result = call()
        elapsed = (time.perf_counter_ns() - start) / 1e6
        if iteration == 0:
            first_ms = elapsed
        elif iteration > warmups:
            samples.append(elapsed)
        digests.add(hashlib.sha256(json.dumps(result, sort_keys=True).encode()).hexdigest())
    assert len(digests) == 1
    assert result is not None
    return {
        "first_call_ms": first_ms,
        "samples_ms": samples,
        "median_ms": statistics.median(samples),
        "p95_ms": sorted(samples)[math.ceil(len(samples) * 0.95) - 1],
        "result_sha256": digests.pop(),
        "rows": result["rows"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--nodes", type=int, default=25000)
    parser.add_argument("--build-fixture", action="store_true")
    parser.add_argument("--repeats", type=int, default=21)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--interfaces",
        nargs="+",
        default=["native", "rest_testclient", "bolt", "bolt_transaction"],
        choices=["native", "rest_testclient", "bolt", "bolt_transaction"],
    )
    args = parser.parse_args()
    if args.nodes < 100 or args.repeats < 1 or args.warmups < 0:
        parser.error("Use at least 100 nodes, positive repeats and nonnegative warmups")
    if args.build_fixture:
        build_fixture(args.database, args.nodes)
    report = {
        "python": platform.python_version(),
        "pyarrow": pa.__version__,
        "platform": platform.platform(),
        "nodes": args.nodes,
        "edges": args.nodes * 3,
        "repeats": args.repeats,
        "warmups": args.warmups,
        "common_cache_limit_env": os.getenv("GRAPHARROW_ADJACENCY_INDEX_MAX_BYTES"),
        "runs": [],
    }
    params = {"name": "device-00000"}
    expected = {
        "outgoing": [{"name": f"device-{i:05d}"} for i in (1, 2, 3)],
        "incoming": [{"name": f"device-{i:05d}"} for i in range(args.nodes - 3, args.nodes)],
        "two_hop": [{"name": f"device-{i:05d}"} for i in (2, 3, 3, 4, 4, 4, 5, 5, 6)],
        "property": [{"name": "device-00002", "weight": 1}, {"name": "device-00003", "weight": 2}],
        "broad_count": [{"count": args.nodes * 3}],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    for interface in args.interfaces:
        for case, query in QUERIES.items():
            with executor(args.database, interface) as execute:
                result = measure(
                    lambda query=query: execute(query, params), args.repeats, args.warmups
                )
            assert result["rows"] == expected[case], (interface, case, result["rows"])
            report["runs"].append({"interface": interface, "case": case, **result})
            args.output.write_text(json.dumps(report, indent=2) + "\n")
            print(f"{interface} {case}: {result['median_ms']:.3f} ms", flush=True)
    app = build_application(args.database, settings=settings(args.database))
    app.use_cases.cypher.execute(QUERIES["outgoing"], params)
    with GraphArrowAgentBackend(app):

        def new_workflow() -> dict[str, Any]:
            with GraphArrowAgentBackend(app) as backend:
                seed = backend.call(
                    "node_search", {"label": "Device", "value": params["name"], "as": "source"}
                )
                expanded = backend.call(
                    "expand",
                    {
                        "from": seed["handle"],
                        "source": "source",
                        "relationship_type": "LINK",
                        "target_label": "Device",
                    },
                )
                page = backend.call("fetch", {"from": expanded["handle"]})
                return {"rows": page["rows"]}

        result = measure(new_workflow, args.repeats, args.warmups)
        assert result["rows"] == [[f"device-{i:05d}"] for i in (1, 2, 3)]
        report["runs"].append({"interface": "ana_new_workflow", "case": "outgoing", **result})
    store = getattr(app.use_cases.graph, "adjacency_indexes", None)
    if store is not None:
        report["cache"] = {
            "reserved_bytes": store.used_bytes,
            "entries": len(store._entries),
            "builds": sum(
                index.build_count for index, _ in store._entries.values() if index is not None
            ),
        }
    module = __import__("grapharrow")
    sources = sorted(Path(module.__file__).parent.rglob("*.py"))
    report["source_sha256"] = hashlib.sha256(
        b"".join(path.read_bytes() for path in sources)
    ).hexdigest()
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"ANA new workflow: {result['median_ms']:.3f} ms", flush=True)


if __name__ == "__main__":
    main()
