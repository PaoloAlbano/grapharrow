"""Compare graph results and snapshot visibility across the public interfaces."""

import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from neo4j import GraphDatabase

from grapharrow.config import GraphArrowSettings
from grapharrow.database import Database
from grapharrow.interfaces.bolt import run_bolt_server
from grapharrow.interfaces.mcp_cypher import create_mcp_cypher_interface
from grapharrow.interfaces.rest import create_rest_app

pytestmark = pytest.mark.integration


@pytest.fixture(scope="session")
def comparison_report() -> Any:
    report: dict[str, Any] = {
        "query_interfaces": ["embedded", "rest", "bolt", "mcp", "ana"],
        "cases": {},
    }
    yield report["cases"]
    if destination := os.environ.get("GRAPHARROW_INTERFACE_COMPARISON_REPORT"):
        path = Path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, indent=2) + "\n")


@dataclass
class Interfaces:
    path: Path
    embedded: Database
    rest: TestClient
    bolt: Any
    mcp: Any

    def rest_query(self, query: str, params: dict[str, Any]) -> dict[str, Any]:
        response = self.rest.post(
            "/v1/query",
            headers={"Authorization": "Bearer rest-secret"},
            json={"query": query, "params": params},
        )
        assert response.status_code == 200, response.text
        return dict(response.json()["data"])

    def read_cypher(self, query: str, params: dict[str, Any]) -> dict[str, Any]:
        with self.embedded.read_transaction() as session:
            embedded = session.query(query, params)
        rest = self.rest_query(query, params)
        with self.bolt.session() as session:
            result = session.run(query, params)
            columns = result.keys()
            bolt = result.data()
        mcp = self.mcp.call_tool("query", {"query": query, "params": params, "max_rows": 100})
        return {
            "embedded": {"columns": list(embedded.columns), "rows": list(embedded.rows)},
            "rest": {"columns": rest["columns"], "rows": rest["rows"]},
            "bolt": {"columns": list(columns), "rows": bolt},
            "mcp": {"columns": mcp["columns"], "rows": mcp["rows"]},
        }

    def session(self) -> dict[str, str]:
        headers = {"Authorization": "Bearer " + "a" * 32}
        response = self.rest.post("/sessions", headers=headers)
        assert response.status_code == 201, response.text
        assert response.headers["X-ANA-Session"] == response.json()["session_id"]
        return {**headers, "X-ANA-Session": response.json()["session_id"]}

    def call(self, headers: dict[str, str], tool: str, args: dict[str, Any]) -> dict[str, Any]:
        response = self.rest.post("/tools/call", headers=headers, json={"tool": tool, "args": args})
        assert response.status_code == 200, response.text
        assert response.headers["X-ANA-Session"] == headers["X-ANA-Session"]
        return dict(response.json()["result"])

    def fetch(self, headers: dict[str, str], handle: str) -> dict[str, Any]:
        rows = []
        offset = 0
        while True:
            page = self.call(headers, "fetch", {"from": handle, "offset": offset, "limit": 1})
            rows.extend(dict(zip(page["columns"], row, strict=True)) for row in page["rows"])
            if page["next_offset"] is None:
                return {"columns": page["columns"], "rows": rows}
            offset = page["next_offset"]

    def write(self, interface: str, query: str, params: dict[str, Any]) -> None:
        if interface == "embedded":
            with self.embedded.write_transaction() as session:
                session.query(query, params)
        elif interface == "rest":
            self.rest_query(query, params)
        elif interface == "bolt":
            with self.bolt.session() as session:
                session.run(query, params).consume()
        elif interface == "mcp":
            self.mcp.call_tool("write_cypher", {"query": query, "params": params})
        elif interface == "cli-ingest":
            source = self.path.parent / "person.json"
            source.write_text(json.dumps([{"labels": ["Person"], **params}]))
            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "grapharrow.interfaces.cli",
                    "bulk-add-nodes",
                    str(self.path),
                    str(source),
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )
            assert completed.returncode == 0, completed.stderr
            assert json.loads(completed.stdout)["row_count"] == 1
        else:
            raise AssertionError(interface)


@pytest.fixture
def interfaces(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    path = tmp_path / "db"
    embedded = Database(path)
    with embedded.write_transaction() as session:
        for name, age in [("Alice", 34), ("Bob", 29), ("Carol", None), ("Alex", 20), ("Alex", 20)]:
            session.query(
                "CREATE (p:Person {name: $name, age: $age, id: $id}) RETURN p.name",
                {"name": name, "age": age, "id": "duplicate"},
            )
        for name in ("Acme", "Beta", "Empty"):
            session.query("CREATE (c:Company {name: $name}) RETURN c.name", {"name": name})
        for name, company, since in [
            ("Alice", "Acme", 2019),
            ("Alice", "Acme", 2023),
            ("Bob", "Acme", 2020),
            ("Alex", "Beta", 2022),
        ]:
            session.query(
                "MATCH (p:Person {name: $name}), (c:Company {name: $company}) "
                "CREATE (p)-[:WORKS {since: $since}]->(c) RETURN p.name",
                {"name": name, "company": company, "since": since},
            )
    auth = tmp_path / "clients.json"
    auth.write_text(
        json.dumps({"clients": [{"id": "reader", "api_key": "a" * 32, "profile": "readonly"}]})
    )
    monkeypatch.setenv("ANA_AUTH_FILE", str(auth))
    settings = GraphArrowSettings(
        server_auth_token="rest-secret",
        bolt_auth_token="bolt-secret",
        bolt_port=0,
        bolt_log_queries=False,
    )
    app = create_rest_app(path, settings=settings)
    with TestClient(app) as client, run_bolt_server(path, settings=settings) as server:
        with GraphDatabase.driver(
            f"bolt://127.0.0.1:{server.port}", auth=("neo4j", "bolt-secret")
        ) as driver:
            yield Interfaces(path, embedded, client, driver, create_mcp_cypher_interface(path))
    assert app.state.ana_resource_budget.used_bytes == 0


@dataclass(frozen=True)
class Case:
    name: str
    query: str
    params: dict[str, Any]
    columns: list[str]
    expected: list[list[Any]]
    tool: str
    args: dict[str, Any]


def test_selective_traversal_reuses_rest_index_in_ana(interfaces: Interfaces) -> None:
    query = (
        "MATCH (p:Person {name:$name})-[:WORKS]->(c:Company) "
        "RETURN c.name AS company ORDER BY c.name"
    )
    expected = {"columns": ["company"], "rows": [{"company": "Acme"}] * 2}
    results = interfaces.read_cypher(query, {"name": "Alice"})
    assert all(result == expected for result in results.values())
    container = interfaces.rest.app.state.grapharrow
    store = container.use_cases.graph.adjacency_indexes
    assert len(store._entries) == 1
    index = next(iter(store._entries.values()))[0]
    assert index.build_count == 1
    headers = interfaces.session()
    try:
        seed = interfaces.call(headers, "node_search", {"label": "Person", "value": "Alice"})
        expanded = interfaces.call(
            headers,
            "expand",
            {
                "from": seed["handle"],
                "source": "person",
                "relationship_type": "WORKS",
                "target_label": "Company",
                "as": "company",
            },
        )
        projected = interfaces.call(
            headers,
            "project",
            {
                "from": expanded["handle"],
                "select": [{"var": "company", "property": "name", "alias": "company"}],
            },
        )
        assert interfaces.fetch(headers, projected["handle"]) == expected
        assert index.build_count == 1 and store.hits > 0
        with interfaces.bolt.session() as session:
            with session.begin_transaction() as transaction:
                assert transaction.run(query, name="Alice").data() == expected["rows"]
                transaction.run(
                    "MATCH (p:Person {name:'Alice'})-[r:WORKS]->(c:Company) DELETE r"
                ).consume()
                assert transaction.run(query, name="Alice").data() == []
                transaction.rollback()
        assert interfaces.rest_query(query, {"name": "Alice"})["rows"] == expected["rows"]
    finally:
        response = interfaces.rest.delete("/sessions/current", headers=headers)
        assert response.status_code == 204
    assert interfaces.rest.app.state.ana_resource_budget.used_bytes == 0


CASES = [
    Case(
        "duplicate-names-and-null",
        "MATCH (p:Person) RETURN p.name AS name, p.age AS age ORDER BY name, age",
        {},
        ["name", "age"],
        [["Alex", 20], ["Alex", 20], ["Alice", 34], ["Bob", 29], ["Carol", None]],
        "pattern_query",
        {
            "start_label": "Person",
            "start_as": "p",
            "hops": [],
            "select": [
                {"var": "p", "property": "name", "alias": "name"},
                {"var": "p", "property": "age", "alias": "age"},
            ],
            "order_by": [{"field": "name"}, {"field": "age"}],
        },
    ),
    Case(
        "parameters",
        "MATCH (p:Person) WHERE p.age >= $minimum "
        "RETURN p.name AS name, p.age AS age ORDER BY name",
        {"minimum": 25},
        ["name", "age"],
        [["Alice", 34], ["Bob", 29]],
        "pattern_query",
        {
            "start_label": "Person",
            "start_as": "p",
            "hops": [],
            "filters": [{"var": "p", "property": "age", "op": "gte", "value": 25}],
            "select": [
                {"var": "p", "property": "name", "alias": "name"},
                {"var": "p", "property": "age", "alias": "age"},
            ],
            "order_by": [{"field": "name"}],
        },
    ),
    Case(
        "parallel-edges",
        "MATCH (p:Person)-[r:WORKS]->(c:Company) "
        "RETURN p.name AS person, c.name AS company, r.since AS since ORDER BY person, since",
        {},
        ["person", "company", "since"],
        [
            ["Alex", "Beta", 2022],
            ["Alex", "Beta", 2022],
            ["Alice", "Acme", 2019],
            ["Alice", "Acme", 2023],
            ["Bob", "Acme", 2020],
        ],
        "relationship_query",
        {
            "source_label": "Person",
            "source_as": "p",
            "target_label": "Company",
            "target_as": "c",
            "relationship_type": "WORKS",
            "rel_as": "r",
            "select": [
                {"var": "p", "property": "name", "alias": "person"},
                {"var": "c", "property": "name", "alias": "company"},
                {"var": "r", "property": "since", "alias": "since"},
            ],
            "order_by": [{"field": "person"}, {"field": "since"}],
            "distinct": False,
        },
    ),
    Case(
        "aggregates",
        "MATCH (p:Person)-[:WORKS]->(c:Company) "
        "RETURN c.name AS company, count(p) AS edges, count(DISTINCT p) AS people ORDER BY company",
        {},
        ["company", "edges", "people"],
        [["Acme", 3, 2], ["Beta", 2, 2]],
        "relationship_query",
        {
            "source_label": "Person",
            "source_as": "p",
            "target_label": "Company",
            "target_as": "c",
            "relationship_type": "WORKS",
            "group_by": [{"var": "c", "property": "name", "alias": "company"}],
            "metrics": [
                {"var": "p", "op": "count", "alias": "edges"},
                {"var": "p", "op": "count_distinct", "alias": "people"},
            ],
            "order_by": [{"field": "company"}],
        },
    ),
    Case(
        "empty-count",
        "MATCH (p:Person) WHERE p.name = $name RETURN count(p) AS count",
        {"name": "Missing"},
        ["count"],
        [[0]],
        "pattern_query",
        {
            "start_label": "Person",
            "start_as": "p",
            "hops": [],
            "filters": [{"var": "p", "property": "name", "op": "eq", "value": "Missing"}],
            "metrics": [{"var": "p", "op": "count", "alias": "count"}],
        },
    ),
]


@pytest.mark.parametrize("case", CASES, ids=[case.name for case in CASES])
def test_identical_results_across_query_interfaces(
    interfaces: Interfaces, case: Case, comparison_report: Any
) -> None:
    expected = {
        "columns": case.columns,
        "rows": [dict(zip(case.columns, row, strict=True)) for row in case.expected],
    }
    results = interfaces.read_cypher(case.query, case.params)
    headers = interfaces.session()
    try:
        result = interfaces.call(headers, case.tool, case.args)
        assert result["handle"] == "h1"
        results["ana"] = interfaces.fetch(headers, result["handle"])
        comparison_report[case.name] = {
            "query": case.query,
            "params": case.params,
            "expected": expected,
            "actual": results,
            "passed": False,
        }
        for name, result in results.items():
            assert result == expected, f"{case.name}: {name}: {result!r} != {expected!r}"
        comparison_report[case.name]["passed"] = True
    finally:
        assert interfaces.rest.delete("/sessions/current", headers=headers).status_code == 204


def test_optional_counts_preserve_zero_across_interfaces(
    interfaces: Interfaces, comparison_report: Any
) -> None:
    query = (
        "MATCH (c:Company) OPTIONAL MATCH (c)<-[:WORKS]-(p:Person) "
        "RETURN c.name AS company, count(DISTINCT p) AS people ORDER BY company"
    )
    results = interfaces.read_cypher(query, {})
    headers = interfaces.session()
    scan = interfaces.call(headers, "node_scan", {"label": "Company", "as": "c", "limit": 10})
    count = interfaces.call(
        headers,
        "optional_expand_count",
        {
            "from": scan["handle"],
            "source": "c",
            "relationship_type": "WORKS",
            "direction": "in",
            "target_label": "Person",
            "group_by": [{"var": "c", "property": "name", "alias": "company"}],
            "alias": "people",
            "distinct": True,
        },
    )
    results["ana"] = interfaces.fetch(headers, count["handle"])
    expected = [
        {"company": "Acme", "people": 2},
        {"company": "Beta", "people": 2},
        {"company": "Empty", "people": 0},
    ]
    comparison_report["optional-zero-counts"] = {
        "query": query,
        "expected": expected,
        "actual": results,
        "passed": False,
    }
    for name, result in results.items():
        assert result["columns"] == ["company", "people"], name
        assert sorted(result["rows"], key=lambda row: row["company"]) == expected, name
    comparison_report["optional-zero-counts"]["passed"] = True


@pytest.mark.parametrize("writer", ["embedded", "rest", "bolt", "mcp", "cli-ingest"])
def test_writes_are_visible_across_interfaces_with_ana_snapshot_isolation(
    interfaces: Interfaces, writer: str, comparison_report: Any
) -> None:
    older = interfaces.session()
    before = interfaces.call(older, "count_nodes", {"label": "Person", "alias": "count"})
    assert before["handle"] == "h1"
    interfaces.write(
        writer,
        "CREATE (p:Person {name: $name, age: $age, id: $id}) RETURN p.name",
        {"name": "Late", "age": 50, "id": "duplicate"},
    )
    results = interfaces.read_cypher("MATCH (p:Person) RETURN count(p) AS count", {})
    report = {"writer": writer, "actual": results, "expected_count": 6, "passed": False}
    comparison_report[f"write-{writer}"] = report
    for name, result in results.items():
        assert result["rows"] == [{"count": 6}], name
    pinned = interfaces.call(older, "count_nodes", {"label": "Person", "alias": "count"})
    report["existing_session"] = interfaces.fetch(older, pinned["handle"])["rows"]
    report["existing_handle"] = interfaces.fetch(older, before["handle"])["rows"]
    assert report["existing_session"] == [{"count": 5}]
    assert report["existing_handle"] == [{"count": 5}]
    fresh = interfaces.session()
    after = interfaces.call(fresh, "count_nodes", {"label": "Person", "alias": "count"})
    assert after["handle"] == "h1"
    report["new_session"] = interfaces.fetch(fresh, after["handle"])["rows"]
    assert report["new_session"] == [{"count": 6}]
    report["passed"] = True
