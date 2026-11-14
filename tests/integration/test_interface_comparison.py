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
from neo4j.exceptions import Neo4jError

from grapharrow.config import GraphArrowSettings
from grapharrow.core.errors import QueryError
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


@pytest.mark.parametrize(
    "query,expected",
    [
        (
            "MATCH (p:Person) WITH CASE WHEN p.age IS NULL THEN 'missing' "
            "WHEN p.age>=30 THEN 'older' ELSE 'younger' END AS category "
            "WITH category,count(*) AS n RETURN category,n ORDER BY category",
            [
                {"category": "missing", "n": 1},
                {"category": "older", "n": 1},
                {"category": "younger", "n": 3},
            ],
        ),
        (
            "CALL () { MATCH (p:Person) RETURN p.name AS name "
            "UNION MATCH (p:Person) RETURN p.name AS name } RETURN name ORDER BY name",
            [{"name": n} for n in ["Alex", "Alice", "Bob", "Carol"]],
        ),
        (
            "MATCH (c:Company) CALL (c) { MATCH (p:Person)-[:WORKS]->(c) "
            "RETURN count(p) AS n } RETURN c.name AS company,n ORDER BY company",
            [
                {"company": "Acme", "n": 3},
                {"company": "Beta", "n": 2},
                {"company": "Empty", "n": 0},
            ],
        ),
        (
            "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:WORKS]->(c) "
            "WHERE c.name='Acme' } RETURN p.name AS name ORDER BY name",
            [{"name": "Alice"}, {"name": "Bob"}],
        ),
    ],
)
def test_composable_cypher_matches_across_frontends(
    interfaces: Interfaces, query: str, expected: list[dict[str, Any]]
) -> None:
    results = interfaces.read_cypher(query, {})
    assert all(result["rows"] == expected for result in results.values())
    with interfaces.bolt.session() as session, session.begin_transaction() as transaction:
        assert transaction.run(query).data() == expected


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


@pytest.mark.parametrize("writer", ["embedded", "rest", "bolt"])
def test_write_correctness_is_visible_through_every_interface(
    interfaces: Interfaces, writer: str
) -> None:
    interfaces.write(writer, 'CREATE (:FixA {name:"a"})<-[:FIXED]-(:FixB {name:"b"})', {})
    expected = {"columns": ["a", "b"], "rows": [{"a": "a", "b": "b"}]}
    results = interfaces.read_cypher(
        "MATCH (a:FixA)<-[:FIXED]-(b:FixB) RETURN a.name AS a,b.name AS b", {}
    )
    assert all(result == expected for result in results.values()), results
    headers = interfaces.session()
    try:
        response = interfaces.call(
            headers,
            "pattern_query",
            {
                "start_label": "FixA",
                "start_as": "a",
                "hops": [
                    {
                        "relationship_type": "FIXED",
                        "direction": "in",
                        "target_label": "FixB",
                        "as": "b",
                    }
                ],
                "select": [
                    {"var": "a", "property": "name", "alias": "a"},
                    {"var": "b", "property": "name", "alias": "b"},
                ],
            },
        )
        assert interfaces.fetch(headers, response["handle"]) == expected
    finally:
        assert interfaces.rest.delete("/sessions/current", headers=headers).status_code == 204

    interfaces.write(writer, "CREATE (n:FixLoop)-[:FIXLOOP]->(n)", {})
    results = interfaces.read_cypher(
        "MATCH (n:FixLoop)-[r:FIXLOOP]-(n) RETURN count(r) AS total", {}
    )
    assert all(result["rows"] == [{"total": 1}] for result in results.values()), results

    interfaces.write(writer, 'CREATE (:FixNull {name:"keep",p:42})', {})
    interfaces.write(writer, "MATCH (n:FixNull) SET n.p=null", {})
    results = interfaces.read_cypher("MATCH (n:FixNull) RETURN keys(n) AS props", {})
    assert all(result["rows"] == [{"props": ["name"]}] for result in results.values()), results

    query = "CREATE (n:FixWindow) RETURN n LIMIT 0"
    if writer == "embedded":
        with interfaces.embedded.write_transaction() as session:
            result = session.query(query)
            assert result.columns == ("n",) and result.rows == ()
    elif writer == "rest":
        assert interfaces.rest_query(query, {}) == {"columns": ["n"], "rows": []}
    else:
        with interfaces.bolt.session() as session:
            result = session.run(query)
            assert list(result.keys()) == ["n"] and result.data() == []
    results = interfaces.read_cypher("MATCH (n:FixWindow) RETURN count(n) AS total", {})
    assert all(result["rows"] == [{"total": 1}] for result in results.values()), results


def test_bolt_null_property_removal_commit_and_rollback(interfaces: Interfaces) -> None:
    interfaces.write("embedded", "CREATE (:TxNull {p:42})", {})
    with interfaces.bolt.session() as session:
        tx = session.begin_transaction()
        assert tx.run("MATCH (n:TxNull) SET n.p=null RETURN n.p AS p").data() == [{"p": None}]
        assert interfaces.rest_query("MATCH (n:TxNull) RETURN n.p AS p", {})["rows"] == [{"p": 42}]
        tx.rollback()
        assert interfaces.rest_query("MATCH (n:TxNull) RETURN n.p AS p", {})["rows"] == [{"p": 42}]
        tx = session.begin_transaction()
        tx.run("MATCH (n:TxNull) SET n.p=null").consume()
        tx.commit()
    results = interfaces.read_cypher("MATCH (n:TxNull) RETURN keys(n) AS props", {})
    assert all(result["rows"] == [{"props": []}] for result in results.values()), results


@pytest.mark.parametrize(
    "query,expected",
    [
        (
            "MATCH (p:Person) WITH p.name AS name WHERE p.name='Alice' RETURN name",
            [{"name": "Alice"}],
        ),
        ("RETURN [1,2,3][null..] AS value", [{"value": None}]),
        (r"RETURN '\u01FF' AS value", [{"value": "ǿ"}]),
        ("RETURN 4^3*2^3 AS value", [{"value": 512.0}]),
        ("UNWIND [1,2.0,5,null] AS x RETURN max(x) AS value", [{"value": 5}]),
        ("RETURN range(3,-1,-2) AS xs", [{"xs": [3, 1, -1]}]),
        (
            "MATCH (p:Person {name:'Alice'}) "
            "RETURN properties(head([p])).name AS name, last([1,2]) AS last, tail([1,2]) AS tail",
            [{"name": "Alice", "last": 2, "tail": [2]}],
        ),
        (
            "RETURN toInteger('-42.9') AS i, toFloat('1.2e2') AS f, "
            "toBoolean(' TRUE ') AS b, toString(false) AS s, sign(-3) AS sign",
            [{"i": -42, "f": 120.0, "b": True, "s": "false", "sign": -1}],
        ),
        (
            "RETURN toString(toFloat('NaN')) AS nan, toString(toFloat('-Infinity')) AS inf",
            [{"nan": "NaN", "inf": "-Infinity"}],
        ),
        (
            "MATCH (p:Person) OPTIONAL MATCH (p)-[:MISSING]->(x) "
            "RETURN p.name AS name,collect(x) AS nodes ORDER BY name",
            [{"name": name, "nodes": []} for name in ["Alex", "Alice", "Bob", "Carol"]],
        ),
    ],
)
def test_semantic_results_match_across_query_interfaces(
    interfaces: Interfaces, query: str, expected: list[dict[str, Any]]
) -> None:
    results = interfaces.read_cypher(query, {})
    assert all(result["rows"] == expected for result in results.values()), results


def test_persisted_temporal_values_across_interfaces(interfaces: Interfaces) -> None:
    from neo4j.time import Date, Duration

    with interfaces.bolt.session() as session:
        session.run(
            "CREATE (:Event {day:$day})-[:LASTS {elapsed:$elapsed}]->(:End)",
            day=Date(2024, 2, 29),
            elapsed=Duration(seconds=1, nanoseconds=2),
        ).consume()

    query = "MATCH (e:Event)-[r:LASTS]->() RETURN e.day AS day, r.elapsed AS elapsed"
    results = interfaces.read_cypher(query, {})
    for name in ("embedded", "rest", "mcp"):
        assert results[name]["rows"] == [{"day": "2024-02-29", "elapsed": "PT1.000000002S"}]
    assert results["bolt"]["rows"] == [
        {"day": Date(2024, 2, 29), "elapsed": Duration(seconds=1, nanoseconds=2)}
    ]

    headers = interfaces.session()
    try:
        result = interfaces.call(
            headers,
            "pattern_query",
            {
                "start_label": "Event",
                "start_as": "e",
                "hops": [
                    {
                        "relationship_type": "LASTS",
                        "direction": "out",
                        "target_label": "End",
                        "as": "end",
                    }
                ],
                "select": [{"var": "e", "property": "day", "alias": "day"}],
            },
        )
        assert interfaces.fetch(headers, result["handle"])["rows"] == [{"day": "2024-02-29"}]
        result = interfaces.call(
            headers, "node_search", {"label": "Event", "property": "day", "value": "2024-02-29"}
        )
        assert result["matched_count"] == 1
    finally:
        assert interfaces.rest.delete("/sessions/current", headers=headers).status_code == 204


@pytest.mark.parametrize(
    ("query", "category", "phase", "detail"),
    [
        ("RETURN )", "SyntaxError", "compile", "UnexpectedSyntax"),
        ("MATCH (r)-[r]->() RETURN r", "SyntaxError", "compile", "VariableTypeConflict"),
        ("MATCH (n) RETURN missing.p", "SyntaxError", "compile", "UndefinedVariable"),
        ("RETURN false AND 123 AS value", "SyntaxError", "compile", "InvalidArgumentType"),
        ("RETURN $missing", "ParameterMissing", "compile", "MissingParameter"),
        ("RETURN 1/0", "ArithmeticError", "runtime", "DivisionByZero"),
        ("RETURN range(1,3,0)", "ArgumentError", "runtime", "NumberOutOfRange"),
        ("RETURN toFloat(true)", "TypeError", "runtime", "InvalidArgumentValue"),
    ],
)
def test_query_error_metadata_matches_across_interfaces(
    interfaces: Interfaces, query: str, category: str, phase: str, detail: str
) -> None:
    with interfaces.embedded.read_transaction() as session, pytest.raises(QueryError) as embedded:
        session.query(query)
    expected = embedded.value.metadata()
    assert (expected["category"], expected["phase"], expected["detail"]) == (
        category,
        phase,
        detail,
    )
    response = interfaces.rest.post(
        "/v1/query", headers={"Authorization": "Bearer rest-secret"}, json={"query": query}
    )
    assert response.status_code == 400, response.text
    assert response.json()["error"] == {
        "code": "QUERYERROR",
        "message": str(embedded.value),
        "details": expected,
    }
    with interfaces.bolt.session() as session, pytest.raises(Neo4jError) as bolt:
        session.run(query).consume()
    assert bolt.value.code == expected["code"]
    assert bolt.value._metadata["grapharrow_error"] == expected
    with pytest.raises(QueryError) as mcp:
        interfaces.mcp.call_tool("query", {"query": query})
    assert mcp.value.metadata() == expected


@pytest.mark.parametrize("interface", ["embedded", "rest", "bolt", "mcp"])
def test_write_pipeline_is_atomic_across_interfaces(interfaces: Interfaces, interface: str) -> None:
    graph = interfaces.embedded._app.use_cases.graph
    before = graph.load_state()
    query = "CREATE (:Scratch) WITH 0 AS x CREATE (:Other {p:1/x})"
    if interface == "rest":
        response = interfaces.rest.post(
            "/v1/query", headers={"Authorization": "Bearer rest-secret"}, json={"query": query}
        )
        assert response.status_code == 400
        assert response.json()["error"]["details"]["detail"] == "DivisionByZero"
    elif interface == "bolt":
        with pytest.raises(Neo4jError) as caught:
            interfaces.write(interface, query, {})
        assert caught.value._metadata["grapharrow_error"]["detail"] == "DivisionByZero"
    else:
        with pytest.raises(QueryError) as error:
            interfaces.write(interface, query, {})
        assert error.value.detail == "DivisionByZero"
    assert graph.load_state() == before

    interfaces.write(
        interface,
        "UNWIND [1,2,3] AS x CREATE (n:Pipeline {x:x}) WITH n SET n.value=n.x*2 REMOVE n.x",
        {},
    )
    assert graph.load_state().snapshot.snapshot_id.value == before.snapshot.snapshot_id.value + 1
    results = interfaces.read_cypher(
        "MATCH (n:Pipeline) RETURN n.value AS value ORDER BY value", {}
    )
    assert all(
        result["rows"] == [{"value": 2}, {"value": 4}, {"value": 6}] for result in results.values()
    )


def test_bolt_explicit_pipeline_visibility_and_commit(interfaces: Interfaces) -> None:
    graph = interfaces.embedded._app.use_cases.graph
    before = graph.load_state().snapshot.snapshot_id.value
    verify = "MATCH (n:PrivatePipeline) RETURN count(n) AS n"
    with interfaces.bolt.session() as session:
        tx = session.begin_transaction()
        assert tx.run(
            "UNWIND [1,2] AS x CREATE (n:PrivatePipeline {x:x}) "
            "WITH n SET n.x=n.x*10 RETURN sum(n.x) AS total"
        ).data() == [{"total": 30}]
        assert tx.run(verify).data() == [{"n": 2}]
        assert interfaces.rest_query(verify, {})["rows"] == [{"n": 0}]
        tx.commit()
    assert interfaces.rest_query(verify, {})["rows"] == [{"n": 2}]
    assert graph.load_state().snapshot.snapshot_id.value == before + 1


def test_named_path_round_trips_through_public_interfaces(interfaces: Interfaces) -> None:
    from neo4j.graph import Path as BoltPath

    query = (
        "CREATE p = (a:PathA)-[:FIRST]->(:PathB)<-[:SECOND]-(:PathC) SET a.ready = true RETURN p"
    )
    with interfaces.bolt.session() as session:
        written = session.run(query).single()["p"]
        assert isinstance(written, BoltPath)
        assert dict(written.nodes[0]) == {"ready": True}
        read = "MATCH p = (:PathA)-[:FIRST]->(:PathB)<-[:SECOND]-(:PathC) RETURN p, [p] AS nested"
        bolt = session.run(read).single()
        assert isinstance(bolt["p"], BoltPath)
        assert bolt["p"] == bolt["nested"][0]
        assert bolt["p"].relationships[1].start_node == bolt["p"].nodes[2]
        assert bolt["p"].relationships[1].end_node == bolt["p"].nodes[1]
    with interfaces.embedded.read_transaction() as session:
        embedded = list(session.query(read).rows)
    assert interfaces.rest_query(read, {})["rows"] == embedded
    assert interfaces.mcp.call_tool("query", {"query": read})["rows"] == embedded
    assert [node["labels"] for node in embedded[0]["p"]["nodes"]] == [
        ["PathA"],
        ["PathB"],
        ["PathC"],
    ]


def test_variable_length_paths_round_trip_through_public_interfaces(interfaces: Interfaces) -> None:
    from neo4j.graph import Path as BoltPath

    interfaces.write("embedded", "CREATE (:PathA)<-[:STEP]-(:PathB)<-[:STEP]-(:PathC)", {})
    query = "MATCH p=(:PathA)<-[:STEP*0..2]-() RETURN p,length(p) AS n ORDER BY n"
    with interfaces.embedded.read_transaction() as session:
        expected = list(session.query(query).rows)
    assert [row["n"] for row in expected] == [0, 1, 2]
    assert interfaces.rest_query(query, {})["rows"] == expected
    assert interfaces.mcp.call_tool("query", {"query": query})["rows"] == expected
    with interfaces.bolt.session() as session:
        rows = list(session.run(query))
        assert [row["n"] for row in rows] == [0, 1, 2]
        for row, comparison in zip(rows, expected, strict=True):
            path = row["p"]
            assert isinstance(path, BoltPath)
            assert [sorted(node.labels) for node in path.nodes] == [
                node["labels"] for node in comparison["p"]["nodes"]
            ]
            assert list(edge.start_node for edge in path.relationships) == list(path.nodes[1:])
