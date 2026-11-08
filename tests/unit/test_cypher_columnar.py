"""Columnar pipelines must preserve row semantics and avoid canonical graph reads."""

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from grapharrow.app import build_application
from grapharrow.core.errors import QueryError


@pytest.fixture
def app(tmp_path: Path) -> Any:
    application = build_application(tmp_path / "columnar")
    application.use_cases.graph.create_nodes_bulk(
        table=pa.table(
            {
                "name": ["Alice", "Bob", "Carol", "Dan"],
                "age": [30, 20, 30, None],
                "country": ["IT", None, "IT", "FR"],
                "payload": ["unrequested" * 100] * 4,
            }
        ),
        default_labels=("Person",),
    )
    return application


def test_narrow_reader_scan_never_loads_canonical_graph(
    app: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    graph = app.use_cases.graph
    scans = []
    original = graph.load_node_property_table

    def scan(**kwargs: Any) -> Any:
        result = original(**kwargs)
        scans.append((kwargs, result.num_rows))
        return result

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("Columnar query loaded the canonical graph")

    monkeypatch.setattr(graph, "load_node_property_table", scan)
    monkeypatch.setattr(graph, "load_state", forbidden)
    monkeypatch.setattr(app.repositories.graph, "load_node_table", forbidden)
    monkeypatch.setattr(app.repositories.graph, "load_edge_table", forbidden)
    result = app.use_cases.cypher.execute(
        "MATCH (p:Person) WHERE p.age>=$low AND p.age<$high "
        "WITH coalesce(p.name,'?') AS name,p.age AS age "
        "WITH name,age+1 AS next RETURN name,next ORDER BY name",
        {"low": 25, "high": 35},
    )
    assert result.rows == ({"name": "Alice", "next": 31}, {"name": "Carol", "next": 31})
    assert len(scans) == 1
    args, count = scans[0]
    assert set(args["columns"]) == {"node_id", "name", "age"}
    assert [(p.column, p.operator, p.value) for p in args["predicates"]] == [
        ("age", ">=", 25),
        ("age", "<", 35),
    ]
    assert count == 2


@pytest.mark.parametrize(
    "query",
    [
        "MATCH (p:Person) RETURN coalesce(p.age,0) AS age ORDER BY age DESC",
        "MATCH (p:Person) WITH p AS q WITH q.name AS name,q.age AS age RETURN name,age ORDER BY "
        "age,name DESC",
        "MATCH (p:Person) RETURN CASE WHEN p.age>=30 THEN 'older' ELSE 'other' END AS category "
        "ORDER BY category",
        "MATCH (p:Person) RETURN CASE p.country WHEN 'IT' THEN 1 ELSE 0 END AS n ORDER BY n",
        "MATCH (p:Person) RETURN coalesce(p.missing,'unknown') AS value",
        "MATCH (p:Person) WHERE p.age IS NULL RETURN coalesce(p.name,'?') AS name",
        "MATCH (p:Person) WHERE p.age IS NOT NULL AND p.name STARTS WITH 'A' RETURN "
        "coalesce(p.age,0) AS age",
        "MATCH (p:Person) WITH p.name AS name ORDER BY name LIMIT 2 WHERE p.age<30 WITH name "
        "RETURN name",
        "MATCH (p:Person) WITH p.name AS name WITH name RETURN DISTINCT name ORDER BY name",
        "MATCH (p:Person) WITH coalesce(p.country,'unknown') AS country WITH country,count(*) "
        "AS n RETURN country,n ORDER BY country",
        "MATCH (p:Person) RETURN coalesce(p.name,'?') AS name UNION MATCH (p:Person) RETURN "
        "coalesce(p.name,'?') AS name",
        "MATCH (p:Person) RETURN coalesce(p.name,'?') AS name UNION ALL MATCH (p:Person) RETURN "
        "coalesce(p.name,'?') AS name",
        "MATCH (p:Person {country:'IT'}) RETURN coalesce(p.name,'?') AS name ORDER BY name",
        "MATCH (p:Person) WHERE p.age<0 WITH coalesce(p.age,0) AS age WITH count(*) AS n RETURN n",
        "MATCH (p:Person) RETURN CASE WHEN true THEN 1 ELSE 9223372036854775807+1 END AS value",
        "MATCH (p:Person) RETURN CASE WHEN true THEN 1 ELSE 1/0 END AS value",
        "MATCH (p:Person) RETURN coalesce(p.age,0.5) AS value ORDER BY value",
        "MATCH (p:Person) RETURN coalesce(p.age,true) AS value ORDER BY value",
    ],
)
def test_columnar_and_row_results_match(
    app: Any, monkeypatch: pytest.MonkeyPatch, query: str
) -> None:
    monkeypatch.setenv("GRAPHARROW_DISABLE_CYPHER_COLUMNAR", "1")
    expected = app.use_cases.cypher.execute(query)
    monkeypatch.delenv("GRAPHARROW_DISABLE_CYPHER_COLUMNAR")
    assert app.use_cases.cypher.execute(query) == expected


@pytest.mark.parametrize(
    "values",
    [
        [{"x": 1}, "literal", None],
        [9007199254740993, 1.5, None],
        [True, 1, None],
        ["ß", "İ", None],
        [float("nan"), 1.0, None],
    ],
)
def test_unsafe_physical_encodings_and_unicode_keep_exact_results(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    values: list[Any],
) -> None:
    app = build_application(tmp_path / "fallback")
    for value in values:
        app.use_cases.graph.create_node(labels=("Item",), properties={"value": value})
    for query in (
        "MATCH (p:Item) RETURN coalesce(p.value,'missing') AS value",
        "MATCH (p:Item) RETURN CASE WHEN p.value=true THEN 1 ELSE 0 END AS value",
    ):
        monkeypatch.setenv("GRAPHARROW_DISABLE_CYPHER_COLUMNAR", "1")
        expected = app.use_cases.cypher.execute(query)
        monkeypatch.delenv("GRAPHARROW_DISABLE_CYPHER_COLUMNAR")
        actual = app.use_cases.cypher.execute(query)
        assert repr(actual) == repr(expected)


def test_fallback_retains_snapshot_captured_before_concurrent_publish(
    app: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    graph = app.use_cases.graph
    original = graph.load_node_property_schema
    first = True

    def schema(**kwargs: Any) -> Any:
        nonlocal first
        result = original(**kwargs)
        if first:
            first = False
            graph.create_node(labels=("Person",), properties={"name": "New", "age": 99})
        # Missing encoding metadata forces fallback after snapshot capture.
        return pa.schema([field.with_metadata(None) for field in result])

    monkeypatch.setattr(graph, "load_node_property_schema", schema)
    query = "MATCH (p:Person) RETURN coalesce(p.name,'?') AS name ORDER BY name"
    assert len(app.use_cases.cypher.execute(query).rows) == 4
    assert len(app.use_cases.cypher.execute(query).rows) == 5


def test_columnar_filter_type_error_is_not_hidden_by_pushdown(app: Any) -> None:
    with pytest.raises(QueryError):
        app.use_cases.cypher.execute(
            "MATCH (p:Person) WHERE 1 AND p.age<0 RETURN coalesce(p.name,'?') AS name"
        )
