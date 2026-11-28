"""Columnar pipelines must preserve row semantics and avoid canonical graph reads."""

import math
from collections import Counter
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from grapharrow.app import build_application
from grapharrow.config import GraphArrowSettings
from grapharrow.core.errors import QueryError
from grapharrow.use_cases import cypher as cypher_module


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


@pytest.mark.parametrize("backend", ["native", "delta"])
def test_fixed_hop_count_pipelines_preserve_duplicates_and_optional_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, backend: str
) -> None:
    if backend == "delta":
        pytest.importorskip("deltalake")
    app = build_application(
        tmp_path / backend, settings=GraphArrowSettings(storage_backend=backend)
    )
    cypher = app.use_cases.cypher
    cypher.execute(
        "CREATE (a:Source {tags:['a']}), (b:Source), (:Source), "
        "(x:Device {band:7, active:true, tags:[1,null]}), "
        "(y:Device {band:7, active:false, tags:[1,null]}), (z:Device), "
        "(a)-[:LINK]->(x), (a)-[:LINK]->(x), (a)-[:LINK]->(y), (b)-[:LINK]->(z)"
    )
    queries = (
        (
            "MATCH (:Source)-[:LINK]->(t:Device) WITH DISTINCT t.band AS band RETURN count(*) AS n",
            2,
        ),
        (
            "MATCH (s:Source) WITH s, s.tags AS tags "
            "OPTIONAL MATCH (s)-[:LINK]->(t:Device) WHERE t.active = true "
            "RETURN count(*) AS n",
            4,
        ),
    )

    def no_records(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("A supported scalar count materialized graph records")

    with monkeypatch.context() as patch:
        patch.setattr(app.use_cases.graph, "load_state", no_records)
        for query, expected in queries:
            assert cypher.execute(query).rows == ({"n": expected},)
    with monkeypatch.context() as patch:
        patch.setenv("GRAPHARROW_DISABLE_CYPHER_COLUMNAR", "1")
        for query, expected in queries:
            assert cypher.execute(query).rows == ({"n": expected},)

    cypher.execute("CREATE (:Source)-[:LINK]->(:Device {tags:[null,1]})")
    list_query = (
        "MATCH (:Source)-[:LINK]->(t:Device) WITH DISTINCT t.tags AS tags RETURN count(*) AS n"
    )
    with monkeypatch.context() as patch:
        patch.setattr(app.use_cases.graph, "load_state", no_records)
        assert cypher.execute(list_query).rows == ({"n": 3},)
    cypher.execute("CREATE (:Source)-[:LINK]->(:Device {tags:[1.0,null]})")
    # Mixed numeric encodings fall back and keep integer/floating equality.
    assert cypher.execute(list_query).rows == ({"n": 3},)
    monkeypatch.setenv("GRAPHARROW_DISABLE_CYPHER_COLUMNAR", "1")
    assert cypher.execute(list_query).rows == ({"n": 3},)


@pytest.mark.parametrize("backend", ["native", "delta"])
def test_with_optional_grouped_counts_use_arrow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, backend: str
) -> None:
    if backend == "delta":
        pytest.importorskip("deltalake")
    app = build_application(
        tmp_path / backend, settings=GraphArrowSettings(storage_backend=backend)
    )
    cypher = app.use_cases.cypher
    cypher.execute(
        "CREATE (a:Source {zone:1}), (b:Source {zone:2}), (:Source {zone:2}), (:Source), "
        "(x:Reading {active:true, value:7}), (y:Reading {active:true}), "
        "(z:Reading {active:false, value:8}), (other:Noise {value:9}), "
        "(a)-[:LINK]->(x), (a)-[:LINK]->(x), (a)-[:LINK]->(y), "
        "(a)-[:LINK]->(other), (b)-[:LINK]->(z), (b)-[:LINK]->(other)"
    )
    query = (
        "MATCH (s:Source) WITH s, s.zone AS zone "
        "OPTIONAL MATCH (s)-[:LINK]->(t:Reading) WHERE t.active = true "
        "RETURN zone AS area, count(*) AS rows, count(t.value) AS readings, "
        "count(zone) AS known ORDER BY area DESC"
    )

    def no_records(*args: Any, **kwargs: Any) -> None:
        pytest.fail("A grouped optional count materialized graph records")

    with monkeypatch.context() as patch:
        patch.setenv("GRAPHARROW_DISABLE_CYPHER_COLUMNAR", "1")
        expected = cypher.execute(query)
    assert expected.rows == (
        {"area": None, "rows": 1, "readings": 0, "known": 0},
        {"area": 2, "rows": 2, "readings": 0, "known": 2},
        {"area": 1, "rows": 3, "readings": 2, "known": 3},
    )
    with monkeypatch.context() as patch:
        patch.setattr(app.use_cases.graph, "load_state", no_records)
        assert cypher.execute(query) == expected
        assert cypher.execute(query.replace(" WHERE t.active = true", "")).rows == (
            {"area": None, "rows": 1, "readings": 0, "known": 0},
            {"area": 2, "rows": 2, "readings": 1, "known": 2},
            {"area": 1, "rows": 3, "readings": 2, "known": 3},
        )
        assert cypher.execute(
            "MATCH (s:Absent) WITH s OPTIONAL MATCH (s)-[:LINK]->(t:Reading) "
            "RETURN count(t.value) AS readings"
        ).rows == ({"readings": 0},)

    cypher.execute("CREATE (:Source {zone:[1]})")
    with monkeypatch.context() as patch:
        patch.setenv("GRAPHARROW_DISABLE_CYPHER_COLUMNAR", "1")
        expected_mixed = cypher.execute(query)
    assert cypher.execute(query) == expected_mixed


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


def test_numeric_functions_and_string_predicates_use_arrow_columns(
    app: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    def forbid_graph_read(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("Query loaded the complete graph")

    monkeypatch.setattr(app.use_cases.graph, "load_state", forbid_graph_read)
    result = app.use_cases.cypher.execute(
        "MATCH (p:Person) RETURN p.name AS name, ceil(p.age) AS ceiling, sqrt(p.age) AS root, "
        "p.age STARTS WITH '3' AS prefix, p.name CONTAINS $suffix AS hasPart ORDER BY name",
        {"suffix": 3},
    )
    assert result.rows == (
        {"name": "Alice", "ceiling": 30.0, "root": math.sqrt(30), "prefix": None, "hasPart": None},
        {"name": "Bob", "ceiling": 20.0, "root": math.sqrt(20), "prefix": None, "hasPart": None},
        {"name": "Carol", "ceiling": 30.0, "root": math.sqrt(30), "prefix": None, "hasPart": None},
        {"name": "Dan", "ceiling": None, "root": None, "prefix": None, "hasPart": None},
    )


@pytest.mark.parametrize("operator", ["CONTAINS", "STARTS WITH", "ENDS WITH"])
def test_negated_string_predicate_on_non_text_does_not_match(app: Any, operator: str) -> None:
    cypher = app.use_cases.cypher
    assert (
        cypher.execute(
            f"MATCH (p:Person) WHERE NOT p.age {operator} '3' RETURN p.name AS name"
        ).rows
        == ()
    )
    assert (
        cypher.execute(
            f"MATCH (p:Person) WHERE NOT p.name {operator} $argument RETURN p.name AS name",
            {"argument": 3},
        ).rows
        == ()
    )


def test_computed_string_predicate_operand_reaches_expression_pipeline(app: Any) -> None:
    assert app.use_cases.cypher.execute(
        "MATCH (p:Person) WHERE p.name STARTS WITH 'A' + 'l' RETURN p.name AS name"
    ).rows == ({"name": "Alice"},)


@pytest.mark.parametrize(
    "anchor, pattern, name, expected",
    [
        (
            "MATCH (p:Person {name:$name})",
            "(a)-[:LINK]->(q:Person)",
            "Alice",
            ["Alice", "Bob", "Bob"],
        ),
        (
            "MATCH (p:Person) WHERE p.name=$name",
            "(a)<-[:LINK]-(q:Person)",
            "Bob",
            ["Alice", "Alice"],
        ),
        (
            "MATCH (p:Person {name:$name})",
            "(a)-[:LINK]->(:Person)<-[:LINK]-(q:Person)",
            "Alice",
            ["Alice", "Alice"],
        ),
        ("MATCH (p:Person {name:$name})", "(a)-[:LINK]->(q:Person)", "Missing", []),
    ],
)
def test_selective_with_match_uses_tabular_expansion(
    app: Any,
    monkeypatch: pytest.MonkeyPatch,
    anchor: str,
    pattern: str,
    name: str,
    expected: list[str],
) -> None:
    cypher = app.use_cases.cypher
    cypher.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) "
        "CREATE (a)-[:LINK]->(a), (a)-[:LINK]->(b), (a)-[:LINK]->(b)"
    )
    cypher.execute("CREATE INDEX person_name FOR (p:Person) ON (p.name)")
    query = f"{anchor} WITH p AS a MATCH {pattern} RETURN q.name AS name"
    params = {"name": name}
    monkeypatch.setenv("GRAPHARROW_DISABLE_CYPHER_COLUMNAR", "1")
    reference = cypher.execute(query, params)
    monkeypatch.delenv("GRAPHARROW_DISABLE_CYPHER_COLUMNAR")

    def forbid_records(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("Selective expansion loaded every graph record")

    graph = app.use_cases.graph
    monkeypatch.setattr(graph, "load_state", forbid_records)
    result = cypher.execute(query, params)
    assert result.columns == reference.columns == ("name",)
    assert sorted(row["name"] for row in result.rows) == expected
    assert sorted(row["name"] for row in reference.rows) == expected
    assert graph.property_indexes.builds == 1
    if expected:
        assert graph.adjacency_indexes._entries


@pytest.mark.parametrize(
    "query, name, ages, encoded_rows",
    [
        (
            "MATCH (a:Person {name:$name})-[:LINK]->(q:Person) RETURN q.age AS age",
            "Alice",
            [30, 20, 20, None],
            [1, 3],
        ),
        (
            "MATCH (a:Person {name:$name}) WITH a AS start "
            "MATCH (start)-[:LINK]->(:Person)-[:LINK]->(q:Person) RETURN q.age AS age",
            "Alice",
            [20, 20, None, 30, 30],
            [1, 3, 3],
        ),
        (
            "MATCH (a:Person {name:$name}) WITH a AS start "
            "MATCH (start)<-[:LINK]-(q:Person) RETURN q.age AS age",
            "Bob",
            [30, 30],
            [1, 1],
        ),
        (
            "MATCH (a:Person {name:$name})-[:LINK]->(q:Person) RETURN q.age AS age",
            "Dan",
            [],
            [1],
        ),
        (
            "MATCH (a:Person {name:$name})-[:WIDE]->(q:Person) RETURN q.age AS age",
            "Alice",
            [30, 20, 30, None, *([40] * 28)],
            [1, 32],
        ),
    ],
)
def test_expansion_encodes_labels_for_reached_nodes(
    app: Any,
    monkeypatch: pytest.MonkeyPatch,
    query: str,
    name: str,
    ages: list[int | None],
    encoded_rows: list[int],
) -> None:
    app.use_cases.graph.create_nodes_bulk(
        table=pa.table({"name": [f"Extra {index}" for index in range(28)], "age": [40] * 28}),
        default_labels=("Person",),
    )
    cypher = app.use_cases.cypher
    cypher.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}) "
        "CREATE (a)-[:LINK]->(a), (a)-[:LINK]->(b), (a)-[:LINK]->(b), "
        "(a)-[:LINK]->(d), (b)-[:LINK]->(c)"
    )
    cypher.execute("MATCH (a:Person {name:'Alice'}), (q:Person) CREATE (a)-[:WIDE]->(q)")
    cypher.execute("CREATE INDEX person_name FOR (p:Person) ON (p.name)")

    encoded = []
    original = cypher_module._encode_binding_labels

    def record_rows(column: pa.ChunkedArray, *, normalize: bool = False) -> pa.Array:
        encoded.append(len(column))
        return original(column, normalize=normalize)

    monkeypatch.setattr(cypher_module, "_encode_binding_labels", record_rows)
    result = cypher.execute(query, {"name": name})
    assert result.columns == ("age",)
    assert Counter(row["age"] for row in result.rows) == Counter(ages)
    assert encoded == encoded_rows


def test_selective_expansion_keeps_snapshot_pinned_during_planning(
    app: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    graph = app.use_cases.graph
    cypher = app.use_cases.cypher
    cypher.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) CREATE (a)-[:LINK]->(b)"
    )
    original_schema = graph.load_node_property_schema

    def publish_after_capture(**kwargs: Any) -> Any:
        monkeypatch.setattr(graph, "load_node_property_schema", original_schema)
        schema = original_schema(**kwargs)
        cypher.execute(
            "MATCH (a:Person {name:'Alice'}), (c:Person {name:'Carol'}) CREATE (a)-[:LINK]->(c)"
        )
        return schema

    monkeypatch.setattr(graph, "load_node_property_schema", publish_after_capture)
    query = (
        "MATCH (a:Person {name:'Alice'}) WITH a MATCH (a)-[:LINK]->(q:Person) RETURN q.name AS name"
    )
    assert cypher.execute(query).rows == ({"name": "Bob"},)
    assert sorted(row["name"] for row in cypher.execute(query).rows) == ["Bob", "Carol"]


def test_selective_expansion_preserves_native_text_values(tmp_path: Path) -> None:
    app = build_application(
        tmp_path / "native-text",
        settings=GraphArrowSettings(_env_file=None, parquet_filter_pushdown_enabled=False),
    )
    graph = app.use_cases.graph
    _, nodes = graph.create_nodes_bulk(
        table=pa.table({"name": ["Alice", '__grapharrow_join_json__:"Alice"', "Bob", "Carol"]}),
        default_labels=("Person",),
    )
    raw = '__grapharrow_join_json__:{"value":1}'
    graph.create_edges_bulk(
        table=pa.table(
            {
                "src_id": [nodes[0].node_id, nodes[1].node_id],
                "dst_id": [nodes[2].node_id, nodes[3].node_id],
                "rel_type": ["LINK", "LINK"],
            }
        )
    )
    cypher = app.use_cases.cypher
    cypher.execute("MATCH (p:Person {name:'Bob'}) SET p.value=$value", {"value": raw})
    result = cypher.execute(
        "MATCH (a:Person {name:'Alice'}) WITH a MATCH (a)-[:LINK]->(b:Person) "
        "RETURN b.value AS value, b.value AS copy"
    )
    assert result.rows == ({"value": raw, "copy": raw},)


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


@pytest.mark.parametrize("direction", ["incoming", "outgoing"])
@pytest.mark.parametrize("pushdown", [True, False])
def test_oversized_snapshot_reads_only_typed_adjacency(
    app: Any, monkeypatch: pytest.MonkeyPatch, direction: str, pushdown: bool
) -> None:
    graph = app.use_cases.graph
    cypher = app.use_cases.cypher
    store = app.repositories.graph
    graph.snapshot_tables.max_bytes = 1
    store._parquet.filter_pushdown_enabled = pushdown
    cypher.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}) "
        "CREATE (a)-[:LINK]->(b), (a)-[:LINK]->(b), (c)-[:LINK]->(a)"
    )

    def forbid_canonical(*args: Any) -> Any:
        raise AssertionError("Selective traversal loaded the canonical edges")

    monkeypatch.setattr(store, "load_edge_table", forbid_canonical)
    calls = []
    incoming = direction == "incoming"
    name = f"load_adjacency_by_{'dest' if incoming else 'source'}_table"
    original = getattr(store, name)

    def read(*args: Any, **kwargs: Any) -> pa.Table:
        calls.append(kwargs["predicates"])
        return original(*args, **kwargs)

    monkeypatch.setattr(store, name, read)
    relationship = "<-[:LINK]-" if incoming else "-[:LINK]->"
    query = (
        "MATCH (a:Person {name:'Alice'}) WITH a AS seed "
        f"MATCH (seed){relationship}(b:Person) RETURN b.name AS name"
    )
    result = cypher.execute(query)
    expected = ["Carol"] if incoming else ["Bob", "Bob"]
    assert [row["name"] for row in result.rows] == expected
    assert len(calls) == len(calls[0]) == 1
    predicate = calls[0][0]
    assert (predicate.column, predicate.operator) == ("dst_id" if incoming else "src_id", "in")
    assert len(predicate.value) == 1


def test_selective_adjacency_falls_back_for_old_manifest(
    app: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dataclasses import replace

    graph = app.use_cases.graph
    cypher = app.use_cases.cypher
    graph.snapshot_tables.max_bytes = 1
    cypher.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) CREATE (a)-[:LINK]->(b)"
    )
    snapshot = replace(graph.capture_snapshot(), adjacency_by_dest=())
    monkeypatch.setattr(graph, "capture_snapshot", lambda: snapshot)
    result = cypher.execute(
        "MATCH (b:Person {name:'Bob'}) WITH b MATCH (b)<-[:LINK]-(a:Person) RETURN a.name AS name"
    )
    assert result.rows == ({"name": "Alice"},)
