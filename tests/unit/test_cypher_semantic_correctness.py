"""Regressions for semantic validation, aggregates, WITH and expression values."""

from pathlib import Path

import pytest

from grapharrow import Database
from grapharrow.core.errors import QueryError


@pytest.fixture
def database(tmp_path: Path) -> Database:
    db = Database(tmp_path / "db")
    with db.write_transaction() as session:
        session.query("CREATE (a:A {name:'B',p:2})-[:R {p:3}]->(b:A {name:'C',p:1})")
    return db


@pytest.mark.parametrize(
    "query",
    [
        "MATCH (r)-[r]->() RETURN r",
        "MATCH ()-[r]->(r) RETURN r",
        "MATCH ()-[r]->() MATCH (r) RETURN r",
        "MATCH (r) MATCH ()-[r]->() RETURN r",
        "MATCH (a)-[r]->()-[r]->(a) RETURN r",
        "MATCH (r)-[r]->() RETURN coalesce(r,null)",
        "MATCH (n:Missing) RETURN type(n)",
        "MATCH ()-[r:Missing]->() RETURN labels(r)",
        "MATCH (n:Missing) RETURN unknown.p",
        "MATCH (n) WHERE n.name=unknown RETURN n",
        "MATCH (n) WHERE (n)-[newRelationship]->() RETURN n",
        "MATCH (n) RETURN (n)-->()",
        "MATCH (n) WITH (n)-->() AS x RETURN x",
        "MATCH () RETURN *",
        "MATCH (n) WITH n, count(*) RETURN n",
        "MATCH (n) RETURN DISTINCT n.name ORDER BY n.p",
        "MATCH (n) WITH n.name AS name, min(n.p) AS p ORDER BY sum(n.p) RETURN name,p",
        "RETURN false AND 123 AS value",
        "RETURN true OR [] AS value",
        "MATCH (n:Missing) RETURN false AND 'x' AS value",
        "RETURN [x IN [1,2] | count(*)] AS value",
        "RETURN 9223372036854775808 AS value",
        "RETURN -9223372036854775809 AS value",
    ],
)
def test_invalid_bindings_and_types_raise_query_error(database: Database, query: str) -> None:
    with database.read_transaction() as session, pytest.raises(QueryError):
        session.query(query)


@pytest.mark.parametrize(
    "query",
    [
        "CREATE (:Scratch {p:missing})",
        "CREATE (:Scratch {p:{nested:missing}})",
        "CREATE (:Scratch {p:[missing]})",
        "CREATE (:Scratch) RETURN *",
        "MATCH (a:A) CREATE (a)-[:R]->(b:Scratch {p:missing})",
        "MATCH (n:A) SET n.p=missing",
    ],
)
def test_undefined_write_values_cannot_publish(database: Database, query: str) -> None:
    before = database._app.use_cases.graph.load_state()
    with database.write_transaction() as session, pytest.raises(QueryError):
        session.query(query)
    after = database._app.use_cases.graph.load_state()
    assert after.snapshot == before.snapshot
    assert after.nodes == before.nodes and after.edges == before.edges


def test_multiple_aggregates_collect_entities(database: Database) -> None:
    with database.read_transaction() as session:
        result = session.query("MATCH (n:A) RETURN count(n) AS total,collect(n) AS nodes")
    assert result.rows[0]["total"] == 2
    assert sorted(node["properties"]["name"] for node in result.rows[0]["nodes"]) == ["B", "C"]


@pytest.mark.parametrize("distinct", ["", "DISTINCT "])
def test_grouped_collect_discards_optional_null_entities(database: Database, distinct: str) -> None:
    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:A) OPTIONAL MATCH (n)-[:MISSING]->(x) "
            f"RETURN n.name AS name, collect({distinct}x) AS nodes ORDER BY name"
        )
    assert result.rows == ({"name": "B", "nodes": []}, {"name": "C", "nodes": []})


@pytest.mark.parametrize("function,expected", [("max", 5), ("min", 1)])
def test_numeric_aggregate_preserves_selected_value_type(
    database: Database, function: str, expected: int
) -> None:
    with database.read_transaction() as session:
        value = session.query(f"UNWIND [1,2.0,5,null,3.2] AS x RETURN {function}(x) AS value").rows[
            0
        ]["value"]
    assert type(value) is int and value == expected


@pytest.mark.parametrize("distinct", ["", "DISTINCT "])
def test_with_filter_sees_projected_incoming_property(database: Database, distinct: str) -> None:
    with database.read_transaction() as session:
        result = session.query(
            f"MATCH (a:A) WITH {distinct}a.name AS name WHERE a.name='B' RETURN name"
        )
    assert result.rows == ({"name": "B"},)


def test_with_filter_and_order_use_input_and_output_scope(database: Database) -> None:
    with database.read_transaction() as session:
        result = session.query(
            "MATCH (a:A) WITH a.name AS name ORDER BY a.p LIMIT 1 "
            "WHERE name='B' OR a.name='C' RETURN name"
        )
    assert result.rows == ({"name": "C"},)


def test_with_cannot_leak_unprojected_variable_to_next_clause(database: Database) -> None:
    with database.read_transaction() as session, pytest.raises(QueryError):
        session.query("MATCH (a:A) WITH a.name AS name WHERE a.name='B' RETURN a")


@pytest.mark.parametrize(
    "expression,expected",
    [
        (r"'it\'s'", "it's"),
        (r"'a\\b\nc\td'", "a\\b\nc\td"),
        (r"'\u01FF'", "ǿ"),
        (r"'\U0001F600'", "😀"),
        ("4 ^ 3 * 2 ^ 3", 512.0),
        ("4 ^ 3 + 2 ^ 3", 72.0),
        ("-3 ^ 2", 9.0),
        ("-(3 ^ 2)", -9.0),
        ("2 ^ 3 ^ 2", 64.0),
        ("[1,2,3][null..null]", None),
        ("[1,2,3][1..null]", None),
        ("[1,2,3][null..2]", None),
        ("[1,2,3][..null]", None),
        ("[1,2,3][null..]", None),
        ("[1,2,3][..2]", [1, 2]),
        ("[1,2,3][1..]", [2, 3]),
    ],
)
def test_expression_values(database: Database, expression: str, expected: object) -> None:
    with database.read_transaction() as session:
        result = session.query(f"RETURN {expression} AS value")
    assert result.rows == ({"value": expected},)
    if isinstance(expected, float):
        assert type(result.rows[0]["value"]) is float


@pytest.mark.parametrize(
    "expression", ["coalesce(null,-9223372036854775808)", "-9223372036854775808"]
)
def test_minimum_integer_literal_remains_valid(database: Database, expression: str) -> None:
    with database.read_transaction() as session:
        assert session.query(f"RETURN {expression} AS value").rows == ({"value": -(2**63)},)


@pytest.mark.parametrize("projection", ["n.p", "labels(n)", "n.p+1"])
@pytest.mark.parametrize("boundary", ["", "WITH n "])
def test_deleted_entity_access_cannot_publish(
    database: Database, projection: str, boundary: str
) -> None:
    before = database._app.use_cases.graph.load_state()
    with database.write_transaction() as session, pytest.raises(QueryError, match="deleted"):
        session.query(f"MATCH (n:A) {boundary}DETACH DELETE n RETURN {projection}")
    after = database._app.use_cases.graph.load_state()
    assert after.snapshot == before.snapshot and after.nodes == before.nodes


def test_adjacent_property_comparison_and_list_addition(database: Database) -> None:
    with database.write_transaction() as session:
        session.query("MATCH (n:A) SET n.list1=[1,2],n.list2=[3]")
    with database.read_transaction() as session:
        assert session.query("MATCH (n:A)-[:R]->(m:A) WHERE n.name=m.name RETURN n").rows == ()
        result = session.query("MATCH (n:A) RETURN n.list2+n.list1 AS value")
    assert result.rows == ({"value": [3, 1, 2]}, {"value": [3, 1, 2]})


def test_heterogeneous_properties_keep_types_and_predicate_values(database: Database) -> None:
    with database.write_transaction() as session:
        session.query(
            "CREATE (root:Root {name:'xx'})-[:T]->(:Child {v:0}),"
            "(root)-[:T]->(:Child {v:'xx'}),(root)-[:T]->(:Child)"
        )
    with database.read_transaction() as session:
        result = session.query(
            "MATCH (:Root)-->(i:Child) WHERE i.v IS NULL OR i.v > 'x' RETURN i.v AS value"
        )
        assert {row["value"] for row in result.rows} == {None, "xx"}
        assert session.query(
            "MATCH (root:Root)-->(i:Child) WHERE root.name=i.v RETURN i.v AS value"
        ).rows == ({"value": "xx"},)


@pytest.mark.parametrize("predicate", ["NOT n.p IN [2,null]", "NOT n.p=2"])
def test_negated_predicates_preserve_nulls(database: Database, predicate: str) -> None:
    with database.write_transaction() as session:
        session.query("CREATE (:A)")
    with database.read_transaction() as session:
        rows = session.query(f"MATCH (n:A) WHERE {predicate} RETURN n.name AS name").rows
    assert rows == (() if "IN" in predicate else ({"name": "C"},))


@pytest.mark.parametrize(
    "predicate", ["NOT n.missing=2", "NOT n.missing IN [2]", "NOT n.missing CONTAINS 'x'"]
)
def test_missing_properties_remain_unknown_under_negation(
    database: Database, predicate: str
) -> None:
    with database.read_transaction() as session:
        assert session.query(f"MATCH (n:A) WHERE {predicate} RETURN n").rows == ()


def test_grouped_numeric_aggregates_preserve_types_and_distinct(database: Database) -> None:
    with database.write_transaction() as session:
        session.query("CREATE (:Mixed {g:'a',v:1}),(:Mixed {g:'a',v:1.0}),(:Mixed {g:'b',v:2})")
    with database.read_transaction() as session:
        rows = session.query(
            "MATCH (n:Mixed) RETURN n.g AS g, min(n.v) AS low, "
            "count(DISTINCT n.v) AS total ORDER BY g"
        ).rows
    assert rows == ({"g": "a", "low": 1, "total": 1}, {"g": "b", "low": 2, "total": 1})
    assert all(type(row["low"]) is int for row in rows)


def test_deleted_relationship_type_and_identity_remain_available(database: Database) -> None:
    with database.write_transaction() as session:
        result = session.query(
            "MATCH ()-[r:R]->() DELETE r RETURN type(r) AS kind,id(r) AS identity ORDER BY type(r)"
        )
    assert len(result.rows) == 1 and result.rows[0]["kind"] == "R"
    assert isinstance(result.rows[0]["identity"], int)
    assert database._app.use_cases.graph.load_state().edges == ()


def test_escaped_nested_property_map_survives_reopen(database: Database, tmp_path: Path) -> None:
    with database.write_transaction() as session:
        session.query(r"CREATE (:Escaped {m:{text:'it\'s,a:b',other:'\u01FF'}})")
    with Database(tmp_path / "db").read_transaction() as session:
        assert session.query("MATCH (n:Escaped) RETURN n.m AS value").rows == (
            {"value": {"text": "it's,a:b", "other": "ǿ"}},
        )


def test_star_projection_columns_match_neo4j_order(database: Database) -> None:
    with database.read_transaction() as session:
        result = session.query("WITH [1] AS xs,[2] AS ys UNWIND xs AS x UNWIND ys AS y RETURN *")
    assert result.columns == ("x", "xs", "y", "ys")


@pytest.mark.parametrize(
    "operation,needle", [("CONTAINS", "B"), ("STARTS WITH", "B"), ("ENDS WITH", "B")]
)
def test_negated_string_predicate_keeps_non_matching_rows(
    database: Database, operation: str, needle: str
) -> None:
    with database.write_transaction() as session:
        session.query("CREATE (:A)")
    with database.read_transaction() as session:
        result = session.query(
            f"MATCH (a:A) WHERE NOT a.name {operation} $needle RETURN a.name AS name",
            {"needle": needle},
        )
    assert result.rows == ({"name": "C"},)


@pytest.mark.parametrize("projection", ["r", "collect(r) AS relationships"])
def test_exact_one_hop_variable_length_binding_is_a_list(
    database: Database, projection: str
) -> None:
    with database.read_transaction() as session:
        result = session.query("MATCH (a)-[r:R*1..1]->(b) RETURN " + projection)
    value = next(iter(result.rows[0].values()))
    if projection.startswith("collect"):
        value = value[0]
    assert isinstance(value, list) and len(value) == 1
    assert value[0]["type"] == "R" and value[0]["properties"] == {"p": 3}
