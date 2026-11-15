"""Observable semantics of composable read expressions, scopes and subqueries."""

from pathlib import Path
from typing import Any

import pytest

from grapharrow import Database
from grapharrow.core.errors import QueryError, TransactionError
from grapharrow.interfaces.cypher_guard import query_requires_write
from grapharrow.use_cases import cypher_expression
from grapharrow.use_cases.cypher import CypherQueryUseCase
from grapharrow.use_cases.graph import GraphTransactionUseCase


@pytest.fixture
def database(tmp_path: Path) -> Database:
    db = Database(tmp_path / "pipeline")
    with db.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name:'Alice',age:30,country:'IT'})"
            "-[:KNOWS {since:2020}]->(b:Person {name:'Bob',age:20})"
        )
        session.query("CREATE (:Person {name:'Carol',age:30,country:'IT'})")
    return db


@pytest.mark.parametrize(
    "expression,expected",
    [
        ("coalesce(null, 4)", 4),
        ("coalesce(null, null)", None),
        ("coalesce(false, true)", False),
        ("coalesce(0, 1/0)", 0),
        ("CASE WHEN true THEN 3 ELSE 1/0 END", 3),
        ("CASE WHEN false THEN 1/0 WHEN null THEN 1/0 ELSE 7 END", 7),
        ("CASE null WHEN null THEN 1 ELSE 2 END", 2),
        ("CASE 2 WHEN 1 THEN 9 WHEN 2 THEN coalesce(null, 5) END", 5),
        ("CASE WHEN true THEN CASE WHEN false THEN 1 ELSE 2 END END", 2),
        ("coalesce(null, 2 + 3 * 4)", 14),
        ("abs(coalesce(null, -3)) + 2", 5),
        ("size(coalesce(null, [1,2,3]))", 3),
        ("toUpper(coalesce(null, 'hello'))", "HELLO"),
        ("CASE WHEN true=1 THEN 1 ELSE 0 END", 0),
        ("CASE WHEN null=1 THEN 1 ELSE 0 END", 0),
        ("coalesce(null, false AND null)", False),
        ("coalesce(null, true OR null)", True),
        ("coalesce(null, true AND null)", None),
        ("coalesce(null, null IN [])", False),
        ("coalesce(null, 1 IN [2,null])", None),
        ("coalesce(null, [1,null]=[1,null])", None),
        ("coalesce(null, -5/2)", -2),
        ("coalesce(null, -5%2)", -1),
        ("coalesce(null, 'age: '+30)", "age: 30"),
        ("coalesce(null, 'EXISTS(n.prop)')", "EXISTS(n.prop)"),
        ("coalesce(null, [1,2,3][-1])", 3),
        ("coalesce(null, [1,2,3][1..])", [2, 3]),
        ("coalesce(null, {nested: {x:4}}.nested.x)", 4),
        ("coalesce(null, [x IN [1,2,3] WHERE x>1 | x*2])", [4, 6]),
        ("coalesce(null, all(x IN [] WHERE x>0))", True),
        ("coalesce(null, any(x IN [null,false] WHERE x))", None),
        ("reverse([1, null, [2, 3]])", [[2, 3], None, 1]),
        ("reverse('caffè')", "èffac"),
        ("reverse(null)", None),
    ],
)
def test_composed_expression_semantics(database: Database, expression: str, expected: Any) -> None:
    with database.read_transaction() as session:
        result = session.query(f"RETURN {expression} AS value")
    assert result.rows == ({"value": expected},)


def test_rand_returns_a_float_in_unit_interval(database: Database) -> None:
    with database.read_transaction() as session:
        rows = session.query("UNWIND [1,2,3] AS x RETURN RaNd() AS sample").rows
    assert len(rows) == 3
    assert all(type(row["sample"]) is float and 0 <= row["sample"] < 1 for row in rows)


def test_rand_evaluates_each_call_and_reuses_projected_aliases(
    database: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    draws = iter((0.1, 0.2, 0.3, 0.4, 0.5, 0.6))
    monkeypatch.setattr(cypher_expression, "random", lambda: next(draws))
    with database.read_transaction() as session:
        query = (
            "UNWIND [1,2] AS i WITH i, rand() AS sample "
            "RETURN i, sample, sample AS copy, rand() AS other ORDER BY i"
        )
        assert session.query(query).rows == (
            {"i": 1, "sample": 0.1, "copy": 0.1, "other": 0.3},
            {"i": 2, "sample": 0.2, "copy": 0.2, "other": 0.4},
        )
        assert session.query("RETURN rand() AS sample").rows == ({"sample": 0.5},)
        assert session.query("RETURN rand() AS sample").rows == ({"sample": 0.6},)


def test_rand_grouping_returns_the_values_used_to_form_groups(
    database: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    draws = iter((0.75, 0.25, 0.75))
    monkeypatch.setattr(cypher_expression, "random", lambda: next(draws))
    with database.read_transaction() as session:
        result = session.query(
            "UNWIND [1,2,3] AS i RETURN rand() AS sample, count(*) AS total ORDER BY sample"
        )
    assert result.rows == ({"sample": 0.25, "total": 1}, {"sample": 0.75, "total": 2})


def test_rand_in_skip_is_evaluated_once_per_clause(
    database: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    draws = iter((0.75,))
    monkeypatch.setattr(cypher_expression, "random", lambda: next(draws))
    with database.read_transaction() as session:
        result = session.query(
            "UNWIND [1,2,3] AS i WITH i ORDER BY i SKIP toInteger(rand() * 3) RETURN i"
        )
    assert result.rows == ({"i": 3},)


def test_rand_rejects_arguments_at_compile_time(database: Database) -> None:
    with database.read_transaction() as session, pytest.raises(QueryError) as failure:
        session.query("RETURN rand(1)")
    assert failure.value.phase == "compile"
    assert failure.value.detail == "InvalidNumberOfArguments"


def test_rand_requires_projection_before_aggregation(database: Database) -> None:
    with database.read_transaction() as session:
        with pytest.raises(QueryError) as failure:
            session.query("RETURN count(rand())")
        assert failure.value.phase == "compile"
        assert failure.value.detail == "NonConstantExpression"
        assert session.query("WITH rand() AS sample RETURN count(sample) AS total").rows == (
            {"total": 1},
        )


@pytest.mark.parametrize("disable_planner", [False, True])
def test_pipeline_scope_aggregation_and_window(
    database: Database, monkeypatch: pytest.MonkeyPatch, disable_planner: bool
) -> None:
    monkeypatch.setenv("GRAPHARROW_DISABLE_CYPHER_PLANNER", str(int(disable_planner)))
    with database.read_transaction() as session:
        result = session.query(
            "MATCH (p:Person) "
            "WITH coalesce(p.country,'unknown') AS country, p.age AS age "
            "WITH country, sum(CASE WHEN age>=30 THEN age ELSE 0 END) AS total "
            "WHERE total>0 WITH country,total ORDER BY total DESC LIMIT 1 "
            "RETURN country,total"
        )
    assert result.rows == ({"country": "IT", "total": 60},)


def test_optional_chain_and_with_star_keep_nulls_and_aliases(database: Database) -> None:
    with database.read_transaction() as session:
        result = session.query(
            "MATCH (p:Person) WITH p "
            "OPTIONAL MATCH (p)-[:KNOWS]->(q:Person) "
            "OPTIONAL MATCH (q)-[:KNOWS]->(r:Person) "
            "WITH *, coalesce(q.name,'none') AS friend "
            "RETURN p.name AS name,friend,r.name AS next ORDER BY name"
        )
    assert result.rows == (
        {"name": "Alice", "friend": "Bob", "next": None},
        {"name": "Bob", "friend": "none", "next": None},
        {"name": "Carol", "friend": "none", "next": None},
    )


def test_multiple_with_unwind_maps_and_entity_aliases(database: Database) -> None:
    with database.read_transaction() as session:
        result = session.query(
            "MATCH (p:Person) WITH collect(p) AS people "
            "WITH [p IN people | {node:p,name:p.name}] AS entries "
            "UNWIND entries AS entry WITH entry.node AS person, entry.name AS name "
            "MATCH (person)-[:KNOWS]->(q) RETURN name,q.name AS friend"
        )
    assert result.rows == ({"name": "Alice", "friend": "Bob"},)


def test_aggregate_empty_input_has_one_row_without_grouping(database: Database) -> None:
    with database.read_transaction() as session:
        result = session.query(
            "MATCH (p:Missing) WITH count(*) AS n,collect(p) AS people "
            "WITH coalesce(n,0) AS n,people RETURN n,people"
        )
    assert result.rows == ({"n": 0, "people": []},)


@pytest.mark.parametrize(
    "query",
    [
        "MATCH (p:Person) WITH p.name AS name WITH p RETURN name",
        "MATCH (p:Person) WITH p.name AS n WITH n MATCH (n) RETURN n",
        "MATCH (p:Person) WITH p AS a,p AS a RETURN a",
        "MATCH (p:Missing) RETURN CASE WHEN true THEN missing ELSE 0 END AS x",
        "MATCH (p:Person) RETURN CASE WHEN true THEN count(*)+p.age ELSE 0 END AS x",
        "WITH 1 AS x WITH x SKIP true RETURN x",
        "WITH 1 AS x WITH x LIMIT -1 RETURN x",
        "RETURN coalesce() AS x",
        "RETURN CASE WHEN true THEN 1 ELSE unknownFunction(2) END AS x",
        "RETURN CASE WHEN 1 THEN 2 ELSE 0 END AS x",
        "MATCH (p:Missing) RETURN CASE WHEN 1 THEN 2 ELSE 0 END AS x",
        "MATCH (p:Missing) WITH p WITH p WHERE 1 RETURN p",
        "UNWIND coalesce([1],[]) AS value MATCH (value) RETURN value",
        "RETURN coalesce(null, {x:1}[[1]]) AS value",
        "RETURN *",
        "CALL () { RETURN * } RETURN 1 AS n",
    ],
)
def test_invalid_expressions_and_scopes_raise(database: Database, query: str) -> None:
    with database.read_transaction() as session, pytest.raises(QueryError):
        session.query(query)


@pytest.mark.parametrize("operator,expected", [("UNION", [1, 2]), ("UNION ALL", [1, 1, 2])])
def test_union_duplicate_semantics(database: Database, operator: str, expected: list[int]) -> None:
    with database.read_transaction() as session:
        result = session.query(f"UNWIND [1,1] AS x RETURN x {operator} RETURN 2 AS x")
    assert [r["x"] for r in result.rows] == expected


def test_union_keeps_null_and_numeric_boolean_identity(database: Database) -> None:
    with database.read_transaction() as session:
        result = session.query(
            "RETURN null AS x UNION RETURN null AS x UNION RETURN true AS x UNION RETURN 1 AS x"
        )
    assert result.rows == ({"x": None}, {"x": True}, {"x": 1})


def test_union_preserves_entity_identity(database: Database) -> None:
    with database.read_transaction() as session:
        result = session.query("MATCH (p:Person) RETURN p UNION MATCH (p:Person) RETURN p")
    assert len(result.rows) == 3
    assert len({r["p"]["id"] for r in result.rows}) == 3


def test_union_aligns_names_in_first_branch_column_order(database: Database) -> None:
    with database.read_transaction() as session:
        result = session.query("RETURN 1 AS a,2 AS b UNION RETURN 3 AS b,4 AS a")
    assert result.columns == ("a", "b")
    assert result.rows == ({"a": 1, "b": 2}, {"a": 4, "b": 3})


def test_quoted_projection_does_not_bypass_pipeline_parser(database: Database) -> None:
    with database.read_transaction() as session:
        assert session.query("RETURN 'a' AS n UNION RETURN 'b' AS n").rows == (
            {"n": "a"},
            {"n": "b"},
        )
        assert session.query("RETURN 'a'+'b' AS n").rows == ({"n": "ab"},)


def test_with_where_sees_input_scope_and_filters_after_limit(database: Database) -> None:
    with database.read_transaction() as session:
        result = session.query(
            "MATCH (p:Person) WITH p.name AS name ORDER BY name LIMIT 2 "
            "WHERE p.age<30 WITH name RETURN name"
        )
    assert result.rows == ({"name": "Bob"},)


def test_comprehension_can_consume_aggregate_entities(database: Database) -> None:
    with database.read_transaction() as session:
        result = session.query(
            "MATCH (p:Person) WITH p ORDER BY p.name RETURN [x IN collect(p) | x.name] AS names"
        )
    assert result.rows == ({"names": ["Alice", "Bob", "Carol"]},)


def test_cached_subqueries_bind_parameters_per_request(database: Database) -> None:
    query = (
        "CALL () { RETURN CASE WHEN $enabled THEN $value ELSE 0 END AS n } "
        "WITH n WITH n+1 AS n RETURN n"
    )
    with database.read_transaction() as session:
        assert session.query(query, {"enabled": True, "value": 8}).rows == ({"n": 9},)
        assert session.query(query, {"enabled": False, "value": 80}).rows == ({"n": 1},)
        assert session.query(query, {"enabled": True, "value": 18}).rows == ({"n": 19},)


def test_branches_and_subqueries_share_one_captured_state(
    database: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    graph = database._app.use_cases.graph
    original = graph.load_state
    captured = []

    def load_state() -> Any:
        state = original()
        captured.append(state)
        return state

    monkeypatch.setattr(graph, "load_state", load_state)
    query = (
        "MATCH (p:Person) CALL (p) { MATCH (p)-[:KNOWS]->(q) RETURN count(q) AS n } "
        "RETURN p.name AS name,n UNION ALL RETURN 'constant' AS name,0 AS n"
    )
    assert len(database._app.use_cases.cypher.execute(query).rows) == 4
    assert len(captured) == 1
    captured.clear()
    assert database._app.use_cases.cypher.execute(
        "CALL () { RETURN coalesce(null,1) AS n } RETURN n UNION RETURN 2 AS n"
    ).rows == ({"n": 1}, {"n": 2})
    assert captured == []


@pytest.mark.parametrize(
    "query",
    [
        "RETURN 1 AS a UNION RETURN 2 AS b",
        "RETURN 1 AS a UNION RETURN 2 AS a,3 AS b",
        "RETURN 1 AS a UNION ALL RETURN 2 AS a UNION RETURN 3 AS a",
    ],
)
def test_union_rejects_incompatible_branches(database: Database, query: str) -> None:
    with database.read_transaction() as session, pytest.raises(QueryError):
        session.query(query)


@pytest.mark.parametrize("imports", ["(p)", "(*)", ""])
def test_correlated_call_imports(database: Database, imports: str) -> None:
    legacy = "WITH p " if imports == "" else ""
    with database.read_transaction() as session:
        result = session.query(
            f"MATCH (p:Person) CALL {imports} {{ {legacy}"
            "MATCH (p)-[:KNOWS]->(q) RETURN q.name AS friend } "
            "RETURN p.name AS person,friend"
        )
    assert result.rows == ({"person": "Alice", "friend": "Bob"},)


def test_uncorrelated_call_and_union_compose(database: Database) -> None:
    with database.read_transaction() as session:
        result = session.query(
            "WITH 10 AS base CALL () { RETURN 1 AS n UNION ALL RETURN 2 AS n } "
            "WITH base+n AS total RETURN total ORDER BY total"
        )
    assert result.rows == ({"total": 11}, {"total": 12})


def test_call_empty_aggregation_keeps_explicit_global_import(database: Database) -> None:
    with database.read_transaction() as session:
        result = session.query(
            "MATCH (p:Person) CALL (p) { MATCH (q:Missing) "
            "WITH count(*) AS n RETURN p.name AS name,n } "
            "RETURN name,n ORDER BY name"
        )
    assert result.rows == tuple({"name": name, "n": 0} for name in ["Alice", "Bob", "Carol"])


def test_correlated_properties_and_predicates_are_evaluated_per_row(database: Database) -> None:
    with database.read_transaction() as session:
        result = session.query(
            "MATCH (p:Person) CALL (p) { MATCH (q:Person {age:p.age}) "
            "WHERE q.name<>p.name RETURN count(q) AS peers } "
            "RETURN p.name AS name,peers ORDER BY name"
        )
    assert result.rows == (
        {"name": "Alice", "peers": 1},
        {"name": "Bob", "peers": 0},
        {"name": "Carol", "peers": 1},
    )


def test_optional_call_and_nested_exists(database: Database) -> None:
    with database.read_transaction() as session:
        result = session.query(
            "MATCH (p:Person) OPTIONAL CALL (p) { "
            "MATCH (p)-[:KNOWS]->(q) WHERE NOT EXISTS { MATCH (q)-[:KNOWS]->() } "
            "RETURN q.name AS friend } RETURN p.name AS name,friend ORDER BY name"
        )
    assert result.rows == (
        {"name": "Alice", "friend": "Bob"},
        {"name": "Bob", "friend": None},
        {"name": "Carol", "friend": None},
    )


@pytest.mark.parametrize(
    "body",
    [
        "MATCH (p)-[:KNOWS]->(q) WHERE q.age>10",
        "MATCH (p)-[:KNOWS]->(q) WITH q WHERE q.age>10",
        "MATCH (p)-[:KNOWS]->(q) RETURN q.name AS name",
        "CALL (p) { MATCH (p)-[:KNOWS]->(q) RETURN q } RETURN q.name AS name",
    ],
)
def test_exists_query_forms(database: Database, body: str) -> None:
    with database.read_transaction() as session:
        result = session.query(f"MATCH (p:Person) WHERE EXISTS {{ {body} }} RETURN p.name AS name")
    assert result.rows == ({"name": "Alice"},)


@pytest.mark.parametrize(
    "query",
    [
        "MATCH (p:Person) CALL { RETURN p.name AS name } RETURN name",
        "MATCH (p:Person) CALL (unknown) { RETURN 1 AS x } RETURN x",
        "MATCH (p:Person) CALL (p) { RETURN p AS p } RETURN p",
        "MATCH (p:Person) CALL (p) { WITH 1 AS p RETURN p AS x } RETURN x",
        "MATCH (p:Person) CALL { WITH p AS q RETURN q } RETURN q",
    ],
)
def test_subquery_scope_errors_are_detected(database: Database, query: str) -> None:
    with database.read_transaction() as session, pytest.raises(QueryError):
        session.query(query)


@pytest.mark.parametrize(
    "query",
    [
        "CALL { CREATE (:Unexpected) RETURN 1 AS n } RETURN n",
        "MATCH (p:Person) WHERE EXISTS { CREATE (:Unexpected) RETURN 1 } RETURN p",
    ],
)
def test_nested_writes_are_classified_and_rejected_before_execution(
    database: Database, query: str
) -> None:
    assert query_requires_write(query)
    with database.read_transaction() as session, pytest.raises(TransactionError):
        session.query(query)
    with database.write_transaction() as session:
        with pytest.raises(QueryError) as error:
            session.query(query)
        assert (error.value.category, error.value.phase) == ("UnsupportedFeature", "compile")
        assert session.query("MATCH (p:Unexpected) RETURN count(*) AS n").rows == ({"n": 0},)


def test_pipeline_reads_transaction_overlay_without_publishing(database: Database) -> None:
    tx = GraphTransactionUseCase(database._app.use_cases.graph)
    cypher = CypherQueryUseCase(tx)
    cypher.execute("CREATE (:Person {name:'Private',age:99})")
    result = cypher.execute("MATCH (p:Person) WITH p WITH p.name AS name RETURN name ORDER BY name")
    assert {r["name"] for r in result.rows} == {"Alice", "Bob", "Carol", "Private"}
    tx.rollback()
    with database.read_transaction() as session:
        assert session.query("MATCH (p:Person) RETURN count(*) AS n").rows == ({"n": 3},)


@pytest.mark.parametrize(
    ("literal", "value"),
    [
        (".5", 0.5),
        ("1E-2", 0.01),
        ("-.1e9", -1e8),
        ("0x7fffffffffffffff", 2**63 - 1),
        ("- 0x8000000000000000", -(2**63)),
        ("-0o1000000000000000000000", -(2**63)),
        ("[0x10,{value:0o10},.5]", [16, {"value": 8}, 0.5]),
    ],
)
def test_opencypher_numeric_literals(database: Database, literal: str, value: Any) -> None:
    with database.read_transaction() as session:
        assert session.query(f"RETURN {literal} AS x").rows == ({"x": value},)


@pytest.mark.parametrize(
    ("literal", "detail"),
    [
        ("0x8000000000000000", "IntegerOverflow"),
        ("-0o1000000000000000000001", "IntegerOverflow"),
        ("1.34E999", "FloatingPointOverflow"),
        ("0x", "InvalidNumberLiteral"),
        ("0x1A2b3j4", "InvalidNumberLiteral"),
        ("123h54", "InvalidNumberLiteral"),
    ],
)
def test_opencypher_invalid_numeric_literal_errors(
    database: Database, literal: str, detail: str
) -> None:
    with database.read_transaction() as session, pytest.raises(QueryError) as error:
        session.query(f"RETURN {literal} AS x")
    assert (error.value.category, error.value.phase, error.value.detail) == (
        "SyntaxError",
        "compile",
        detail,
    )
