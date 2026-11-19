"""Relationship identities survive ordering and updates after WITH/MATCH."""

from pathlib import Path

import pytest

from grapharrow.app import GraphArrowApplication, build_application
from grapharrow.core.errors import QueryError
from grapharrow.use_cases import CypherQueryUseCase, GraphTransactionUseCase


def graph(path: Path) -> GraphArrowApplication:
    app = build_application(path)
    app.use_cases.cypher.execute(
        "CREATE (a:Vertex)-[:LINK {writer:'seed',witness:'seed',value:30}]->(b:Vertex) "
        "WITH a,b CREATE (a)-[:LINK {writer:'seed',witness:'seed',value:10}]->(b) "
        "WITH a,b CREATE (b)-[:LINK {writer:'seed',witness:'seed',value:20}]->(a)"
    )
    return app


@pytest.mark.parametrize(
    ("projection", "window", "expected"),
    [
        ("id(r) AS edge_id", "ORDER BY id(r)", [{"edge_id": 1}, {"edge_id": 2}, {"edge_id": 3}]),
        ("id(r) AS edge_id", "ORDER BY id(r) DESC SKIP 1 LIMIT 1", [{"edge_id": 2}]),
        ("r.value AS value", "ORDER BY id(r) DESC", [{"value": 20}, {"value": 10}, {"value": 30}]),
        ("id(r) AS edge_id", "ORDER BY edge_id DESC LIMIT 2", [{"edge_id": 3}, {"edge_id": 2}]),
    ],
)
def test_relationship_identity_ordering(
    tmp_path: Path, projection: str, window: str, expected: list[dict[str, int]]
) -> None:
    app = graph(tmp_path)
    result = app.use_cases.cypher.execute(f"MATCH ()-[r:LINK]->() RETURN {projection} {window}")
    assert list(result.rows) == expected


def test_relationship_identity_ordering_after_with_and_optional_null(tmp_path: Path) -> None:
    app = graph(tmp_path)
    app.use_cases.cypher.execute("CREATE (:Vertex)")
    result = app.use_cases.cypher.execute(
        "MATCH (a:Vertex) OPTIONAL MATCH (a)-[r:LINK]->() "
        "WITH r RETURN id(r) AS edge_id ORDER BY id(r) DESC"
    )
    assert result.rows == ({"edge_id": None}, {"edge_id": 3}, {"edge_id": 2}, {"edge_id": 1})


UPDATE = (
    "MATCH ()-[other:LINK]->() WHERE id(other)=2 "
    "WITH other.writer AS dependency "
    "MATCH ()-[r:LINK]->() WHERE id(r)=$id AND dependency='seed' "
    "SET r.writer='candidate', r.witness='candidate' "
)


@pytest.mark.parametrize("edge_id", [1, 3])
def test_with_match_set_updates_the_bound_relationship_and_persists(
    tmp_path: Path, edge_id: int
) -> None:
    app = graph(tmp_path)
    initial = app.use_cases.graph.capture_snapshot()
    result = app.use_cases.cypher.execute(UPDATE + "RETURN r.writer AS stamp", {"id": edge_id})
    assert result.rows == ({"stamp": "candidate"},)
    state = build_application(tmp_path).use_cases.graph.load_state()
    assert state.snapshot.snapshot_id.value == initial.snapshot_id.value + 1
    assert [edge.properties["writer"] for edge in state.edges] == [
        "candidate" if edge.edge_id == edge_id else "seed" for edge in state.edges
    ]
    assert all(edge.properties["writer"] == edge.properties["witness"] for edge in state.edges)
    assert (
        app.use_cases.graph.load_state(snapshot=initial).edges[edge_id - 1].properties["writer"]
        == "seed"
    )
    assert app.use_cases.admin.verify(verify_checksums=True).ok


def test_with_match_set_handles_multiple_relationships_and_result_windows(tmp_path: Path) -> None:
    app = graph(tmp_path)
    query = UPDATE.replace("id(r)=$id AND ", "")
    result = app.use_cases.cypher.execute(
        query + "RETURN id(r) AS edge_id ORDER BY id(r) DESC LIMIT 1"
    )
    assert result.rows == ({"edge_id": 3},)
    assert [edge.properties["writer"] for edge in app.use_cases.graph.load_state().edges] == [
        "candidate"
    ] * 3


def test_with_match_set_empty_match_has_no_side_effects(tmp_path: Path) -> None:
    app = graph(tmp_path)
    initial = app.use_cases.graph.capture_snapshot()
    result = app.use_cases.cypher.execute(UPDATE + "RETURN r.writer AS stamp", {"id": 99})
    assert result.rows == ()
    assert app.use_cases.graph.capture_snapshot() == initial


def test_with_match_set_rolls_back_when_result_evaluation_fails(tmp_path: Path) -> None:
    app = graph(tmp_path)
    initial = app.use_cases.graph.capture_snapshot()
    with pytest.raises(QueryError) as error:
        app.use_cases.cypher.execute(UPDATE + "RETURN 1 / 0 AS invalid", {"id": 1})
    assert error.value.category == "ArithmeticError"
    assert app.use_cases.graph.capture_snapshot() == initial
    assert [edge.properties["writer"] for edge in app.use_cases.graph.load_state().edges] == [
        "seed"
    ] * 3


def test_with_match_set_respects_explicit_transaction_rollback(tmp_path: Path) -> None:
    app = graph(tmp_path)
    initial = app.use_cases.graph.capture_snapshot()
    transaction = GraphTransactionUseCase(app.use_cases.graph)
    executor = CypherQueryUseCase(transaction)  # type: ignore[arg-type]
    result = executor.execute(UPDATE + "RETURN r.writer AS stamp", {"id": 1})
    assert result.rows == ({"stamp": "candidate"},)
    assert transaction.load_state().edges[0].properties["writer"] == "candidate"
    assert app.use_cases.graph.capture_snapshot() == initial
    transaction.rollback()
    assert app.use_cases.graph.load_state().edges[0].properties["writer"] == "seed"


def test_relationship_identity_is_separate_from_a_stored_id_property(tmp_path: Path) -> None:
    app = build_application(tmp_path)
    query = app.use_cases.cypher.execute
    query(
        "CREATE (a:Vertex)-[:LINK {id:999,writer:'seed'}]->(b:Vertex) "
        "WITH a,b CREATE (a)-[:LINK {id:4,writer:'seed'}]->(b)"
    )
    assert query("MATCH ()-[r:LINK]->() RETURN id(r) AS edge_id ORDER BY id(r)").rows == (
        {"edge_id": 1},
        {"edge_id": 2},
    )
    assert query(
        "WITH 1 AS x MATCH ()-[r:LINK]->() WHERE id(r)=1 "
        "SET r.writer='changed' RETURN id(r) AS edge_id"
    ).rows == ({"edge_id": 1},)
    edges = app.use_cases.graph.load_state().edges
    assert [(edge.edge_id, edge.properties) for edge in edges] == [
        (1, {"id": 999, "writer": "changed"}),
        (2, {"id": 4, "writer": "seed"}),
    ]
