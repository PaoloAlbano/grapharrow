"""Regression tests for matching and creating complete MERGE patterns."""

from pathlib import Path

import pytest

from grapharrow import Database
from grapharrow.core.errors import QueryError


def test_merge_partial_pattern_creates_unbound_nodes_and_reuses_bound_endpoint(
    tmp_path: Path,
) -> None:
    db = Database(tmp_path / "db")
    with db.write_transaction() as session:
        session.query("CREATE (:A)-[:R]->(:B)")
        query = (
            "MATCH (a:A) MERGE p = (a)-[:R]->(b:B)<-[:S]-(c:C) "
            "ON CREATE SET b.created=true ON MATCH SET b.matched=true RETURN length(p) AS n"
        )
        assert session.query(query).rows == ({"n": 2},)
        assert session.query(query).rows == ({"n": 2},)
        assert session.query(
            "MATCH (b:B) WHERE b.created=true RETURN b.matched AS matched"
        ).rows == ({"matched": True},)
    state = db._app.use_cases.graph.load_state()
    assert len(state.nodes) == 4
    assert len(state.edges) == 3


def test_merge_multiple_input_rows_observe_previous_matches_and_writes(tmp_path: Path) -> None:
    db = Database(tmp_path / "db")
    with db.write_transaction() as session:
        result = session.query(
            "UNWIND [1,1,2] AS x MERGE p = (a:A {x:x})-[:R]->(:B)-[:S]->(:C) "
            "ON CREATE SET a.n=1 ON MATCH SET a.n=a.n+1 "
            "RETURN length(p) AS hops,a.x AS x,a.n AS n ORDER BY x"
        )
        assert result.rows == (
            {"hops": 2, "x": 1, "n": 2},
            {"hops": 2, "x": 1, "n": 2},
            {"hops": 2, "x": 2, "n": 1},
        )
    state = db._app.use_cases.graph.load_state()
    assert len(state.nodes) == 6 and len(state.edges) == 4


def test_merge_does_not_match_a_relationship_twice_in_one_path(tmp_path: Path) -> None:
    db = Database(tmp_path / "db")
    with db.write_transaction() as session:
        session.query("CREATE (:A)-[:R]->(:B)")
        session.query("MATCH (a:A) MERGE (a)-[:R]-(b:B)-[:R]-(a)")
    state = db._app.use_cases.graph.load_state()
    assert len(state.nodes) == 3 and len(state.edges) == 3


def test_multihop_merge_conflict_rolls_back_earlier_clauses(tmp_path: Path) -> None:
    from grapharrow.domain import (
        CatalogVersion,
        ConstraintDefinition,
        LabelDefinition,
        LabelId,
        VersionedCatalog,
    )

    db = Database(tmp_path / "db")
    db.initialize()
    db.write_catalog(
        VersionedCatalog(
            version=CatalogVersion(2),
            labels=(LabelDefinition(LabelId(1), "Unique"),),
            constraints=(
                ConstraintDefinition(
                    kind="unique", owner_kind="label", owner_name="Unique", property_name="x"
                ),
            ),
        )
    )
    graph = db._app.use_cases.graph
    with db.write_transaction() as session:
        session.query("CREATE (:Unique {x:1})-[:R]->(:B)")
        before = graph.load_state()
        with pytest.raises(QueryError) as caught:
            session.query("CREATE (:Scratch) MERGE (:Unique {x:1})-[:R]->(:B)-[:S]->(:C)")
        assert caught.value.detail == "ConstraintViolation"
        assert graph.load_state() == before
