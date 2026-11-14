"""Regression checks for direction, multiplicity, null properties and write windows."""

from collections import Counter
from pathlib import Path

import pytest

from grapharrow import Database
from grapharrow.core.errors import QueryError
from grapharrow.use_cases import CypherQueryUseCase, GraphTransactionUseCase


@pytest.mark.parametrize("planner", ["0", "1"])
@pytest.mark.parametrize(
    "setup",
    [
        "CREATE (:A)<-[:R]-(:B)",
        "MATCH (a:A) CREATE (a)<-[:R]-(b:B)",
        "MATCH (a:A)-[:SEED]->(b:B) CREATE (a)<-[:R]-(b)",
        "MATCH (a:A) WITH a CREATE (a)<-[:R]-(b:B)",
    ],
)
def test_created_incoming_relationship_survives_reopen(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, planner: str, setup: str
) -> None:
    monkeypatch.setenv("GRAPHARROW_DISABLE_CYPHER_PLANNER", planner)
    db = Database(tmp_path / "db")
    with db.write_transaction() as session:
        if setup.startswith("MATCH"):
            session.query("CREATE (a:A)-[:SEED]->(b:B)" if ":SEED" in setup else "CREATE (:A),(:B)")
        session.query(setup)
    reopened = Database(tmp_path / "db")
    state = reopened._app.use_cases.graph.load_state()
    nodes = {n.node_id: n for n in state.nodes}
    edges = [edge for edge in state.edges if edge.rel_type == "R"]
    assert len(edges) == 1
    edge = edges[0]
    assert nodes[edge.src_id].labels == ("B",) and nodes[edge.dst_id].labels == ("A",)
    with reopened.read_transaction() as session:
        assert len(session.query("MATCH (a:A)<-[:R]-(b:B) RETURN a,b").rows) == 1


@pytest.mark.parametrize(
    "query",
    [
        "CREATE (:Scratch),(:A)-[:R]-(:B)",
        "MATCH (a:A) CREATE (a)-[:R]-(:B)",
        "MATCH (a:A) WITH a CREATE (a)-[:R]-(:B)",
    ],
)
def test_undirected_create_rejected_before_publication(tmp_path: Path, query: str) -> None:
    db = Database(tmp_path / "db")
    with db.write_transaction() as session:
        session.query("CREATE (:A)")
        before = db._app.use_cases.graph.load_state()
        with pytest.raises(QueryError, match="direction|directed"):
            session.query(query)
    after = db._app.use_cases.graph.load_state()
    assert (
        after.snapshot == before.snapshot
        and after.nodes == before.nodes
        and after.edges == before.edges
    )


@pytest.mark.parametrize("planner", ["0", "1"])
def test_undirected_self_loop_emitted_once_without_deduplicating_other_matches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, planner: str
) -> None:
    monkeypatch.setenv("GRAPHARROW_DISABLE_CYPHER_PLANNER", planner)
    db = Database(tmp_path / "db")
    with db.write_transaction() as s:
        s.query("CREATE (a:A)-[:R]->(a)")
        s.query("MATCH (a:A) CREATE (a)-[:R]->(a)")
        s.query("MATCH (a:A) CREATE (a)-[:R]->(b:B)")
        rows = s.query("MATCH (a)-[r:R]-(b) RETURN id(a) AS a,id(r) AS r,id(b) AS b").rows
        counts = Counter(row["r"] for row in rows)
        state = db._app.use_cases.graph.load_state()
        assert counts == {e.edge_id: 1 if e.src_id == e.dst_id else 2 for e in state.edges}
        assert len(s.query("MATCH (a)-[r:R]->(a) RETURN r").rows) == 2
        assert len(s.query("MATCH (a)<-[r:R]-(a) RETURN r").rows) == 2
        duplicated = s.query("UNWIND [1,1] AS x MATCH (a)-[r:R]-(a) RETURN r").rows
        assert len(duplicated) == 4


@pytest.mark.parametrize("planner", ["0", "1"])
def test_connected_undirected_pattern_preserves_loops_and_both_directions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, planner: str
) -> None:
    monkeypatch.setenv("GRAPHARROW_DISABLE_CYPHER_PLANNER", planner)
    db = Database(tmp_path / "db")
    with db.write_transaction() as session:
        session.query("CREATE (a:A)-[:R]->(a)")
        session.query("MATCH (a:A) CREATE (a)-[:R]->(b:B)")
        session.query("MATCH (a:A) CREATE (a)<-[:R]-(b:B)")
        single = session.query("MATCH (a:A)-[r:R]-(b) RETURN id(r) AS r").rows
        connected = session.query("MATCH (a:A), (a)-[r:R]-(b) RETURN id(r) AS r").rows
        assert Counter(row["r"] for row in connected) == Counter(row["r"] for row in single)
        assert len(connected) == 3


@pytest.mark.parametrize("update", ["SET n.p=null", "SET n += {p:null,q:2}"])
def test_cypher_null_removes_property_from_persisted_records(tmp_path: Path, update: str) -> None:
    db = Database(tmp_path / "db")
    with db.write_transaction() as s:
        s.query("CREATE (:A {p:42})")
        before = db.load_snapshot().snapshot_id.value
        s.query("MATCH (n:A) " + update + " RETURN n")
    state = Database(tmp_path / "db")._app.use_cases.graph.load_state()
    assert "p" not in state.nodes[0].properties
    assert state.snapshot.snapshot_id.value == before + 1


@pytest.mark.parametrize(
    "query",
    [
        "CREATE (:A {p:null,keep:[null,1],m:{x:null}})",
        "MERGE (n:A {key:1}) ON CREATE SET n.p=null",
        "MATCH (a:A) MERGE (a)-[r:R]->(:B) ON CREATE SET r.p=null",
    ],
)
def test_new_cypher_entities_do_not_store_null_properties(tmp_path: Path, query: str) -> None:
    db = Database(tmp_path / "db")
    with db.write_transaction() as s:
        if query.startswith("MATCH"):
            s.query("CREATE (:A)")
        s.query(query)
    state = Database(tmp_path / "db")._app.use_cases.graph.load_state()
    assert all("p" not in n.properties for n in state.nodes)
    assert all("p" not in r.properties for r in state.edges)
    if query.startswith("CREATE"):
        assert state.nodes[0].properties == {"keep": [None, 1], "m": {"x": None}}


@pytest.mark.parametrize("clause", ["LIMIT 0", "SKIP 1", "SKIP 2 LIMIT 3"])
@pytest.mark.parametrize("verb", ["CREATE", "MERGE"])
def test_standalone_write_result_window_preserves_mutation(
    tmp_path: Path, clause: str, verb: str
) -> None:
    db = Database(tmp_path / "db")
    with db.write_transaction() as s:
        result = s.query(f"{verb} (n:A {{key:1}}) RETURN n {clause}")
    assert result.columns == ("n",) and result.rows == ()
    assert len(db._app.use_cases.graph.load_state().nodes) == 1


@pytest.mark.parametrize("limit", [-1, True, 1.5, None])
def test_invalid_write_limit_cannot_publish(tmp_path: Path, limit: object) -> None:
    db = Database(tmp_path / "db")
    before = db._app.use_cases.graph.load_state()
    with db.write_transaction() as s:
        with pytest.raises(QueryError, match="integer"):
            s.query("CREATE (n:A) RETURN n LIMIT $bound", {"bound": limit})
    assert db._app.use_cases.graph.load_state().snapshot == before.snapshot


def test_null_removal_is_private_until_bolt_transaction_commit(tmp_path: Path) -> None:
    db = Database(tmp_path / "db")
    with db.write_transaction() as s:
        s.query("CREATE (:A {p:42})")
    graph = db._app.use_cases.graph
    tx = GraphTransactionUseCase(graph)
    cypher = CypherQueryUseCase(tx)
    cypher.execute("MATCH (n:A) SET n.p=null RETURN n")
    assert "p" not in tx.load_state().nodes[0].properties
    assert graph.load_state().nodes[0].properties["p"] == 42
    tx.rollback()
    tx = GraphTransactionUseCase(graph)
    CypherQueryUseCase(tx).execute("MATCH (n:A) SET n.p=null")
    tx.commit()
    assert (
        "p" not in Database(tmp_path / "db")._app.use_cases.graph.load_state().nodes[0].properties
    )


@pytest.mark.parametrize(
    "query",
    [
        "MATCH (a:A)-[:SEED]->(b:B) MERGE (a)<-[r:R]-(b) RETURN r",
        'MATCH (a:A) MERGE (a)<-[r:R]-(b:B {name:"b"}) RETURN r',
    ],
)
def test_incoming_merge_is_idempotent(tmp_path: Path, query: str) -> None:
    db = Database(tmp_path / "db")
    with db.write_transaction() as s:
        s.query('CREATE (a:A)-[:SEED]->(b:B {name:"b"})')
        s.query(query)
        s.query(query)
    state = Database(tmp_path / "db")._app.use_cases.graph.load_state()
    edges = [e for e in state.edges if e.rel_type == "R"]
    assert len(edges) == 1
    nodes = {n.node_id: n for n in state.nodes}
    assert nodes[edges[0].src_id].labels == ("B",)
    assert nodes[edges[0].dst_id].labels == ("A",)


@pytest.mark.parametrize(
    "query",
    [
        "MATCH (a:A)-[:SEED]->(b:B) MERGE (a)-[r:R]-(b) RETURN r",
        'MATCH (a:A) MERGE (a)-[r:R]-(b:B {name:"b"}) RETURN r',
    ],
)
def test_undirected_merge_reuses_an_incoming_relationship(tmp_path: Path, query: str) -> None:
    db = Database(tmp_path / "db")
    with db.write_transaction() as s:
        s.query('CREATE (a:A)-[:SEED]->(b:B {name:"b"})')
        s.query("MATCH (a:A)-[:SEED]->(b:B) CREATE (a)<-[:R]-(b)")
        s.query(query)
        s.query(query)
    assert len([e for e in db._app.use_cases.graph.load_state().edges if e.rel_type == "R"]) == 1


@pytest.mark.parametrize("projection", ["count(*) AS total", "id(r) AS rid", "a"])
@pytest.mark.parametrize("planner", ["0", "1"])
def test_self_loop_scalar_aggregate_and_optional_matches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, projection: str, planner: str
) -> None:
    monkeypatch.setenv("GRAPHARROW_DISABLE_CYPHER_PLANNER", planner)
    db = Database(tmp_path / "db")
    with db.write_transaction() as s:
        s.query("CREATE (a:A)-[:R]->(a)")
        assert len(s.query("MATCH (a)-[r:R|OTHER]-(a) RETURN " + projection).rows) == 1
        result = s.query("MATCH (a:A) OPTIONAL MATCH (a)-[r:R]-(b) RETURN a,r,b")
        assert len(result.rows) == 1
        if projection.startswith("count"):
            assert s.query("MATCH (a)-[r:R]-(b) RETURN " + projection).rows == ({"total": 1},)


@pytest.mark.parametrize(
    "clause",
    [
        "SET r.p=null",
        "SET r += {p:null,q:2}",
    ],
)
def test_relationship_null_removal_and_native_null_contract(tmp_path: Path, clause: str) -> None:
    db = Database(tmp_path / "db")
    with db.write_transaction() as s:
        s.add_node(labels=["Native"], properties={"p": None})
        s.query("CREATE (:A)-[:R {p:42}]->(:B)")
        s.query("MATCH ()-[r:R]->() " + clause + " RETURN r")
    state = Database(tmp_path / "db")._app.use_cases.graph.load_state()
    assert "p" not in state.edges[0].properties
    assert next(n for n in state.nodes if n.labels == ("Native",)).properties == {"p": None}


@pytest.mark.parametrize(
    "query",
    [
        "MERGE (n:A {key:1}) ON MATCH SET n.p=null RETURN n",
        "MATCH (n:A) WITH n SET n.p=null RETURN n",
        "UNWIND [{key:1}] AS row WITH row MERGE (n:A {key:row.key}) ON MATCH SET n.p=null RETURN n",
    ],
)
def test_merge_and_with_null_removal(tmp_path: Path, query: str) -> None:
    db = Database(tmp_path / "db")
    with db.write_transaction() as s:
        s.query("CREATE (:A {key:1,p:42})")
        result = s.query(query)
        assert result.rows[0]["n"]["properties"] == {"key": 1}
    assert db._app.use_cases.graph.load_state().nodes[0].properties == {"key": 1}


def test_set_null_order_and_nested_null_values(tmp_path: Path) -> None:
    db = Database(tmp_path / "db")
    with db.write_transaction() as s:
        s.query("CREATE (:A {p:42})")
        s.query("MATCH (n:A) SET n.p=null,n.q=2,n.p=3,n.m={x:null},n.l=[null,1]")
    assert db._app.use_cases.graph.load_state().nodes[0].properties == {
        "p": 3,
        "q": 2,
        "m": {"x": None},
        "l": [None, 1],
    }


@pytest.mark.parametrize("verb", ["CREATE", "MERGE"])
def test_write_parameterized_windows_and_aggregate_limit(tmp_path: Path, verb: str) -> None:
    db = Database(tmp_path / "db")
    with db.write_transaction() as s:
        result = s.query(
            f"{verb} (n:A {{key:1}}) RETURN n.key AS value "
            "ORDER BY value DESC SKIP $off LIMIT $bound",
            {"off": 0, "bound": 1},
        )
        assert result.rows == ({"value": 1},)
        result = s.query(f"{verb} (n:B {{key:2}}) RETURN count(*) AS total LIMIT 0")
        assert result.columns == ("total",) and result.rows == ()
    assert len(db._app.use_cases.graph.load_state().nodes) == 2


def test_merge_distinct_happens_before_result_window(tmp_path: Path) -> None:
    db = Database(tmp_path / "db")
    with db.write_transaction() as s:
        s.query('CREATE (:A {tag:"x",p:1}),(:A {tag:"x",p:1}),(:A {tag:"x",p:2})')
        result = s.query('MERGE (n:A {tag:"x"}) RETURN DISTINCT n.p AS p ORDER BY p SKIP 1 LIMIT 1')
        assert result.rows == ({"p": 2},)


def test_with_window_and_return_window_apply_on_opposite_sides_of_write(tmp_path: Path) -> None:
    db = Database(tmp_path / "db")
    with db.write_transaction() as s:
        result = s.query(
            "UNWIND [3,1,2] AS value WITH value ORDER BY value LIMIT 2 "
            "CREATE (n:A {p:value}) RETURN n.p ORDER BY n.p DESC LIMIT 0"
        )
        assert result.rows == ()
    assert sorted(n.properties["p"] for n in db._app.use_cases.graph.load_state().nodes) == [1, 2]
