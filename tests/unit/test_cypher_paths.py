"""Regression tests for path values in Cypher reads and writes."""

from pathlib import Path

import pytest

from grapharrow import Database
from grapharrow.core.errors import QueryError


def test_read_path_preserves_order_and_relationship_direction(tmp_path: Path) -> None:
    with Database(tmp_path / "db").write_transaction() as session:
        session.query("CREATE (:A)-[:R]->(:B)<-[:S]-(:C)")
        row = session.query(
            "MATCH p = (:A)-[:R]->(:B)<-[:S]-(:C) "
            "WITH p WHERE length(p)=2 RETURN p, nodes(p) AS ns, relationships(p) AS rs"
        ).rows[0]
    assert row["p"] == {"nodes": row["ns"], "relationships": row["rs"]}
    assert [node["labels"] for node in row["ns"]] == [["A"], ["B"], ["C"]]
    assert row["rs"][0]["source"] == row["ns"][0]["id"]
    assert row["rs"][1]["source"] == row["ns"][2]["id"]
    assert row["rs"][1]["target"] == row["ns"][1]["id"]


def test_self_loop_path_and_optional_path_binding(tmp_path: Path) -> None:
    with Database(tmp_path / "db").write_transaction() as session:
        session.query("CREATE (n:A)-[:LOOP]->(n)")
        assert session.query("MATCH p = (:A)-[:LOOP]-(:A) RETURN count(p) AS count").rows == (
            {"count": 1},
        )
        assert session.query(
            "OPTIONAL MATCH p = (:A)-[:MISSING]->() RETURN p, length(p) AS n"
        ).rows == ({"p": None, "n": None},)
        assert session.query("MATCH p=(:A) RETURN length(p) AS n").rows == ({"n": 0},)


@pytest.mark.parametrize("verb", ["CREATE", "MERGE"])
def test_named_write_returns_path_with_latest_properties(tmp_path: Path, verb: str) -> None:
    with Database(tmp_path / "db").write_transaction() as session:
        row = session.query(
            f"{verb} p = (a:A)-[:R]->(:B) SET a.flag=true RETURN p,length(p) AS n"
        ).rows[0]
    assert row["n"] == 1
    assert row["p"]["nodes"][0]["properties"] == {"flag": True}
    assert row["p"]["relationships"][0]["type"] == "R"


@pytest.mark.parametrize(
    ("query", "detail"),
    [
        ("MATCH p = (p:A)-[:R]->() RETURN p", "VariableAlreadyBound"),
        ("MATCH (p:A) MATCH p = ()-[:R]->() RETURN p", "VariableAlreadyBound"),
        ("MATCH p = ()-[:R]->(), (p:A) RETURN p", "VariableTypeConflict"),
    ],
)
def test_path_alias_conflict_is_a_compile_error(tmp_path: Path, query: str, detail: str) -> None:
    with Database(tmp_path / "db").read_transaction() as session:
        with pytest.raises(QueryError) as caught:
            session.query(query)
    assert caught.value.phase == "compile"
    assert caught.value.detail == detail


def test_detach_delete_path_removes_nodes_and_incident_edges(tmp_path: Path) -> None:
    db = Database(tmp_path / "db")
    with db.write_transaction() as session:
        session.query("CREATE (a:A)-[:R]->(:B), (a)-[:OTHER]->(:Kept)")
        session.query("MATCH p = (:A)-[:R]->(:B) DETACH DELETE p")
        after = db._app.use_cases.graph.load_state()
        assert [node.labels for node in after.nodes] == [("Kept",)]
        assert after.edges == ()
        session.query("OPTIONAL MATCH p = (:Missing)-[:R]->() DETACH DELETE p")
        assert db._app.use_cases.graph.load_state() == after


def test_path_cannot_be_stored_as_a_property(tmp_path: Path) -> None:
    db = Database(tmp_path / "db")
    graph = db._app.use_cases.graph
    before = graph.load_state()
    with db.write_transaction() as session, pytest.raises(QueryError) as caught:
        session.query("CREATE p=(a:A)-[:R]->(:B) SET a.path=p")
    assert caught.value.detail == "InvalidPropertyType"
    assert graph.load_state() == before


def test_named_path_with_escaped_bindings(tmp_path: Path) -> None:
    with Database(tmp_path / "db").write_transaction() as session:
        session.query("CREATE (:A)-[:R]->(:B)")
        assert session.query(
            "MATCH `my path`=(`my node`:A)-[`my edge`:R]->() "
            "RETURN length(`my path`) AS hops,labels(`my node`) AS labels,type(`my edge`) AS type"
        ).rows == ({"hops": 1, "labels": ["A"], "type": "R"},)


@pytest.mark.parametrize("planner", ["0", "1"])
def test_bounded_path_retains_intermediate_nodes_and_relationship_bindings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, planner: str
) -> None:
    monkeypatch.setenv("GRAPHARROW_DISABLE_CYPHER_PLANNER", planner)
    with Database(tmp_path / "db").write_transaction() as session:
        session.query("CREATE (:A)-[:R {step:1}]->(:B)-[:R {step:2}]->(:C)-[:S]->(:D)")
        rows = session.query(
            "MATCH p=(:A)-[rs:R*1..2]->(dst) RETURN p,rs,length(p) AS n ORDER BY n"
        ).rows
    assert [row["n"] for row in rows] == [1, 2]
    assert [node["labels"] for node in rows[1]["p"]["nodes"]] == [["A"], ["B"], ["C"]]
    assert rows[1]["rs"] == rows[1]["p"]["relationships"]
    assert [edge["properties"]["step"] for edge in rows[1]["rs"]] == [1, 2]


def test_range_can_contribute_zero_hops_and_enforce_endpoint_constraints(tmp_path: Path) -> None:
    with Database(tmp_path / "db").write_transaction() as session:
        session.query("CREATE (:A {key:1})-[:R]->(:B)-[:S]->(:C)")
        rows = session.query(
            "MATCH p=(a:A)-[r:R*0..1]->(b)-[s:S*0..1]->(c) "
            "RETURN p,r,s,id(a)=id(b) AS same,length(p) AS n ORDER BY n"
        ).rows
        zero = session.query(
            "MATCH p=(a:A)-[r:R*0]->(b:A {key:1}) RETURN p,r,id(a)=id(b) AS same"
        ).rows[0]
        assert len(zero["p"]["nodes"]) == 1
        assert zero["p"]["relationships"] == zero["r"] == []
        assert zero["same"] is True
        assert session.query("MATCH p=(:A)-[:R*0]->(:B) RETURN p").rows == ()
    assert [(row["n"], len(row["r"]), len(row["s"]), row["same"]) for row in rows] == [
        (0, 0, 0, True),
        (1, 1, 0, False),
        (2, 1, 1, False),
    ]


def test_bounded_undirected_path_does_not_reuse_edges_or_duplicate_loops(tmp_path: Path) -> None:
    with Database(tmp_path / "db").write_transaction() as session:
        session.query("CREATE (a:A)-[:R]->(a), (:B)-[:R]->(a)")
        rows = session.query(
            "MATCH p=(:A)-[r:R*1..2]-() RETURN length(p) AS n,count(*) AS total ORDER BY n"
        ).rows
        reverse = session.query("MATCH p=(:A)<-[:R*1..2]-(:B) RETURN p ORDER BY length(p)").rows
    assert rows == ({"n": 1, "total": 2}, {"n": 2, "total": 1})
    assert len(reverse) == 2
    assert reverse[1]["p"]["relationships"][1]["source"] == reverse[1]["p"]["nodes"][-1]["id"]


def test_ranged_paths_keep_parallel_edges_and_filter_each_hop(tmp_path: Path) -> None:
    with Database(tmp_path / "db").write_transaction() as session:
        session.query(
            "CREATE (a:A)-[:R {active:true}]->(b:B), "
            "(a)-[:R {active:true}]->(b), (b)-[:R {active:false}]->(:C)"
        )
        assert session.query(
            "MATCH p=(:A)-[:R*..2 {active:true}]->() RETURN count(DISTINCT p) AS paths"
        ).rows == ({"paths": 2},)


@pytest.mark.parametrize("label", ["B", "Missing"])
def test_ranged_path_joins_connected_patterns(tmp_path: Path, label: str) -> None:
    with Database(tmp_path / "db").write_transaction() as session:
        session.query("CREATE (:A)-[:R]->(:B)-[:S]->(:C)")
        rows = session.query(
            "OPTIONAL MATCH (:A)-[:R]->(b), "
            f"p=(b:{label})-[:S*0..1]->() RETURN length(p) AS n ORDER BY n"
        ).rows
    assert rows == (({"n": 0}, {"n": 1}) if label == "B" else ({"n": None},))


def test_named_range_can_read_and_delete_the_write_overlay(tmp_path: Path) -> None:
    db = Database(tmp_path / "db")
    with db.write_transaction() as session:
        rows = session.query(
            "CREATE (:A)-[:R]->(:B)-[:R]->(:C) "
            "WITH 1 AS seed MATCH p=(:A)-[:R*2]->(:C) "
            "WITH p, length(p) AS n DETACH DELETE p RETURN n"
        ).rows
    assert rows == ({"n": 2},)
    assert db._app.use_cases.graph.load_state().nodes == ()
