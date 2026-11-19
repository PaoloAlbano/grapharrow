"""User properties named id remain separate from graph entity identities."""

from pathlib import Path

import pytest

from grapharrow.app import GraphArrowApplication, build_application
from grapharrow.core.errors import QueryError
from grapharrow.use_cases import CypherQueryUseCase, GraphTransactionUseCase


@pytest.fixture(params=[False, True], ids=["planner", "fallback"])
def app(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> GraphArrowApplication:
    if request.param:
        monkeypatch.setenv("GRAPHARROW_DISABLE_CYPHER_PLANNER", "1")
    else:
        monkeypatch.delenv("GRAPHARROW_DISABLE_CYPHER_PLANNER", raising=False)
    application = build_application(tmp_path)
    application.use_cases.cypher.execute(
        "CREATE (a:IdStart {id:700})-[:IdLink {id:999,name:'first'}]->(b:IdEnd {id:800}) "
        "WITH a,b CREATE (a)-[:IdLink {id:4,name:'second'}]->(b) "
        "WITH a,b CREATE (b)-[:IdLink {name:'missing'}]->(a)"
    )
    return application


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        (
            "MATCH ()-[r:IdLink]->() RETURN id(r) AS identity,r.id AS property ORDER BY id(r)",
            [
                {"identity": 1, "property": 999},
                {"identity": 2, "property": 4},
                {"identity": 3, "property": None},
            ],
        ),
        (
            "MATCH ()-[r:IdLink]->() WHERE r.id=999 RETURN id(r) AS identity,r.id AS property",
            [{"identity": 1, "property": 999}],
        ),
        (
            "MATCH ()-[r:IdLink {id:4}]->() RETURN id(r) AS identity,r.id AS property",
            [{"identity": 2, "property": 4}],
        ),
        (
            "MATCH ()-[r:IdLink]->() WITH r AS edge "
            "RETURN id(edge) AS identity,edge.id AS property ORDER BY edge.id SKIP 1 LIMIT 1",
            [{"identity": 1, "property": 999}],
        ),
        (
            "MATCH ()-[r:IdLink]->() WITH r.id AS property,id(r) AS identity "
            "RETURN identity,property ORDER BY identity",
            [
                {"identity": 1, "property": 999},
                {"identity": 2, "property": 4},
                {"identity": 3, "property": None},
            ],
        ),
        (
            "MATCH ()-[r:IdLink]->() "
            "RETURN count(r.id) AS present,sum(r.id) AS total,count(DISTINCT id(r)) AS entities",
            [{"present": 2, "total": 1003, "entities": 3}],
        ),
        (
            "MATCH ()-[r:IdLink]->() WHERE r.id IS NULL RETURN id(r) AS identity",
            [{"identity": 3}],
        ),
    ],
)
def test_relationship_id_property_queries(
    app: GraphArrowApplication, query: str, expected: list[dict[str, object]]
) -> None:
    assert list(app.use_cases.cypher.execute(query).rows) == expected


def test_full_relationship_and_property_functions_preserve_id(app: GraphArrowApplication) -> None:
    query = app.use_cases.cypher.execute
    edge = query("MATCH ()-[r:IdLink]->() WHERE id(r)=1 RETURN r").rows[0]["r"]
    assert edge["id"] == 1
    assert edge["properties"] == {"id": 999, "name": "first"}
    row = query(
        "MATCH ()-[r:IdLink]->() WHERE id(r)=1 "
        "RETURN r['id'] AS dynamic,properties(r) AS props,keys(r) AS names"
    ).rows[0]
    assert row["dynamic"] == 999
    assert row["props"] == {"id": 999, "name": "first"}
    assert set(row["names"]) == {"id", "name"}


def test_created_relationship_returns_its_id_property(app: GraphArrowApplication) -> None:
    result = app.use_cases.cypher.execute(
        "CREATE ()-[r:CreatedId {id:'external-key'}]->() RETURN id(r) AS identity,r.id AS property"
    )
    assert result.rows == ({"identity": 4, "property": "external-key"},)


@pytest.mark.parametrize(
    "update",
    [
        "MATCH ()-[r:IdLink]->() WHERE id(r)=1 SET r.id=123",
        "MATCH ()-[r:IdLink]->() WHERE id(r)=1 SET r += {id:123}",
        "WITH 123 AS value MATCH ()-[r:IdLink]->() WHERE id(r)=1 SET r.id=value",
        "WITH {id:123} AS values MATCH ()-[r:IdLink]->() WHERE id(r)=1 SET r += values",
        "MATCH ()-[r:IdLink]->() WHERE r.id=999 SET r.id=123",
    ],
)
def test_id_property_updates_preserve_identity_and_snapshots(
    app: GraphArrowApplication, update: str
) -> None:
    before = app.use_cases.graph.capture_snapshot()
    assert app.use_cases.cypher.execute(
        update + " RETURN id(r) AS identity,r.id AS property"
    ).rows == ({"identity": 1, "property": 123},)
    reopened = build_application(app.layout.root)
    state = reopened.use_cases.graph.load_state()
    assert [(edge.edge_id, edge.properties.get("id")) for edge in state.edges] == [
        (1, 123),
        (2, 4),
        (3, None),
    ]
    assert app.use_cases.graph.load_state(snapshot=before).edges[0].properties["id"] == 999
    assert app.use_cases.admin.verify(verify_checksums=True).ok


@pytest.mark.parametrize("update", ["SET r.id=NULL", "REMOVE r.id"])
def test_removing_id_property_preserves_relationship(
    app: GraphArrowApplication, update: str
) -> None:
    result = app.use_cases.cypher.execute(
        "MATCH ()-[r:IdLink]->() WHERE id(r)=1 "
        + update
        + " RETURN id(r) AS identity,r.id AS property"
    )
    assert result.rows == ({"identity": 1, "property": None},)
    assert "id" not in app.use_cases.graph.load_state().edges[0].properties


def test_id_property_updates_roll_back(app: GraphArrowApplication) -> None:
    before = app.use_cases.graph.capture_snapshot()
    with pytest.raises(QueryError):
        app.use_cases.cypher.execute(
            "MATCH ()-[r:IdLink]->() WHERE id(r)=1 SET r.id=123 RETURN 1/0 AS invalid"
        )
    assert app.use_cases.graph.capture_snapshot() == before
    transaction = GraphTransactionUseCase(app.use_cases.graph)
    executor = CypherQueryUseCase(transaction)  # type: ignore[arg-type]
    assert executor.execute(
        "MATCH ()-[r:IdLink]->() WHERE id(r)=1 SET r.id=123 "
        "RETURN id(r) AS identity,r.id AS property"
    ).rows == ({"identity": 1, "property": 123},)
    assert transaction.load_state().edges[0].properties["id"] == 123
    transaction.rollback()
    assert app.use_cases.graph.load_state().edges[0].properties["id"] == 999
    assert app.use_cases.graph.capture_snapshot() == before


def test_merge_actions_and_delete_use_relationship_identity(app: GraphArrowApplication) -> None:
    query = app.use_cases.cypher.execute
    assert query(
        "MATCH (a:IdStart),(b:IdEnd) MERGE (a)-[r:IdLink {name:'first'}]->(b) "
        "ON MATCH SET r.id=321 RETURN id(r) AS identity,r.id AS property"
    ).rows == ({"identity": 1, "property": 321},)
    assert query(
        "MATCH (a:IdStart),(b:IdEnd) MERGE (a)-[r:IdLink {name:'created'}]->(b) "
        "ON CREATE SET r.id=654 RETURN id(r) AS identity,r.id AS property"
    ).rows == ({"identity": 4, "property": 654},)
    query("MATCH ()-[r:IdLink]->() WHERE r.id=321 DELETE r")
    assert [edge.edge_id for edge in app.use_cases.graph.load_state().edges] == [2, 3, 4]


def test_optional_id_property_and_node_ids(app: GraphArrowApplication) -> None:
    app.use_cases.cypher.execute("CREATE (:IdStart {id:900})")
    result = app.use_cases.cypher.execute(
        "MATCH (a:IdStart) OPTIONAL MATCH (a)-[r:IdLink]->() "
        "RETURN a.id AS node_property,id(a) AS node_identity,r.id AS property,id(r) AS identity "
        "ORDER BY a.id,id(r)"
    )
    assert result.rows == (
        {"node_property": 700, "node_identity": 1, "property": 999, "identity": 1},
        {"node_property": 700, "node_identity": 1, "property": 4, "identity": 2},
        {"node_property": 900, "node_identity": 3, "property": None, "identity": None},
    )


@pytest.mark.parametrize("value", [None, "external-key", 0, -7, True])
def test_relationship_id_property_types_and_missing_values(tmp_path: Path, value: object) -> None:
    query = build_application(tmp_path).use_cases.cypher.execute
    query("CREATE ()-[:TypedId {id:$value}]->()", {"value": value})
    assert query("MATCH ()-[r:TypedId]->() RETURN id(r) AS identity,r.id AS property").rows == (
        {"identity": 1, "property": value},
    )
    assert query(
        "MATCH ()-[r:TypedId]->() WITH r AS edge RETURN id(edge) AS identity,edge.id AS property"
    ).rows == ({"identity": 1, "property": value},)


def test_selective_id_set_does_not_load_the_complete_graph(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = build_application(tmp_path)
    query = app.use_cases.cypher.execute
    query("CREATE ()-[:PointId {id:999}]->()")

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("An id property edit must retain the selective SET path")

    monkeypatch.setattr(app.use_cases.graph, "load_state", forbidden)
    assert query(
        "MATCH ()-[r:PointId]->() WHERE id(r)=1 SET r.id=123 RETURN r.id AS property"
    ).rows == ({"property": 123},)
    assert build_application(tmp_path).use_cases.cypher.execute(
        "MATCH ()-[r:PointId]->() RETURN id(r) AS identity,r.id AS property"
    ).rows == ({"identity": 1, "property": 123},)


def test_equal_properties_do_not_merge_relationship_identities(tmp_path: Path) -> None:
    query = build_application(tmp_path).use_cases.cypher.execute
    query("CREATE (a:Loop)-[:LoopLink {id:7}]->(a), (a)-[:LoopLink {id:7}]->(a)")
    assert query(
        "MATCH (a:Loop)-[r:LoopLink]->(b)-[s:LoopLink]->(c) "
        "RETURN id(r) AS first,id(s) AS second,r.id AS first_property,s.id AS second_property "
        "ORDER BY id(r)"
    ).rows == (
        {"first": 1, "second": 2, "first_property": 7, "second_property": 7},
        {"first": 2, "second": 1, "first_property": 7, "second_property": 7},
    )
    assert query("MATCH p=(:Loop)-[:LoopLink*2]->() RETURN length(p) AS length").rows == (
        {"length": 2},
        {"length": 2},
    )


def test_node_id_property_updates_do_not_change_node_identity(app: GraphArrowApplication) -> None:
    assert app.use_cases.cypher.execute(
        "MATCH (a:IdStart) SET a.id='external-node' RETURN id(a) AS identity,a.id AS property"
    ).rows == ({"identity": 1, "property": "external-node"},)
    state = app.use_cases.graph.load_state()
    assert state.nodes[0].node_id == 1
    assert state.nodes[0].properties["id"] == "external-node"
    assert all(edge.src_id in {1, 2} and edge.dst_id in {1, 2} for edge in state.edges)


@pytest.mark.parametrize("suffix", ["", "ORDER BY r.id", "ORDER BY r.id DESC SKIP 0 LIMIT 1"])
def test_absent_id_property_is_null_instead_of_identity(tmp_path: Path, suffix: str) -> None:
    query = build_application(tmp_path).use_cases.cypher.execute
    query("CREATE ()-[:AbsentId]->()")
    assert query(
        "MATCH ()-[r:AbsentId]->() RETURN id(r) AS identity,r.id AS property " + suffix
    ).rows == ({"identity": 1, "property": None},)
