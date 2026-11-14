from importlib.metadata import version
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import grapharrow
from grapharrow.config import GraphArrowSettings
from grapharrow.core.errors import WriteConflictError
from grapharrow.interfaces.rest import create_rest_app


def test_rest_reports_the_installed_package_version(tmp_path: Path) -> None:
    app = create_rest_app(tmp_path / "version")
    assert grapharrow.__version__ == version("grapharrow")
    assert app.version == grapharrow.__version__
    assert TestClient(app).get("/openapi.json").json()["info"]["version"] == grapharrow.__version__


def test_rest_write_conflict_is_a_409_response(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = create_rest_app(tmp_path / "conflict")

    def fail(query: str, params: object = None) -> object:
        raise WriteConflictError("Database changed. Retry in a new transaction.")

    monkeypatch.setattr(app.state.grapharrow.use_cases.cypher, "execute", fail)
    response = TestClient(app).post("/v1/query", json={"query": "CREATE (:Person)"})
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "WRITE_CONFLICT"


def test_rest_health_reports_a_bootstrapped_database(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    response = client.get("/v1/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["data"]["status"] == "ok"
    assert payload["data"]["catalog_version"] == 1
    assert payload["data"]["snapshot_id"] == 1


def test_rest_query_can_create_and_match_graph_data(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    create_response = client.post(
        "/v1/query",
        json={
            "query": "CREATE (n:Person {name: $name, age: 30}) RETURN n",
            "params": {"name": "Alice"},
        },
    )
    match_response = client.post(
        "/v1/query",
        json={
            "query": ("MATCH (n:Person) WHERE n.name = $name RETURN n ORDER BY n.age DESC LIMIT 1"),
            "params": {"name": "Alice"},
        },
    )

    assert create_response.status_code == 200
    assert match_response.status_code == 200
    create_payload = create_response.json()
    match_payload = match_response.json()
    assert create_payload["ok"] is True
    assert match_payload["ok"] is True
    assert create_payload["meta"]["row_count"] == 1
    assert match_payload["data"]["rows"][0]["n"]["properties"]["name"] == "Alice"


def test_rest_query_supports_match_with_pipeline(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    client.post(
        "/v1/query",
        json={"query": "CREATE (n:Person {name: 'Alice'}) RETURN n", "params": {}},
    )
    client.post(
        "/v1/query",
        json={"query": "CREATE (n:Person {name: 'Bob'}) RETURN n", "params": {}},
    )

    response = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (n:Person) WITH n.name AS name WHERE name STARTS WITH 'A' RETURN name"
            ),
            "params": {},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["data"]["rows"] == [{"name": "Alice"}]


def test_rest_query_supports_match_with_match_pipeline(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    client.post(
        "/v1/query",
        json={
            "query": (
                "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, b"
            ),
            "params": {},
        },
    )

    response = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (n:Person) WITH n MATCH (n)-[:KNOWS]->(m:Person) RETURN n.name, m.name"
            ),
            "params": {},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["data"]["rows"] == [{"n.name": "Alice", "m.name": "Bob"}]


def test_rest_query_supports_optional_match_pipeline(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    client.post(
        "/v1/query",
        json={"query": "CREATE (n:Person {name: 'Alice'}) RETURN n", "params": {}},
    )

    response = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (n:Person) OPTIONAL MATCH (n)-[r:KNOWS]->(m:Person) "
                "RETURN n.name, type(r), m.name"
            ),
            "params": {},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["data"]["rows"] == [{"n.name": "Alice", "type(r)": None, "m.name": None}]


def test_rest_query_supports_match_with_optional_match_pipeline(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    client.post(
        "/v1/query",
        json={"query": "CREATE (n:Person {name: 'Alice'}) RETURN n", "params": {}},
    )

    response = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (n:Person) WITH n OPTIONAL MATCH (n)-[r:KNOWS]->(m:Person) "
                "RETURN n.name, type(r), m.name"
            ),
            "params": {},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["data"]["rows"] == [{"n.name": "Alice", "type(r)": None, "m.name": None}]


def test_rest_query_supports_unwind_pipeline(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    client.post(
        "/v1/query",
        json={"query": "CREATE (n:Person {name: 'Alice'}) RETURN n", "params": {}},
    )

    response = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (n:Person) WITH n UNWIND ['x', 'y'] AS tag "
                "RETURN n.name, tag ORDER BY tag ASC"
            ),
            "params": {},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["data"]["rows"] == [
        {"n.name": "Alice", "tag": "x"},
        {"n.name": "Alice", "tag": "y"},
    ]


def test_rest_query_supports_aggregate_only_with_pipeline(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    client.post(
        "/v1/query",
        json={"query": "CREATE (n:Person {name: 'Alice'}) RETURN n", "params": {}},
    )
    client.post(
        "/v1/query",
        json={"query": "CREATE (n:Person {name: 'Bob'}) RETURN n", "params": {}},
    )

    response = client.post(
        "/v1/query",
        json={
            "query": "MATCH (n:Person) WITH COUNT(*) AS total RETURN total",
            "params": {},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["data"]["rows"] == [{"total": 2}]


def test_rest_query_supports_grouped_with_pipeline(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    client.post(
        "/v1/query",
        json={"query": "CREATE (n:Person {name: 'Alice'}) RETURN n", "params": {}},
    )
    client.post(
        "/v1/query",
        json={"query": "CREATE (n:Person {name: 'Alice'}) RETURN n", "params": {}},
    )
    client.post(
        "/v1/query",
        json={"query": "CREATE (n:Person {name: 'Bob'}) RETURN n", "params": {}},
    )

    response = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (n:Person) "
                "WITH n.name AS name, COUNT(*) AS total "
                "RETURN name, total ORDER BY name ASC"
            ),
            "params": {},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["data"]["rows"] == [
        {"name": "Alice", "total": 2},
        {"name": "Bob", "total": 1},
    ]


def test_rest_query_supports_tolower_and_toupper(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    client.post(
        "/v1/query",
        json={"query": "CREATE (n:Person {name: 'Alice'}) RETURN n", "params": {}},
    )
    client.post(
        "/v1/query",
        json={"query": "CREATE (n:Person {name: 'BOB'}) RETURN n", "params": {}},
    )

    response = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (n:Person) WHERE toLower(n.name) CONTAINS 'o' "
                "RETURN toLower(n.name) as lower_name, toUpper(n.name) as upper_name "
                "ORDER BY toUpper(n.name) DESC"
            ),
            "params": {},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["data"]["rows"] == [{"lower_name": "bob", "upper_name": "BOB"}]


def test_rest_query_supports_size_on_string_and_list_values(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    client.post(
        "/v1/query",
        json={
            "query": "CREATE (n:Person {name: 'Alice', tags: ['ml', 'graph']}) RETURN n",
            "params": {},
        },
    )
    client.post(
        "/v1/query",
        json={
            "query": "CREATE (n:Person {name: 'Bob', tags: ['db']}) RETURN n",
            "params": {},
        },
    )

    response = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (n:Person) WHERE size(n.tags) > 1 "
                "RETURN n.name, size(n.name) as name_size, size(n.tags) as tag_count "
                "ORDER BY size(n.name) DESC"
            ),
            "params": {},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["data"]["rows"] == [
        {"n.name": "Alice", "name_size": 5, "tag_count": 2},
    ]


def test_rest_query_supports_idempotent_node_merge(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    first = client.post(
        "/v1/query",
        json={
            "query": "MERGE (n:Person {name: 'Alice'}) RETURN n",
            "params": {},
        },
    )
    second = client.post(
        "/v1/query",
        json={
            "query": "MERGE (n:Person {name: 'Alice'}) RETURN n",
            "params": {},
        },
    )
    count = client.post(
        "/v1/query",
        json={
            "query": "MATCH (n:Person) RETURN COUNT(*) AS total",
            "params": {},
        },
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert count.status_code == 200
    assert first.json()["data"]["rows"] == [
        {"n": {"id": 1, "labels": ["Person"], "properties": {"name": "Alice"}}}
    ]
    assert second.json()["data"]["rows"] == [
        {"n": {"id": 1, "labels": ["Person"], "properties": {"name": "Alice"}}}
    ]
    assert count.json()["data"]["rows"] == [{"total": 1}]


def test_rest_query_supports_merge_on_create_and_on_match_set(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    created = client.post(
        "/v1/query",
        json={
            "query": (
                "MERGE (n:Person {name: 'Alice'}) "
                "ON CREATE SET n.age = 30 "
                "ON MATCH SET n.last_seen = 1 "
                "RETURN n"
            ),
            "params": {},
        },
    )
    matched = client.post(
        "/v1/query",
        json={
            "query": (
                "MERGE (n:Person {name: 'Alice'}) "
                "ON CREATE SET n.age = 30 "
                "ON MATCH SET n.last_seen = 1 "
                "RETURN n"
            ),
            "params": {},
        },
    )

    assert created.status_code == 200
    assert matched.status_code == 200
    assert created.json()["data"]["rows"] == [
        {"n": {"id": 1, "labels": ["Person"], "properties": {"name": "Alice", "age": 30}}}
    ]
    assert matched.json()["data"]["rows"] == [
        {
            "n": {
                "id": 1,
                "labels": ["Person"],
                "properties": {"name": "Alice", "age": 30, "last_seen": 1},
            }
        }
    ]


def test_rest_query_supports_match_merge_relationship_creation(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    create = client.post(
        "/v1/query",
        json={
            "query": (
                "CREATE (a:Person {name: 'Alice'})-[:FRIEND]->(b:Person {name: 'Bob'}) RETURN a, b"
            ),
            "params": {},
        },
    )
    first_merge = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (a:Person)-[:FRIEND]->(b:Person) "
                "MERGE (a)-[:KNOWS]->(b) "
                "RETURN a.name, b.name"
            ),
            "params": {},
        },
    )
    second_merge = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (a:Person)-[:FRIEND]->(b:Person) "
                "MERGE (a)-[:KNOWS]->(b) "
                "RETURN a.name, b.name"
            ),
            "params": {},
        },
    )
    count = client.post(
        "/v1/query",
        json={
            "query": "MATCH ()-[r:KNOWS]->() RETURN COUNT(r) AS total",
            "params": {},
        },
    )

    assert create.status_code == 200
    assert first_merge.status_code == 200
    assert second_merge.status_code == 200
    assert count.status_code == 200
    assert first_merge.json()["data"]["rows"] == [{"a.name": "Alice", "b.name": "Bob"}]
    assert second_merge.json()["data"]["rows"] == [{"a.name": "Alice", "b.name": "Bob"}]
    assert count.json()["data"]["rows"] == [{"total": 1}]


def test_rest_query_supports_inline_properties_on_bound_merge_nodes(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    client.post(
        "/v1/query",
        json={
            "query": (
                "CREATE (a:Person {name: 'Alice', status: 'active'})-[:FRIEND]->"
                "(b:Person {name: 'Bob'}) RETURN a, b"
            ),
            "params": {},
        },
    )
    response = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (a:Person)-[:FRIEND]->(b:Person) "
                "MERGE (a:Person {status: 'active'})-[:KNOWS]->(b:Person) "
                "RETURN a.name, b.name"
            ),
            "params": {},
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["rows"] == [{"a.name": "Alice", "b.name": "Bob"}]


def test_rest_query_supports_unbound_inline_node_merge_pattern(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    client.post(
        "/v1/query",
        json={
            "query": "CREATE (a:Person {name: 'Alice'}) RETURN a",
            "params": {},
        },
    )
    created = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (a:Person {name: 'Alice'}) "
                "MERGE (a)-[:TAGGED]->(t:Tag {name: 'vip'}) "
                "ON CREATE SET t.created = true "
                "ON MATCH SET t.last_seen = 1 "
                "RETURN t"
            ),
            "params": {},
        },
    )
    matched = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (a:Person {name: 'Alice'}) "
                "MERGE (a)-[:TAGGED]->(t:Tag {name: 'vip'}) "
                "ON CREATE SET t.created = true "
                "ON MATCH SET t.last_seen = 1 "
                "RETURN t"
            ),
            "params": {},
        },
    )

    assert created.status_code == 200
    assert matched.status_code == 200
    assert created.json()["data"]["rows"] == [
        {"t": {"id": 2, "labels": ["Tag"], "properties": {"name": "vip", "created": True}}}
    ]
    assert matched.json()["data"]["rows"] == [
        {
            "t": {
                "id": 2,
                "labels": ["Tag"],
                "properties": {"name": "vip", "created": True, "last_seen": 1},
            }
        }
    ]


def test_rest_query_supports_unbound_inline_left_node_merge_pattern(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    client.post(
        "/v1/query",
        json={
            "query": "CREATE (a:Person {name: 'Alice'}) RETURN a",
            "params": {},
        },
    )
    created = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (a:Person {name: 'Alice'}) "
                "MERGE (t:Tag {name: 'vip'})-[:TAGGED]->(a) "
                "ON CREATE SET t.created = true "
                "ON MATCH SET t.last_seen = 1 "
                "RETURN t"
            ),
            "params": {},
        },
    )
    matched = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (a:Person {name: 'Alice'}) "
                "MERGE (t:Tag {name: 'vip'})-[:TAGGED]->(a) "
                "ON CREATE SET t.created = true "
                "ON MATCH SET t.last_seen = 1 "
                "RETURN t"
            ),
            "params": {},
        },
    )

    assert created.status_code == 200
    assert matched.status_code == 200
    assert created.json()["data"]["rows"] == [
        {"t": {"id": 2, "labels": ["Tag"], "properties": {"name": "vip", "created": True}}}
    ]
    assert matched.json()["data"]["rows"] == [
        {
            "t": {
                "id": 2,
                "labels": ["Tag"],
                "properties": {"name": "vip", "created": True, "last_seen": 1},
            }
        }
    ]


def test_rest_query_supports_match_merge_on_create_and_on_match_set(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    client.post(
        "/v1/query",
        json={
            "query": (
                "CREATE (a:Person {name: 'Alice'})-[:FRIEND]->(b:Person {name: 'Bob'}) RETURN a, b"
            ),
            "params": {},
        },
    )
    created = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (a:Person)-[:FRIEND]->(b:Person) "
                "MERGE (a)-[:KNOWS]->(b) "
                "ON CREATE SET a.flag = true "
                "ON MATCH SET b.seen = true "
                "RETURN a.flag, b.seen"
            ),
            "params": {},
        },
    )
    matched = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (a:Person)-[:FRIEND]->(b:Person) "
                "MERGE (a)-[:KNOWS]->(b) "
                "ON CREATE SET a.flag = true "
                "ON MATCH SET b.seen = true "
                "RETURN a.flag, b.seen"
            ),
            "params": {},
        },
    )

    assert created.status_code == 200
    assert matched.status_code == 200
    assert created.json()["data"]["rows"] == [{"a.flag": True, "b.seen": None}]
    assert matched.json()["data"]["rows"] == [{"a.flag": True, "b.seen": True}]


def test_rest_query_supports_match_merge_relationship_variable(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    client.post(
        "/v1/query",
        json={
            "query": (
                "CREATE (a:Person {name: 'Alice'})-[:FRIEND]->(b:Person {name: 'Bob'}) RETURN a, b"
            ),
            "params": {},
        },
    )
    created = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (a:Person)-[:FRIEND]->(b:Person) "
                "MERGE (a)-[r:KNOWS]->(b) "
                "ON CREATE SET r.since = 2024 "
                "ON MATCH SET r.last_seen = 1 "
                "RETURN type(r), r.since, r.last_seen"
            ),
            "params": {},
        },
    )
    matched = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (a:Person)-[:FRIEND]->(b:Person) "
                "MERGE (a)-[r:KNOWS]->(b) "
                "ON CREATE SET r.since = 2024 "
                "ON MATCH SET r.last_seen = 1 "
                "RETURN r"
            ),
            "params": {},
        },
    )

    assert created.status_code == 200
    assert matched.status_code == 200
    assert created.json()["data"]["rows"] == [
        {"type(r)": "KNOWS", "r.since": 2024, "r.last_seen": None}
    ]
    assert matched.json()["data"]["rows"] == [
        {
            "r": {
                "id": 2,
                "type": "KNOWS",
                "source": 1,
                "target": 2,
                "properties": {"since": 2024, "last_seen": 1},
            }
        }
    ]


def test_rest_query_supports_set_plus_equals_map_merge(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    client.post(
        "/v1/query",
        json={
            "query": "CREATE (n:Person {name: 'Alice', age: 30}) RETURN n",
            "params": {},
        },
    )

    response = client.post(
        "/v1/query",
        json={
            "query": ("MATCH (n:Person) WHERE n.name = 'Alice' SET n += $props RETURN n"),
            "params": {"props": {"age": 31, "city": "Rome"}},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["data"]["rows"] == [
        {
            "n": {
                "id": 1,
                "labels": ["Person"],
                "properties": {"name": "Alice", "age": 31, "city": "Rome"},
            }
        }
    ]


def test_rest_query_supports_detach_delete(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    client.post(
        "/v1/query",
        json={
            "query": (
                "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, b"
            ),
            "params": {},
        },
    )

    delete_response = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (n:Person {name: 'Alice'}) WITH n,n.name AS name DETACH DELETE n RETURN name"
            ),
            "params": {},
        },
    )
    count_response = client.post(
        "/v1/query",
        json={
            "query": "MATCH ()-[r]->() RETURN COUNT(r) AS total",
            "params": {},
        },
    )

    assert delete_response.status_code == 200
    assert delete_response.json()["data"]["rows"] == [{"name": "Alice"}]
    assert count_response.status_code == 200
    assert count_response.json()["data"]["rows"] == [{"total": 0}]


def test_rest_imperative_endpoints_can_create_nodes_edges_and_triples(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    first_node = client.post(
        "/v1/nodes",
        json={"labels": ["Person"], "properties": {"name": "Alice"}},
    )
    second_node = client.post(
        "/v1/nodes",
        json={"labels": ["Person"], "properties": {"name": "Bob"}},
    )
    edge = client.post(
        "/v1/edges",
        json={"src_id": 1, "dst_id": 2, "rel_type": "KNOWS", "properties": {"since": 2024}},
    )
    triple = client.post(
        "/v1/triples",
        json={"subject_id": 2, "predicate": "KNOWS", "object_id": 1, "properties": {}},
    )
    match = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name ORDER BY a.name ASC"
            ),
            "params": {},
        },
    )

    assert first_node.status_code == 201
    assert second_node.status_code == 201
    assert edge.status_code == 201
    assert triple.status_code == 201
    assert match.status_code == 200
    match_payload = match.json()
    assert match_payload["meta"]["row_count"] == 2
    assert match_payload["data"]["rows"] == [
        {"a.name": "Alice", "b.name": "Bob"},
        {"a.name": "Bob", "b.name": "Alice"},
    ]


def test_rest_bulk_endpoints_can_create_nodes_and_edges(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    nodes = client.post(
        "/v1/nodes/bulk",
        json={
            "rows": [
                {"external_id": "alice", "name": "Alice"},
                {"external_id": "bob", "name": "Bob"},
            ],
            "default_labels": ["Person"],
        },
    )
    edges = client.post(
        "/v1/edges/bulk",
        json={
            "rows": [
                {
                    "src_key": "alice",
                    "dst_key": "bob",
                    "rel_type": "KNOWS",
                    "properties": {"since": 2024},
                }
            ],
            "source_key": {"label": "Person", "key": "external_id"},
            "target_key": {"label": "Person", "key": "external_id"},
        },
    )
    match = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
                "RETURN a.external_id, b.external_id, r.since"
            ),
            "params": {},
        },
    )

    assert nodes.status_code == 201
    assert nodes.json()["meta"]["row_count"] == 2
    assert nodes.json()["data"]["nodes"] == [
        {
            "id": 1,
            "labels": ["Person"],
            "properties": {"external_id": "alice", "name": "Alice"},
        },
        {
            "id": 2,
            "labels": ["Person"],
            "properties": {"external_id": "bob", "name": "Bob"},
        },
    ]
    assert edges.status_code == 201
    assert edges.json()["meta"]["row_count"] == 1
    assert edges.json()["data"]["edges"] == [
        {
            "id": 1,
            "type": "KNOWS",
            "source": 1,
            "target": 2,
            "properties": {"since": 2024},
        }
    ]
    assert match.status_code == 200
    assert match.json()["data"]["rows"] == [
        {"a.external_id": "alice", "b.external_id": "bob", "r.since": 2024}
    ]


def test_rest_bulk_edges_support_internal_ids(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    client.post(
        "/v1/nodes/bulk",
        json={
            "rows": [
                {"labels": ["Person"], "name": "Alice"},
                {"labels": ["Person"], "name": "Bob"},
            ]
        },
    )
    response = client.post(
        "/v1/edges/bulk",
        json={
            "rows": [
                {"src_id": 1, "dst_id": 2, "rel_type": "KNOWS", "since": 2024},
            ]
        },
    )

    assert response.status_code == 201
    assert response.json()["data"]["edges"] == [
        {
            "id": 1,
            "type": "KNOWS",
            "source": 1,
            "target": 2,
            "properties": {"since": 2024},
        }
    ]


def test_rest_bulk_edges_require_both_reference_configs(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    client.post(
        "/v1/nodes/bulk",
        json={
            "rows": [
                {"external_id": "alice"},
                {"external_id": "bob"},
            ],
            "default_labels": ["Person"],
        },
    )
    response = client.post(
        "/v1/edges/bulk",
        json={
            "rows": [{"src_key": "alice", "dst_key": "bob", "rel_type": "KNOWS"}],
            "source_key": {"label": "Person", "key": "external_id"},
        },
    )

    assert response.status_code == 400
    payload = response.json()
    assert payload["ok"] is False
    assert payload["error"]["code"] == "QUERYERROR"


def test_rest_edges_can_reference_nodes_by_business_key(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    client.post(
        "/v1/nodes",
        json={"labels": ["Person"], "properties": {"external_id": "alice", "name": "Alice"}},
    )
    client.post(
        "/v1/nodes",
        json={"labels": ["Person"], "properties": {"external_id": "bob", "name": "Bob"}},
    )
    edge = client.post(
        "/v1/edges",
        json={
            "source_ref": {"label": "Person", "key": "external_id", "value": "alice"},
            "target_ref": {"label": "Person", "key": "external_id", "value": "bob"},
            "rel_type": "KNOWS",
            "properties": {"since": 2025},
        },
    )
    match = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name ORDER BY a.name ASC"
            ),
            "params": {},
        },
    )

    assert edge.status_code == 201
    edge_payload = edge.json()
    assert edge_payload["data"]["type"] == "KNOWS"
    assert match.status_code == 200
    assert match.json()["data"]["rows"] == [{"a.name": "Alice", "b.name": "Bob"}]


def test_rest_returns_not_found_for_missing_edge_references(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    response = client.post(
        "/v1/edges",
        json={"src_id": 1, "dst_id": 999, "rel_type": "KNOWS", "properties": {}},
    )

    assert response.status_code == 404
    payload = response.json()
    assert payload["ok"] is False
    assert payload["error"]["code"] == "NOT_FOUND"


def test_rest_returns_bad_request_for_ambiguous_business_key_references(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    client.post(
        "/v1/nodes",
        json={"labels": ["Person"], "properties": {"external_id": "alice", "name": "Alice One"}},
    )
    client.post(
        "/v1/nodes",
        json={"labels": ["Person"], "properties": {"external_id": "alice", "name": "Alice Two"}},
    )
    client.post(
        "/v1/nodes",
        json={"labels": ["Person"], "properties": {"external_id": "bob", "name": "Bob"}},
    )

    response = client.post(
        "/v1/edges",
        json={
            "source_ref": {"label": "Person", "key": "external_id", "value": "alice"},
            "target_ref": {"label": "Person", "key": "external_id", "value": "bob"},
            "rel_type": "KNOWS",
            "properties": {},
        },
    )

    assert response.status_code == 400
    payload = response.json()
    assert payload["ok"] is False
    assert payload["error"]["code"] == "QUERYERROR"


def test_rest_admin_endpoints_expose_verify_and_recover(tmp_path: Path) -> None:
    database_path = tmp_path / "example-db"
    client = TestClient(create_rest_app(database_path))
    orphan_path = database_path / "staged" / "tx-000001"
    orphan_path.mkdir(parents=True)

    verify_response = client.get("/v1/admin/verify")
    recover_response = client.post(
        "/v1/admin/recover",
        json={"dry_run": True, "cleanup_orphan_staged": True, "verify_checksums": False},
    )

    assert verify_response.status_code == 200
    assert recover_response.status_code == 200
    verify_payload = verify_response.json()
    recover_payload = recover_response.json()
    assert verify_payload["ok"] is True
    assert verify_payload["data"]["ok"] is True
    assert recover_payload["ok"] is True
    assert recover_payload["data"]["orphan_staged_paths"] == ["staged/tx-000001"]
    assert recover_payload["data"]["removed_paths"] == []
    assert orphan_path.exists() is True


def test_rest_admin_endpoints_expose_compact_and_vacuum(tmp_path: Path) -> None:
    database_path = tmp_path / "example-db"
    client = TestClient(create_rest_app(database_path))

    client.post(
        "/v1/query",
        json={"query": "CREATE (n:Person {name: 'Alice'}) RETURN n", "params": {}},
    )
    client.post(
        "/v1/query",
        json={"query": "CREATE (n:Person {name: 'Bob'}) RETURN n", "params": {}},
    )

    compact_response = client.post("/v1/admin/compact")
    vacuum_response = client.post("/v1/admin/vacuum")

    assert compact_response.status_code == 200
    assert compact_response.json()["data"]["snapshot_id"] >= 4
    assert vacuum_response.status_code == 200
    assert any(
        path.startswith("manifests/snapshot-000001")
        for path in vacuum_response.json()["data"]["removed_paths"]
    )


def test_rest_auth_can_be_enabled_with_a_static_bearer_token(tmp_path: Path) -> None:
    client = TestClient(
        create_rest_app(
            tmp_path / "example-db",
            settings=GraphArrowSettings(server_auth_token="secret-token"),
        )
    )

    unauthorized_response = client.post(
        "/v1/query",
        json={"query": "MATCH (n) RETURN n", "params": {}},
    )
    health_response = client.get("/v1/health")
    authorized_response = client.post(
        "/v1/query",
        json={"query": "MATCH (n) RETURN n", "params": {}},
        headers={"Authorization": "Bearer secret-token"},
    )

    assert unauthorized_response.status_code == 401
    assert unauthorized_response.json()["detail"] == "Invalid or missing authorization token."
    assert health_response.status_code == 200
    assert authorized_response.status_code == 200


def test_rest_query_supports_match_create_with_bound_variables(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    client.post(
        "/v1/query",
        json={
            "query": (
                "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, b"
            ),
            "params": {},
        },
    )

    response = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (a:Person)-[:KNOWS]->(b:Person) "
                "CREATE (a)-[:LIKES]->(b), (b)-[:LIKES]->(a) "
                "RETURN a.name, b.name ORDER BY a.name ASC"
            ),
            "params": {},
        },
    )
    verify = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (a:Person)-[:LIKES]->(b:Person) RETURN a.name, b.name ORDER BY a.name ASC"
            ),
            "params": {},
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["rows"] == [{"a.name": "Alice", "b.name": "Bob"}]
    assert verify.status_code == 200
    assert verify.json()["data"]["rows"] == [
        {"a.name": "Alice", "b.name": "Bob"},
        {"a.name": "Bob", "b.name": "Alice"},
    ]


def test_rest_query_supports_in_predicates(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "example-db"))

    client.post(
        "/v1/nodes",
        json={"labels": ["Person"], "properties": {"name": "Alice"}},
    )
    client.post(
        "/v1/nodes",
        json={"labels": ["Person"], "properties": {"name": "Bob"}},
    )
    client.post(
        "/v1/nodes",
        json={"labels": ["Person"], "properties": {"name": "Carol"}},
    )

    response = client.post(
        "/v1/query",
        json={
            "query": ("MATCH (n:Person) WHERE n.name IN $names RETURN n.name ORDER BY n.name ASC"),
            "params": {"names": ["Alice", "Carol"]},
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["rows"] == [
        {"n.name": "Alice"},
        {"n.name": "Carol"},
    ]
