import pytest
from fastapi.testclient import TestClient

from grapharrow.interfaces.rest import create_rest_app
from tests.integration.support import (
    MotoS3Context,
    assert_expected_storage_layout,
    has_parquet_magic,
    list_object_keys,
    load_json_object,
    read_parquet_object,
)


@pytest.mark.integration
def test_rest_supports_s3_storage_via_moto_end_to_end(
    moto_s3_filesystem: MotoS3Context,
) -> None:
    filesystem = moto_s3_filesystem.filesystem
    bucket_uri = moto_s3_filesystem.bucket_uri
    client = TestClient(create_rest_app(f"{bucket_uri}/example-db", filesystem=filesystem))

    health = client.get("/v1/health")
    create = client.post(
        "/v1/query",
        json={
            "query": (
                "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, b"
            ),
            "params": {},
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
    functions_query = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
                "RETURN id(a) as source_id, labels(a) as source_labels, type(r) as rel_type"
            ),
            "params": {},
        },
    )
    exists_query = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
                "WHERE EXISTS(a.name) AND NOT EXISTS(r.since) "
                "RETURN count(r) as count"
            ),
            "params": {},
        },
    )
    created = load_json_object(moto_s3_filesystem, "current_snapshot.json")
    create_manifest = load_json_object(moto_s3_filesystem, str(created["manifest_path"]))
    create_files_payload = create_manifest["files"]
    assert isinstance(create_files_payload, dict)
    create_nodes_payload = create_files_payload["nodes"]
    create_edges_payload = create_files_payload["edges"]
    assert isinstance(create_nodes_payload, list)
    assert isinstance(create_edges_payload, list)
    create_edge_file = str(create_edges_payload[0]["path"])
    create_node_file = str(create_nodes_payload[0]["path"])
    create_edge_table = read_parquet_object(moto_s3_filesystem, create_edge_file)
    create_node_table = read_parquet_object(moto_s3_filesystem, create_node_file)
    verify = client.get("/v1/admin/verify")
    compact = client.post("/v1/admin/compact")
    vacuum = client.post("/v1/admin/vacuum")
    current_snapshot = load_json_object(moto_s3_filesystem, "current_snapshot.json")
    current_manifest = load_json_object(
        moto_s3_filesystem,
        str(current_snapshot["manifest_path"]),
    )
    keys = list_object_keys(moto_s3_filesystem, prefix="example-db/")
    files_payload = current_manifest["files"]
    assert isinstance(files_payload, dict)
    nodes_payload = files_payload["nodes"]
    edges_payload = files_payload["edges"]
    node_labels_payload = files_payload["node_labels"]
    adjacency_payload = files_payload["adjacency_by_source"]
    assert isinstance(nodes_payload, list)
    assert isinstance(edges_payload, list)
    assert isinstance(node_labels_payload, list)
    assert isinstance(adjacency_payload, list)
    node_file = str(nodes_payload[0]["path"])
    edge_file = str(edges_payload[0]["path"])
    node_label_file = str(node_labels_payload[0]["path"])
    adjacency_file = str(adjacency_payload[0]["path"])
    node_table = read_parquet_object(moto_s3_filesystem, node_file)
    edge_table = read_parquet_object(moto_s3_filesystem, edge_file)

    assert health.status_code == 200
    assert health.json()["data"]["status"] == "ok"
    assert create.status_code == 200
    assert match.status_code == 200
    pairs = {(row["a.name"], row["b.name"]) for row in match.json()["data"]["rows"]}
    assert pairs == {("Alice", "Bob")}
    assert functions_query.status_code == 200
    assert functions_query.json()["data"]["rows"] == [
        {
            "source_id": 1,
            "source_labels": ["Person"],
            "rel_type": "KNOWS",
        }
    ]
    assert exists_query.status_code == 200
    assert exists_query.json()["data"]["rows"] == [{"count": 1}]
    assert create_manifest["snapshot_id"] == 2
    assert create_edge_table.to_pylist() == [
        {
            "edge_id": 1,
            "rel_type": "KNOWS",
            "src_id": 1,
            "dst_id": 2,
            "properties_json": "{}",
        }
    ]
    assert {row["properties_json"] for row in create_node_table.to_pylist()} == {
        '{"name": "Alice"}',
        '{"name": "Bob"}',
    }
    assert verify.status_code == 200
    assert verify.json()["data"]["ok"] is True
    assert compact.status_code == 200
    assert compact.json()["data"]["snapshot_id"] >= 3
    assert vacuum.status_code == 200
    assert_expected_storage_layout(
        keys=keys,
        node_file=node_file,
        edge_file=edge_file,
        node_label_file=node_label_file,
        adjacency_file=adjacency_file,
    )
    assert has_parquet_magic(moto_s3_filesystem, node_file) is True
    assert has_parquet_magic(moto_s3_filesystem, edge_file) is True
    assert has_parquet_magic(moto_s3_filesystem, node_label_file) is True
    assert has_parquet_magic(moto_s3_filesystem, adjacency_file) is True
    assert {row["properties_json"] for row in node_table.to_pylist()} == {
        '{"name": "Alice"}',
        '{"name": "Bob"}',
    }
    assert {
        (
            row["edge_id"],
            row["rel_type"],
            row["src_id"],
            row["dst_id"],
            row["properties_json"],
        )
        for row in edge_table.to_pylist()
    } >= {
        (
            1,
            "KNOWS",
            1,
            2,
            "{}",
        )
    }
    assert vacuum.json()["data"]["removed_paths"] == []


@pytest.mark.integration
def test_rest_bulk_endpoints_support_s3_storage_via_moto(
    moto_s3_filesystem: MotoS3Context,
) -> None:
    filesystem = moto_s3_filesystem.filesystem
    bucket_uri = moto_s3_filesystem.bucket_uri
    client = TestClient(create_rest_app(f"{bucket_uri}/example-db", filesystem=filesystem))

    bulk_nodes = client.post(
        "/v1/nodes/bulk",
        json={
            "rows": [
                {"external_id": "alice", "name": "Alice"},
                {"external_id": "bob", "name": "Bob"},
            ],
            "default_labels": ["Person"],
        },
    )
    bulk_edges = client.post(
        "/v1/edges/bulk",
        json={
            "rows": [
                {
                    "src_key": "alice",
                    "dst_key": "bob",
                    "rel_type": "KNOWS",
                    "since": 2024,
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

    current_snapshot = load_json_object(moto_s3_filesystem, "current_snapshot.json")
    current_manifest = load_json_object(
        moto_s3_filesystem,
        str(current_snapshot["manifest_path"]),
    )
    keys = list_object_keys(moto_s3_filesystem, prefix="example-db/")
    files_payload = current_manifest["files"]
    assert isinstance(files_payload, dict)
    nodes_payload = files_payload["nodes"]
    edges_payload = files_payload["edges"]
    node_labels_payload = files_payload["node_labels"]
    adjacency_payload = files_payload["adjacency_by_source"]
    assert isinstance(nodes_payload, list)
    assert isinstance(edges_payload, list)
    assert isinstance(node_labels_payload, list)
    assert isinstance(adjacency_payload, list)

    node_file = str(nodes_payload[0]["path"])
    edge_file = str(edges_payload[0]["path"])
    node_label_file = str(node_labels_payload[0]["path"])
    adjacency_file = str(adjacency_payload[0]["path"])
    node_table = read_parquet_object(moto_s3_filesystem, node_file)
    edge_table = read_parquet_object(moto_s3_filesystem, edge_file)
    node_label_table = read_parquet_object(moto_s3_filesystem, node_label_file)
    adjacency_table = read_parquet_object(moto_s3_filesystem, adjacency_file)

    assert bulk_nodes.status_code == 201
    assert bulk_nodes.json()["meta"]["row_count"] == 2
    assert bulk_edges.status_code == 201
    assert bulk_edges.json()["meta"]["row_count"] == 1
    assert match.status_code == 200
    assert match.json()["data"]["rows"] == [
        {"a.external_id": "alice", "b.external_id": "bob", "r.since": 2024}
    ]
    assert_expected_storage_layout(
        keys=keys,
        node_file=node_file,
        edge_file=edge_file,
        node_label_file=node_label_file,
        adjacency_file=adjacency_file,
    )
    assert has_parquet_magic(moto_s3_filesystem, node_file)
    assert has_parquet_magic(moto_s3_filesystem, edge_file)
    assert has_parquet_magic(moto_s3_filesystem, node_label_file)
    assert has_parquet_magic(moto_s3_filesystem, adjacency_file)
    assert node_table.to_pylist() == [
        {
            "node_id": 1,
            "labels": ["Person"],
            "properties_json": '{"external_id": "alice", "name": "Alice"}',
        },
        {
            "node_id": 2,
            "labels": ["Person"],
            "properties_json": '{"external_id": "bob", "name": "Bob"}',
        },
    ]
    assert edge_table.to_pylist() == [
        {
            "edge_id": 1,
            "rel_type": "KNOWS",
            "src_id": 1,
            "dst_id": 2,
            "properties_json": '{"since": 2024}',
        }
    ]
    assert node_label_table.to_pylist() == [{"node_id": 1}, {"node_id": 2}]
    assert adjacency_table.to_pylist() == [{"src_id": 1, "dst_id": 2, "edge_id": 1}]


@pytest.mark.integration
def test_one_rest_server_preserves_ten_concurrent_s3_writers(
    moto_s3_filesystem: MotoS3Context,
) -> None:
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    from grapharrow.use_cases.ana import GraphArrowAgentBackend

    app = create_rest_app(
        f"{moto_s3_filesystem.bucket_uri}/shared-writers",
        filesystem=moto_s3_filesystem.filesystem,
    )
    graph = app.state.grapharrow.use_cases.graph
    with TestClient(app) as client:
        created = client.post(
            "/v1/query",
            json={"query": "CREATE (:Person)-[:LINK {value:0}]->(:Person)"},
        )
        assert created.status_code == 200
        initial = graph.capture_snapshot()
        barrier = Barrier(10)

        def increment(index: int) -> list[int]:
            barrier.wait(timeout=10)
            observed = []
            for _ in range(2):
                response = client.post(
                    "/v1/query",
                    json={
                        "query": (
                            "MATCH ()-[r:LINK]->() SET r.value=r.value+1 RETURN r.value AS value"
                        )
                    },
                )
                assert response.status_code == 200, response.text
                observed.append(response.json()["data"]["rows"][0]["value"])
            return observed

        with GraphArrowAgentBackend(app.state.grapharrow):
            with ThreadPoolExecutor(max_workers=10) as pool:
                values = list(pool.map(increment, range(10)))
            assert sorted(value for batch in values for value in batch) == list(range(1, 21))
            assert graph.capture_snapshot().snapshot_id.value == initial.snapshot_id.value + 20
            assert graph.load_state().edges[0].properties["value"] == 20
            vacuum = client.post("/v1/admin/vacuum")
            assert vacuum.status_code == 200
            assert graph.load_state(snapshot=initial).edges[0].properties["value"] == 0
            assert app.state.grapharrow.use_cases.admin.verify(verify_checksums=True).ok


@pytest.mark.integration
def test_rest_point_set_preserves_s3_snapshot_and_avoids_full_state(
    moto_s3_filesystem: MotoS3Context, monkeypatch: pytest.MonkeyPatch
) -> None:
    from typing import Any

    from grapharrow.use_cases.ana import GraphArrowAgentBackend

    app = create_rest_app(
        f"{moto_s3_filesystem.bucket_uri}/point-set", filesystem=moto_s3_filesystem.filesystem
    )
    with TestClient(app) as client:
        assert (
            client.post(
                "/v1/query", json={"query": "CREATE ()-[:LINK {value:'old'}]->()"}
            ).status_code
            == 200
        )
        graph = app.state.grapharrow.use_cases.graph
        initial = graph.capture_snapshot()
        with GraphArrowAgentBackend(app.state.grapharrow):

            def no_full_state(*args: Any, **kwargs: Any) -> Any:
                pytest.fail("REST point SET must not materialize the complete graph")

            with monkeypatch.context() as patch:
                patch.setattr(graph, "load_state", no_full_state)
                updated = client.post(
                    "/v1/query",
                    json={
                        "query": (
                            "MATCH ()-[r:LINK]->() WHERE id(r)=1 "
                            "SET r.value='new' RETURN r.value AS value"
                        )
                    },
                )
            assert updated.status_code == 200, updated.text
            assert updated.json()["data"]["rows"] == [{"value": "new"}]
            assert graph.load_state(snapshot=initial).edges[0].properties["value"] == "old"
            assert graph.load_state().edges[0].properties["value"] == "new"
            assert client.post("/v1/admin/vacuum").status_code == 200
            assert app.state.grapharrow.use_cases.admin.verify(verify_checksums=True).ok
