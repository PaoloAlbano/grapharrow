"""Arrow REST payloads use the same graph and write receipt as JSON."""

import json
from pathlib import Path

import pyarrow as pa
import pytest
from fastapi.testclient import TestClient

from grapharrow.interfaces.rest import create_rest_app
from grapharrow.interfaces.rest_arrow import ARROW_FILE, ARROW_STREAM
from grapharrow.use_cases import cypher as engine


def _ipc(table: pa.Table, media_type: str) -> bytes:
    sink = pa.BufferOutputStream()
    factory = pa.ipc.new_stream if media_type == ARROW_STREAM else pa.ipc.new_file
    with factory(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


@pytest.mark.parametrize("media_type", [ARROW_STREAM, ARROW_FILE])
def test_arrow_bulk_ingest_and_query(tmp_path: Path, media_type: str) -> None:
    client = TestClient(create_rest_app(tmp_path / "graph"))
    headers = {"Content-Type": media_type, "Accept": media_type}
    read = pa.ipc.open_stream if media_type == ARROW_STREAM else pa.ipc.open_file

    nodes = client.post(
        "/v1/nodes/bulk",
        params={"default_labels": "Person"},
        content=_ipc(pa.table({"name": ["Ada", "Grace"]}), media_type),
        headers=headers,
    )
    assert nodes.status_code == 201, nodes.text
    assert nodes.headers["content-type"] == media_type
    table = read(nodes.content).read_all()
    assert table["id"].to_pylist() == [1, 2]
    assert table["labels"].to_pylist() == [["Person"], ["Person"]]
    assert json.loads(table["properties"][0].as_py()) == {"name": "Ada"}

    body = pa.table(
        {"src_key": ["Ada"], "dst_key": ["Grace"], "rel_type": ["KNOWS"], "since": [2024]}
    )
    edges = client.post(
        "/v1/edges/bulk",
        params={
            "source_label": "Person",
            "source_key": "name",
            "target_label": "Person",
            "target_key": "name",
        },
        content=_ipc(body, media_type),
        headers=headers,
    )
    assert edges.status_code == 201, edges.text
    assert read(edges.content).read_all().select(["source", "target"]).to_pylist() == [
        {"source": 1, "target": 2}
    ]

    payload = {
        "query": "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name AS name, r.since AS since"
    }
    result = client.post("/v1/query", json=payload, headers={"Accept": media_type})
    assert result.status_code == 200
    assert result.headers["content-type"] == media_type and result.headers["vary"] == "Accept"
    assert read(result.content).read_all().to_pylist() == [{"name": "Ada", "since": 2024}]
    assert client.post("/v1/query", json=payload).json()["data"]["rows"] == [
        {"name": "Ada", "since": 2024}
    ]


def test_arrow_malformed_payload_cannot_publish(tmp_path: Path) -> None:
    app = create_rest_app(tmp_path / "graph")
    client = TestClient(app)
    graph = app.state.grapharrow.use_cases.graph
    before = graph.capture_snapshot()
    for payload in (b"truncated", _ipc(pa.table([[1], [2]], names=["name", "name"]), ARROW_STREAM)):
        response = client.post(
            "/v1/nodes/bulk", content=payload, headers={"Content-Type": ARROW_STREAM}
        )
        assert response.status_code == 422
    assert graph.capture_snapshot() == before


def test_arrow_query_preserves_sparse_and_heterogeneous_values(tmp_path: Path) -> None:
    client = TestClient(create_rest_app(tmp_path / "graph"))
    payload = {
        "query": (
            "UNWIND [1, 2, 3] AS i RETURN "
            "CASE i WHEN 1 THEN 9007199254740993 WHEN 2 THEN 1.5 ELSE null END AS amount, "
            "CASE i WHEN 1 THEN {a: 1} ELSE {a: null, b: 2} END AS info"
        )
    }
    response = client.post("/v1/query", json=payload, headers={"Accept": ARROW_STREAM})
    assert response.status_code == 200
    table = pa.ipc.open_stream(response.content).read_all()
    assert table.schema.field("amount").metadata == {b"grapharrow.encoding": b"json"}
    assert [
        json.loads(value) if value is not None else None for value in table["amount"].to_pylist()
    ] == [9007199254740993, 1.5, None]
    assert [json.loads(value) for value in table["info"].to_pylist()] == [
        {"a": 1},
        {"a": None, "b": 2},
        {"a": None, "b": 2},
    ]
    preference = f"{ARROW_STREAM};q=0.5, application/json;q=1"
    assert client.post("/v1/query", json=payload, headers={"Accept": preference}).json()[
        "meta"
    ] == {"row_count": 3}


def test_arrow_and_json_retry_share_one_idempotency_receipt(tmp_path: Path) -> None:
    app = create_rest_app(tmp_path / "graph")
    client = TestClient(app)
    graph = app.state.grapharrow.use_cases.graph
    payload = {"query": "CREATE (n:Person {name:'Ada'}) RETURN n.name AS name"}
    headers = {"Idempotency-Key": "arrow-replay"}
    result = client.post("/v1/query", json=payload, headers={**headers, "Accept": ARROW_STREAM})
    assert result.status_code == 200, result.text
    published = graph.capture_snapshot()
    rows = pa.ipc.open_stream(result.content).read_all().to_pylist()
    replay = client.post("/v1/query", json=payload, headers=headers)
    assert replay.status_code == 200
    assert replay.json()["data"]["rows"] == rows == [{"name": "Ada"}]
    assert graph.capture_snapshot() == published


def test_arrow_stream_reuses_the_engine_columns_in_multiple_batches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = create_rest_app(tmp_path / "graph")
    graph = app.state.grapharrow.use_cases.graph
    graph.create_nodes_bulk(
        table=pa.table({"rank": list(range(9000)), "name": ["ada"] * 9000}),
        default_labels=("Person",),
    )
    client = TestClient(app)
    query = {"query": "MATCH (n:Person) RETURN n.rank AS rank, n.name AS name ORDER BY rank"}
    json_rows = client.post("/v1/query", json=query).json()["data"]["rows"]

    def forbid_row_conversion(*args: object, **kwargs: object) -> None:
        pytest.fail("Simple Arrow projection was converted to Python rows.")

    monkeypatch.setattr(engine, "_build_query_result_from_table", forbid_row_conversion)
    response = client.post("/v1/query", json=query, headers={"Accept": ARROW_STREAM})
    assert response.status_code == 200
    assert "content-length" not in response.headers
    with pa.ipc.open_stream(response.content) as reader:
        assert reader.schema.names == ["rank", "name"]
        batches = list(reader)
    assert len(batches) > 1
    assert pa.Table.from_batches(batches).to_pylist() == json_rows

    response = client.post("/v1/query", json=query, headers={"Accept": ARROW_FILE})
    assert pa.ipc.open_file(response.content).read_all().to_pylist() == json_rows


def test_arrow_projection_keeps_the_public_encoding_for_nested_values(tmp_path: Path) -> None:
    app = create_rest_app(tmp_path / "graph")
    client = TestClient(app)
    payload = {"query": "CREATE (:Person {profile:{name:'Ada', active:true}})"}
    assert client.post("/v1/query", json=payload).status_code == 200
    query = {"query": "MATCH (n:Person) RETURN n.profile AS profile"}
    result = client.post("/v1/query", json=query, headers={"Accept": ARROW_STREAM})
    assert result.status_code == 200
    table = pa.ipc.open_stream(result.content).read_all()
    assert table.schema.field("profile").metadata == {b"grapharrow.encoding": b"json"}
    assert json.loads(table["profile"][0].as_py()) == {"active": True, "name": "Ada"}
