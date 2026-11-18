"""Verify the providers send the conditions that protect the database head."""

from io import BytesIO
from urllib.parse import parse_qs, urlparse

import pytest
from requests import PreparedRequest, Response, Session
from requests.structures import CaseInsensitiveDict
from urllib3.response import HTTPResponse

from grapharrow.adapters.filesystem import FileSystemAdapter
from grapharrow.config import GraphArrowSettings


def test_azure_blob_pointer_uses_etags_from_the_content_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("azure.storage.blob")
    pytest.importorskip("azure.identity")
    settings = GraphArrowSettings(
        azure_account_name="grapharrow",
        azure_account_key="dGVzdA==",
        azure_blob_storage_authority="localhost:10000",
        azure_blob_storage_scheme="http",
    )
    fs = FileSystemAdapter.from_settings(settings)
    location = "az://graphgraphs/db/current_snapshot.json"
    requests: list[PreparedRequest] = []

    def send(session: Session, request: PreparedRequest, **kwargs: object) -> Response:
        requests.append(request)
        response = Response()
        response.request = request
        response.status_code = 201
        response.headers = CaseInsensitiveDict(
            {"ETag": '"old"', "Content-Type": "application/json"}
        )
        data = b""
        if request.method == "GET":
            response.status_code = 206
            data = b'{"snapshot_id":1}'
            response.headers["Content-Range"] = f"bytes 0-{len(data) - 1}/{len(data)}"
            response.headers["x-ms-blob-type"] = "BlockBlob"
        elif request.headers.get("If-Match") == '"stale"':
            response.status_code = 412
            response.headers["x-ms-error-code"] = "ConditionNotMet"
        response.headers["Content-Length"] = str(len(data))
        response.raw = BytesIO(data)
        return response

    monkeypatch.setattr(Session, "send", send)
    assert fs.read_pointer(location) == ({"snapshot_id": 1}, '"old"')
    assert fs.replace_pointer(location, {"snapshot_id": 1}, None)
    assert not fs.replace_pointer(location, {"snapshot_id": 2}, '"stale"')
    assert fs.replace_pointer(location, {"snapshot_id": 2}, '"old"')

    assert all(
        request.url == "http://localhost:10000/grapharrow/graphgraphs/db/current_snapshot.json"
        for request in requests
    )
    assert requests[1].headers["If-None-Match"] == "*"
    assert requests[2].headers["If-Match"] == '"stale"'
    assert requests[3].headers["If-Match"] == '"old"'


def test_gcs_pointer_requires_the_same_object_generation(monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("google.cloud.storage")
    fs = FileSystemAdapter.from_settings(
        GraphArrowSettings(
            gcs_anonymous=True,
            gcs_project_id="grapharrow-tests",
            gcs_endpoint="http://localhost:4443",
        )
    )
    location = "gs://graphs/db/current_snapshot.json"
    conditions: list[str] = []

    def send(session: Session, request: PreparedRequest, **kwargs: object) -> Response:
        assert request.url is not None
        query = parse_qs(urlparse(request.url).query)
        response = Response()
        response.request = request
        response.status_code = 200
        response.headers = CaseInsensitiveDict({"Content-Type": "application/json"})
        data = b'{"generation":"42","name":"db/current_snapshot.json","bucket":"graphs"}'
        if "ifGenerationMatch" in query:
            condition = query["ifGenerationMatch"][0]
            conditions.append(condition)
            if condition == "40":
                response.status_code = 412
                data = b'{"error":{"message":"condition failed"}}'
        if query.get("alt") == ["media"]:
            data = b'{"snapshot_id":1}'
        response._content = data
        response.headers["Content-Length"] = str(len(data))
        response.raw = HTTPResponse(
            body=BytesIO(data), headers=response.headers, preload_content=False
        )
        return response

    monkeypatch.setattr(Session, "send", send)
    assert fs.read_pointer(location) == ({"snapshot_id": 1}, 42)
    assert fs.replace_pointer(location, {"snapshot_id": 1}, None)
    assert not fs.replace_pointer(location, {"snapshot_id": 2}, 40)
    assert fs.replace_pointer(location, {"snapshot_id": 2}, 42)
    assert conditions == ["42", "0", "40", "42"]
