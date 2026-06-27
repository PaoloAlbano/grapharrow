from pathlib import Path

import pyarrow.fs as pafs  # type: ignore[import-untyped]

from grapharrow.adapters import FileSystemAdapter
from grapharrow.config import GraphArrowSettings
from grapharrow.interfaces.bolt import (
    PackstreamStructure,
    _packstream_result_value,
    build_bolt_application,
    run_bolt_server,
)


def test_build_bolt_application_uses_settings_and_database_path(tmp_path: Path) -> None:
    database_path = tmp_path / "example-db"
    settings = GraphArrowSettings(
        database_path=database_path,
        bolt_host="0.0.0.0",
        bolt_port=8765,
        bolt_auth_token="secret",
    )

    application = build_bolt_application(database_path, settings=settings)

    assert application.grapharrow.layout.root == database_path.resolve()
    assert application.host == "0.0.0.0"
    assert application.port == 8765
    assert application.auth_token == "secret"


def test_build_bolt_application_supports_uri_paths_with_custom_filesystem() -> None:
    filesystem = FileSystemAdapter(
        mounts={"s3://grapharrow-tests": (pafs._MockFileSystem(), "grapharrow-tests")}
    )

    application = build_bolt_application(
        "s3://grapharrow-tests/example-db",
        filesystem=filesystem,
    )

    assert application.grapharrow.layout.root == "s3://grapharrow-tests/example-db"


def test_packstream_result_value_encodes_regular_graph_entities() -> None:
    node = _packstream_result_value(
        "n",
        {"id": 1, "labels": ["Location"], "properties": {"name": "Castle Black"}},
    )
    relationship = _packstream_result_value(
        "r",
        {
            "id": 2,
            "type": "HAPPENED_IN",
            "source": 3,
            "target": 1,
            "properties": {},
        },
    )

    assert isinstance(node, PackstreamStructure)
    assert node.signature == ord("N")
    assert node.fields == [1, ["Location"], {"name": "Castle Black"}]
    assert isinstance(relationship, PackstreamStructure)
    assert relationship.signature == ord("R")
    assert relationship.fields == [2, 3, 1, "HAPPENED_IN", {}]


def test_run_bolt_server_starts_and_stops_on_ephemeral_port(tmp_path: Path) -> None:
    database_path = tmp_path / "example-db"
    settings = GraphArrowSettings(
        database_path=database_path,
        bolt_host="127.0.0.1",
        bolt_port=0,
    )

    with run_bolt_server(database_path, settings=settings) as handle:
        assert handle.host == "127.0.0.1"
        assert handle.port > 0
        assert handle.thread.is_alive() is True

    assert handle.thread.is_alive() is False
