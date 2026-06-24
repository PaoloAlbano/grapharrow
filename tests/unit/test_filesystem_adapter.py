from pathlib import Path
from typing import cast

import pyarrow.fs as pafs  # type: ignore[import-untyped]
import pytest

from grapharrow.adapters import FileSystemAdapter
from grapharrow.config import GraphArrowSettings


def test_filesystem_adapter_resolves_local_paths(tmp_path: Path) -> None:
    adapter = FileSystemAdapter()

    filesystem, normalized_path = adapter.resolve(tmp_path / "example-db")

    assert isinstance(filesystem, pafs.LocalFileSystem)
    assert normalized_path.endswith("example-db")


def test_filesystem_adapter_resolves_file_uris(tmp_path: Path) -> None:
    adapter = FileSystemAdapter()

    filesystem, normalized_path = adapter.resolve((tmp_path / "example-db").as_uri())

    assert isinstance(filesystem, pafs.LocalFileSystem)
    assert normalized_path.endswith("example-db")


def test_filesystem_adapter_resolves_configured_scheme_filesystems() -> None:
    mock_filesystem = object()
    adapter = FileSystemAdapter(scheme_filesystems={"s3": cast(pafs.FileSystem, mock_filesystem)})

    filesystem, normalized_path = adapter.resolve("s3://bucket/prefix/example-db")

    assert filesystem is mock_filesystem
    assert normalized_path == "bucket/prefix/example-db"


def test_filesystem_adapter_from_settings_builds_configured_cloud_backends(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: dict[str, dict[str, object]] = {}

    class FakeS3FileSystem:
        def __init__(self, **kwargs: object) -> None:
            created["s3"] = kwargs

    class FakeGcsFileSystem:
        def __init__(self, **kwargs: object) -> None:
            created["gs"] = kwargs

    class FakeAzureFileSystem:
        def __init__(self, account_name: str, **kwargs: object) -> None:
            created["az"] = {"account_name": account_name, **kwargs}

    monkeypatch.setattr(pafs, "S3FileSystem", FakeS3FileSystem)
    monkeypatch.setattr(pafs, "GcsFileSystem", FakeGcsFileSystem)
    monkeypatch.setattr(pafs, "AzureFileSystem", FakeAzureFileSystem)

    settings = GraphArrowSettings(
        s3_access_key="access",
        s3_secret_key="secret",
        s3_region="eu-west-1",
        s3_endpoint="localhost:9000",
        s3_scheme="http",
        gcs_anonymous=True,
        gcs_project_id="grapharrow-test",
        azure_account_name="grapharrowstorage",
        azure_account_key="azure-secret",
    )

    adapter = FileSystemAdapter.from_settings(settings)

    s3_filesystem, s3_path = adapter.resolve("s3://bucket/example-db")
    gcs_filesystem, gcs_path = adapter.resolve("gs://bucket/example-db")
    azure_filesystem, azure_path = adapter.resolve("az://container/example-db")

    assert isinstance(s3_filesystem, FakeS3FileSystem)
    assert s3_path == "bucket/example-db"
    assert created["s3"]["access_key"] == "access"
    assert created["s3"]["secret_key"] == "secret"
    assert created["s3"]["region"] == "eu-west-1"
    assert created["s3"]["endpoint_override"] == "localhost:9000"
    assert created["s3"]["scheme"] == "http"

    assert isinstance(gcs_filesystem, FakeGcsFileSystem)
    assert gcs_path == "bucket/example-db"
    assert created["gs"]["anonymous"] is True
    assert created["gs"]["project_id"] == "grapharrow-test"

    assert isinstance(azure_filesystem, FakeAzureFileSystem)
    assert azure_path == "container/example-db"
    assert created["az"]["account_name"] == "grapharrowstorage"
    assert created["az"]["account_key"] == "azure-secret"


def test_filesystem_adapter_from_settings_requires_azure_account_name() -> None:
    settings = GraphArrowSettings(azure_account_key="azure-secret")

    with pytest.raises(ValueError, match="GRAPHARROW_AZURE_ACCOUNT_NAME"):
        FileSystemAdapter.from_settings(settings)
