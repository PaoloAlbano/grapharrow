from __future__ import annotations

import os
import shutil
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import boto3  # type: ignore[import-untyped]
import pyarrow.fs as pafs  # type: ignore[import-untyped]
import pytest
from moto.server import ThreadedMotoServer

from grapharrow.adapters import FileSystemAdapter
from tests.integration.support import LocalDiskContext, MotoS3Context, RemoteObjectStorageContext


@pytest.fixture
def moto_s3_filesystem() -> Iterator[MotoS3Context]:
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    endpoint_url = f"http://{host}:{port}"

    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )
    bucket_name = f"grapharrow-tests-{uuid4().hex[:12]}"
    client.create_bucket(Bucket=bucket_name)

    s3_filesystem = pafs.S3FileSystem(
        access_key="test",
        secret_key="test",
        region="us-east-1",
        scheme="http",
        endpoint_override=f"{host}:{port}",
    )
    filesystem = FileSystemAdapter(mounts={f"s3://{bucket_name}": (s3_filesystem, bucket_name)})

    try:
        yield MotoS3Context(
            filesystem=filesystem,
            client=client,
            bucket_name=bucket_name,
            bucket_uri=f"s3://{bucket_name}",
            endpoint_url=endpoint_url,
        )
    finally:
        server.stop()


@pytest.fixture
def local_disk_database() -> Iterator[LocalDiskContext]:
    runtime_root = Path(__file__).parent / ".runtime"
    runtime_root.mkdir(parents=True, exist_ok=True)
    root_dir = runtime_root / uuid4().hex[:12]
    database_dir = root_dir / "example-db"
    database_dir.mkdir(parents=True, exist_ok=True)

    try:
        yield LocalDiskContext(root_dir=root_dir, database_dir=database_dir)
    finally:
        shutil.rmtree(root_dir, ignore_errors=True)


def _live_object_storage_context(
    *,
    provider: str,
    env_name: str,
) -> Iterator[RemoteObjectStorageContext]:
    base_uri = os.environ.get(env_name)
    if not base_uri:
        pytest.skip(f"{env_name} is not configured.")

    filesystem = FileSystemAdapter()
    root_uri = f"{base_uri.rstrip('/')}/{uuid4().hex[:12]}"
    database_uri = f"{root_uri}/example-db"

    try:
        yield RemoteObjectStorageContext(
            filesystem=filesystem,
            provider=provider,
            root_uri=root_uri,
            database_uri=database_uri,
        )
    finally:
        try:
            filesystem.delete_dir(root_uri)
        except Exception:
            pass


@pytest.fixture
def live_s3_database() -> Iterator[RemoteObjectStorageContext]:
    yield from _live_object_storage_context(
        provider="s3",
        env_name="GRAPHARROW_TEST_REAL_S3_URI",
    )


@pytest.fixture
def live_gcs_database() -> Iterator[RemoteObjectStorageContext]:
    yield from _live_object_storage_context(
        provider="gcs",
        env_name="GRAPHARROW_TEST_REAL_GCS_URI",
    )


@pytest.fixture
def live_azure_database() -> Iterator[RemoteObjectStorageContext]:
    yield from _live_object_storage_context(
        provider="azure",
        env_name="GRAPHARROW_TEST_REAL_AZURE_URI",
    )
