# Real Object Storage Testing

GraphArrow already validates remote-style storage behavior through URI-aware integration tests and
through S3-compatible tests backed by `moto`. This document describes the extra live integration
tests that can be used against real cloud object storage accounts.

## Supported Providers

The live test harness currently supports:

- AWS S3
- Google Cloud Storage
- Azure Blob Storage

The tests are intentionally optional. They run only when the corresponding environment variable is
configured with a writable base URI.

## Environment Variables

Set one or more of these variables before running the live suite:

- `GRAPHARROW_TEST_REAL_S3_URI`
- `GRAPHARROW_TEST_REAL_GCS_URI`
- `GRAPHARROW_TEST_REAL_AZURE_URI`

Each variable must point to a writable base prefix, not to a pre-existing database directory.

Examples:

```bash
export GRAPHARROW_TEST_REAL_S3_URI="s3://my-grapharrow-tests/bootstrap"
export GRAPHARROW_TEST_REAL_GCS_URI="gs://my-grapharrow-tests/bootstrap"
export GRAPHARROW_TEST_REAL_AZURE_URI="az://grapharrow-tests/bootstrap"
```

For each test run, GraphArrow appends a random subdirectory below the configured prefix:

```text
<base-uri>/<random-run-id>/example-db
```

This keeps the tests isolated and allows best-effort cleanup without touching unrelated data.

## Credentials

Credentials are delegated to `pyarrow.fs.FileSystem.from_uri(...)` and the underlying cloud SDK
support compiled into the local Arrow build.

Typical provider expectations are:

- S3: AWS credentials or IAM role available to the runtime
- GCS: application default credentials or equivalent GCS auth available to the runtime
- Azure: storage credentials or identity configuration available to the runtime

The exact mechanism depends on the local Arrow build and provider configuration.

GraphArrow can also build explicit cloud filesystem clients from settings and environment
variables before falling back to provider-default discovery.

Examples:

- S3:
  - `GRAPHARROW_S3_ACCESS_KEY`
  - `GRAPHARROW_S3_SECRET_KEY`
  - `GRAPHARROW_S3_SESSION_TOKEN`
  - `GRAPHARROW_S3_REGION`
  - `GRAPHARROW_S3_ENDPOINT`
  - `GRAPHARROW_S3_SCHEME`
  - `GRAPHARROW_S3_ANONYMOUS`
- GCS:
  - `GRAPHARROW_GCS_ANONYMOUS`
  - `GRAPHARROW_GCS_ACCESS_TOKEN`
  - `GRAPHARROW_GCS_CREDENTIAL_TOKEN_EXPIRATION`
  - `GRAPHARROW_GCS_PROJECT_ID`
  - `GRAPHARROW_GCS_ENDPOINT`
  - `GRAPHARROW_GCS_SCHEME`
- Azure:
  - `GRAPHARROW_AZURE_ACCOUNT_NAME`
  - `GRAPHARROW_AZURE_ACCOUNT_KEY`
  - `GRAPHARROW_AZURE_SAS_TOKEN`
  - `GRAPHARROW_AZURE_CLIENT_ID`
  - `GRAPHARROW_AZURE_CLIENT_SECRET`
  - `GRAPHARROW_AZURE_TENANT_ID`
  - `GRAPHARROW_AZURE_BLOB_STORAGE_AUTHORITY`
  - `GRAPHARROW_AZURE_BLOB_STORAGE_SCHEME`
  - `GRAPHARROW_AZURE_DFS_STORAGE_AUTHORITY`
  - `GRAPHARROW_AZURE_DFS_STORAGE_SCHEME`

These settings are optional. If they are not present, GraphArrow continues to rely on the default
credential mechanisms provided by the provider SDK integration inside Arrow.

## Running the Tests

Run the normal live integration target:

```bash
make test-integration-live
```

Or run the file directly:

```bash
uv run pytest -m "integration and live_storage" tests/integration/test_live_object_storage.py
```

If a provider-specific environment variable is missing, the corresponding test is skipped.

## What the Live Tests Validate

Each live test verifies:

- database bootstrap against a real cloud URI
- write and read roundtrip through the GraphArrow API
- `verify(verify_checksums=True)`
- existence of the expected storage layout
- manifest and snapshot pointer readability
- Parquet format for canonical and derived datasets
- expected content for nodes, edges, node-label data, and adjacency data

## Notes

- These tests are intentionally narrower than the full local and `moto` suites.
- They are meant to confirm that the URI-aware storage model works on real providers.
- REST and Bolt already exercise the same storage protocol through local disk and S3-compatible
  integration tests; once live provider credentials are available in CI or a staging environment,
  the same approach can be extended there too.
