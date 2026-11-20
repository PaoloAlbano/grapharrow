"""Conditional object writes for the small database head pointer."""

from __future__ import annotations

from functools import cached_property
from importlib import import_module
from typing import TYPE_CHECKING, Any, Protocol, cast

from grapharrow.config import GraphArrowSettings
from grapharrow.core.errors import StorageError

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential
    from azure.storage.blob import BlobServiceClient
    from google.auth.credentials import Credentials as GoogleCredentials

Revision = str | int


def _require_sdk(extra: str, *modules: str) -> None:
    """Load an SDK only when its provider is selected, with installation guidance."""
    for module in modules:
        try:
            import_module(module)
        except ModuleNotFoundError as exc:
            if exc.name is None or not (module == exc.name or module.startswith(exc.name + ".")):
                raise
            raise StorageError(
                f"Cloud storage requires the '{extra}' extra. "
                f"Install it with: pip install 'grapharrow[{extra}]'"
            ) from exc


class ObjectPointer(Protocol):
    def read(self, bucket: str, key: str) -> tuple[bytes, Revision] | None: ...
    def replace(self, bucket: str, key: str, data: bytes, expected: Revision | None) -> bool: ...


class S3Pointer:
    def __init__(self, settings: GraphArrowSettings) -> None:
        _require_sdk("s3", "boto3")
        self._settings = settings

    @cached_property
    def _client(self) -> Any:
        import boto3  # type: ignore[import-untyped]
        from botocore import UNSIGNED  # type: ignore[import-untyped]
        from botocore.config import Config  # type: ignore[import-untyped]

        settings = self._settings
        endpoint = settings.s3_endpoint
        if endpoint and "://" not in endpoint:
            endpoint = f"{settings.s3_scheme or 'https'}://{endpoint}"
        return boto3.session.Session().client(
            "s3",
            endpoint_url=endpoint,
            region_name=settings.s3_region,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            aws_session_token=settings.s3_session_token,
            config=Config(
                signature_version=UNSIGNED if settings.s3_anonymous else None,
                ignore_configured_endpoint_urls=True,
            ),
        )

    def read(self, bucket: str, key: str) -> tuple[bytes, Revision] | None:
        from botocore.exceptions import ClientError  # type: ignore[import-untyped]

        try:
            response = self._client.get_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            if exc.response["Error"]["Code"] in {"NoSuchKey", "404"}:
                return None
            raise
        body = response["Body"]
        try:
            return body.read(), response["ETag"]
        finally:
            body.close()

    def replace(self, bucket: str, key: str, data: bytes, expected: Revision | None) -> bool:
        from botocore.exceptions import ClientError

        condition = {"IfNoneMatch": "*"} if expected is None else {"IfMatch": expected}
        try:
            self._client.put_object(
                Bucket=bucket, Key=key, Body=data, ContentType="application/json", **condition
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] in {
                "PreconditionFailed",
                "ConditionalRequestConflict",
                "409",
                "412",
            }:
                return False
            raise
        return True


class AzurePointer:
    def __init__(self, settings: GraphArrowSettings, account: str) -> None:
        _require_sdk("azure", "azure.storage.blob", "azure.identity")
        self._settings = settings
        self._account = account

    @cached_property
    def _client(self) -> BlobServiceClient:
        from azure.core.credentials import AzureNamedKeyCredential
        from azure.identity import ClientSecretCredential, DefaultAzureCredential
        from azure.storage.blob import BlobServiceClient

        settings = self._settings
        authority = settings.azure_blob_storage_authority or ".blob.core.windows.net"
        authority = authority.rstrip("/").removesuffix(f"/{self._account}")
        scheme = settings.azure_blob_storage_scheme or "https"
        endpoint = (
            f"{self._account}{authority}"
            if authority.startswith(".")
            else f"{authority.rstrip('/')}/{self._account}"
        )
        credential: str | AzureNamedKeyCredential | TokenCredential | None = (
            settings.azure_sas_token
        )
        if settings.azure_account_key:
            credential = AzureNamedKeyCredential(self._account, settings.azure_account_key)
        if credential is None:
            if (
                settings.azure_tenant_id
                and settings.azure_client_id
                and settings.azure_client_secret
            ):
                credential = ClientSecretCredential(
                    settings.azure_tenant_id, settings.azure_client_id, settings.azure_client_secret
                )
            else:
                credential = DefaultAzureCredential(
                    managed_identity_client_id=settings.azure_client_id
                )
        return BlobServiceClient(f"{scheme}://{endpoint}", credential=credential)

    def read(self, bucket: str, key: str) -> tuple[bytes, Revision] | None:
        from azure.core.exceptions import ResourceNotFoundError

        try:
            download = self._client.get_blob_client(bucket, key).download_blob()
        except ResourceNotFoundError:
            return None
        return download.readall(), download.properties.etag

    def replace(self, bucket: str, key: str, data: bytes, expected: Revision | None) -> bool:
        from azure.core import MatchConditions
        from azure.core.exceptions import HttpResponseError
        from azure.storage.blob import ContentSettings

        blob = self._client.get_blob_client(bucket, key)
        content_settings = ContentSettings(content_type="application/json")
        try:
            if expected is None:
                blob.upload_blob(data, overwrite=False, content_settings=content_settings)
            else:
                blob.upload_blob(
                    data,
                    overwrite=True,
                    etag=cast(str, expected),
                    match_condition=MatchConditions.IfNotModified,
                    content_settings=content_settings,
                )
        except HttpResponseError as exc:
            if exc.status_code == 412 or (
                expected is None
                and getattr(exc, "error_code", None) in {"BlobAlreadyExists", "ConditionNotMet"}
            ):
                return False
            raise
        return True


class GCSPointer:
    def __init__(self, settings: GraphArrowSettings) -> None:
        _require_sdk("gcs", "google.cloud.storage")
        self._settings = settings

    @cached_property
    def _client(self) -> Any:
        from google.auth.credentials import AnonymousCredentials
        from google.cloud.storage import Client  # type: ignore[import-untyped]
        from google.oauth2.credentials import Credentials

        settings = self._settings
        credentials: GoogleCredentials | None = None
        if settings.gcs_anonymous:
            credentials = AnonymousCredentials()  # type: ignore[no-untyped-call]
        elif settings.gcs_access_token:
            credentials = Credentials(settings.gcs_access_token)  # type: ignore[no-untyped-call]
        endpoint = settings.gcs_endpoint
        if endpoint and "://" not in endpoint:
            endpoint = f"{settings.gcs_scheme or 'https'}://{endpoint}"
        return Client(
            project=settings.gcs_project_id or ("" if credentials is not None else None),
            credentials=credentials,
            client_options={"api_endpoint": endpoint} if endpoint else None,
        )

    def read(self, bucket: str, key: str) -> tuple[bytes, Revision] | None:
        from google.api_core.exceptions import NotFound, PreconditionFailed

        container = self._client.bucket(bucket)
        for _ in range(4):
            blob = container.get_blob(key)
            if blob is None:
                return None
            generation = cast(int, blob.generation)
            try:
                return blob.download_as_bytes(if_generation_match=generation), generation
            except (NotFound, PreconditionFailed):
                # The head changed between the metadata and content requests.
                continue
        raise OSError("Could not read a stable GCS database pointer.")

    def replace(self, bucket: str, key: str, data: bytes, expected: Revision | None) -> bool:
        from google.api_core.exceptions import PreconditionFailed

        blob = self._client.bucket(bucket).blob(key)
        try:
            blob.upload_from_string(
                data, content_type="application/json", if_generation_match=expected or 0
            )
        except PreconditionFailed:
            return False
        return True
