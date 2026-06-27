"""Runtime settings for GraphArrow."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class GraphArrowSettings(BaseSettings):
    """Project-wide settings loaded from environment variables when present."""

    model_config = SettingsConfigDict(
        env_prefix="GRAPHARROW_",
        env_file=".env",
        extra="ignore",
    )

    environment: str = Field(default="development")
    database_path: str | Path | None = Field(default=None)
    verify_checksums_on_verify: bool = Field(default=False)
    server_host: str = Field(default="127.0.0.1")
    server_port: int = Field(default=8000)
    rest_prefix: str = Field(default="/v1")
    server_auth_token: str | None = Field(default=None)
    bolt_host: str = Field(default="127.0.0.1")
    bolt_port: int = Field(default=7687)
    bolt_auth_token: str | None = Field(default=None)
    bolt_server_agent: str = Field(default="Neo4j/5.26.0")
    bolt_log_queries: bool = Field(default=True)

    s3_access_key: str | None = Field(default=None)
    s3_secret_key: str | None = Field(default=None)
    s3_session_token: str | None = Field(default=None)
    s3_region: str | None = Field(default=None)
    s3_endpoint: str | None = Field(default=None)
    s3_scheme: str | None = Field(default=None)
    s3_anonymous: bool = Field(default=False)

    gcs_anonymous: bool = Field(default=False)
    gcs_access_token: str | None = Field(default=None)
    gcs_credential_token_expiration: datetime | None = Field(default=None)
    gcs_project_id: str | None = Field(default=None)
    gcs_endpoint: str | None = Field(default=None)
    gcs_scheme: str | None = Field(default=None)

    azure_account_name: str | None = Field(default=None)
    azure_account_key: str | None = Field(default=None)
    azure_sas_token: str | None = Field(default=None)
    azure_client_id: str | None = Field(default=None)
    azure_client_secret: str | None = Field(default=None)
    azure_tenant_id: str | None = Field(default=None)
    azure_blob_storage_authority: str | None = Field(default=None)
    azure_blob_storage_scheme: str | None = Field(default=None)
    azure_dfs_storage_authority: str | None = Field(default=None)
    azure_dfs_storage_scheme: str | None = Field(default=None)
