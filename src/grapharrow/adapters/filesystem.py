"""Filesystem adapter built on top of ``pyarrow.fs``."""

from __future__ import annotations

import json
import os
from contextlib import suppress
from pathlib import Path
from stat import S_IMODE
from typing import cast
from urllib.parse import ParseResult, urlparse
from uuid import uuid4

import pyarrow.fs as pafs  # type: ignore[import-untyped]

from grapharrow.config import GraphArrowSettings


class FileSystemAdapter:
    """Resolve local paths and URIs through a small ``pyarrow.fs`` wrapper."""

    def __init__(
        self,
        *,
        mounts: dict[str, tuple[pafs.FileSystem, str]] | None = None,
        scheme_filesystems: dict[str, pafs.FileSystem] | None = None,
    ) -> None:
        self._local = pafs.LocalFileSystem()
        self._mounts = {
            prefix.rstrip("/"): (filesystem, base_path.strip("/"))
            for prefix, (filesystem, base_path) in (mounts or {}).items()
        }
        self._scheme_filesystems = {
            scheme.rstrip(":/").lower(): filesystem
            for scheme, filesystem in (scheme_filesystems or {}).items()
        }

    @classmethod
    def from_settings(cls, settings: GraphArrowSettings) -> FileSystemAdapter:
        """Build a filesystem adapter from explicit GraphArrow storage settings."""

        scheme_filesystems: dict[str, pafs.FileSystem] = {}

        s3_options = {
            "access_key": settings.s3_access_key,
            "secret_key": settings.s3_secret_key,
            "session_token": settings.s3_session_token,
            "region": settings.s3_region,
            "endpoint_override": settings.s3_endpoint,
            "scheme": settings.s3_scheme,
            "anonymous": settings.s3_anonymous,
        }
        if (
            any(value is not None for key, value in s3_options.items() if key != "anonymous")
            or settings.s3_anonymous
        ):
            scheme_filesystems["s3"] = pafs.S3FileSystem(
                access_key=settings.s3_access_key,
                secret_key=settings.s3_secret_key,
                session_token=settings.s3_session_token,
                region=settings.s3_region,
                endpoint_override=settings.s3_endpoint,
                scheme=settings.s3_scheme,
                anonymous=settings.s3_anonymous,
            )

        gcs_has_explicit_config = (
            any(
                value is not None
                for value in (
                    settings.gcs_access_token,
                    settings.gcs_credential_token_expiration,
                    settings.gcs_project_id,
                    settings.gcs_endpoint,
                    settings.gcs_scheme,
                )
            )
            or settings.gcs_anonymous
        )
        if gcs_has_explicit_config:
            scheme_filesystems["gs"] = pafs.GcsFileSystem(
                anonymous=settings.gcs_anonymous,
                access_token=settings.gcs_access_token,
                credential_token_expiration=settings.gcs_credential_token_expiration,
                project_id=settings.gcs_project_id,
                endpoint_override=settings.gcs_endpoint,
                scheme=settings.gcs_scheme,
            )

        azure_has_explicit_config = any(
            value is not None
            for value in (
                settings.azure_account_name,
                settings.azure_account_key,
                settings.azure_sas_token,
                settings.azure_client_id,
                settings.azure_client_secret,
                settings.azure_tenant_id,
                settings.azure_blob_storage_authority,
                settings.azure_blob_storage_scheme,
                settings.azure_dfs_storage_authority,
                settings.azure_dfs_storage_scheme,
            )
        )
        if azure_has_explicit_config:
            if settings.azure_account_name is None:
                raise ValueError(
                    "GRAPHARROW_AZURE_ACCOUNT_NAME is required when configuring Azure storage."
                )
            scheme_filesystems["az"] = pafs.AzureFileSystem(
                account_name=settings.azure_account_name,
                account_key=settings.azure_account_key,
                sas_token=settings.azure_sas_token,
                client_id=settings.azure_client_id,
                client_secret=settings.azure_client_secret,
                tenant_id=settings.azure_tenant_id,
                blob_storage_authority=settings.azure_blob_storage_authority,
                blob_storage_scheme=settings.azure_blob_storage_scheme,
                dfs_storage_authority=settings.azure_dfs_storage_authority,
                dfs_storage_scheme=settings.azure_dfs_storage_scheme,
            )

        return cls(scheme_filesystems=scheme_filesystems)

    def resolve(self, location: str | Path) -> tuple[pafs.FileSystem, str]:
        """Resolve ``location`` into a filesystem instance and normalized path."""

        if isinstance(location, Path):
            return self._local, str(location)

        text = str(location)
        if "://" not in text:
            return self._local, str(Path(text))

        mounted = self._resolve_mount(text)
        if mounted is not None:
            return mounted

        parsed = urlparse(text)
        scheme = parsed.scheme.lower()
        if scheme in self._scheme_filesystems:
            normalized_path = self._normalize_remote_path(parsed)
            return self._scheme_filesystems[scheme], normalized_path

        filesystem, normalized_path = pafs.FileSystem.from_uri(text)
        return filesystem, normalized_path

    def exists(self, location: str | Path) -> bool:
        """Return whether ``location`` exists."""

        filesystem, normalized_path = self.resolve(location)
        info = filesystem.get_file_info([normalized_path])[0]
        return bool(info.type != pafs.FileType.NotFound)

    def create_dir(self, location: str | Path) -> None:
        """Create ``location`` recursively when supported by the filesystem."""

        filesystem, normalized_path = self.resolve(location)
        filesystem.create_dir(normalized_path, recursive=True)

    def read_text(self, location: str | Path) -> str:
        """Read a UTF-8 text file from ``location``."""

        filesystem, normalized_path = self.resolve(location)
        with filesystem.open_input_file(normalized_path) as handle:
            return cast(bytes, handle.read()).decode("utf-8")

    def write_text(self, location: str | Path, text: str) -> None:
        """Write a UTF-8 text file to ``location``."""

        filesystem, normalized_path = self.resolve(location)
        parent = normalized_path.rsplit("/", 1)[0] if "/" in normalized_path else ""
        if parent:
            filesystem.create_dir(parent, recursive=True)
        with filesystem.open_output_stream(normalized_path) as handle:
            handle.write(text.encode("utf-8"))

    def read_json(self, location: str | Path) -> object:
        """Read and decode JSON from ``location``."""

        return json.loads(self.read_text(location))

    def write_json(self, location: str | Path, payload: object) -> None:
        """Encode and write JSON to ``location``."""

        self.write_text(location, json.dumps(payload, indent=2, sort_keys=True))

    def replace_json(self, location: str | Path, payload: object) -> None:
        """Replace a native local JSON file atomically, or use the normal remote write."""

        filesystem, normalized_path = self.resolve(location)
        if not isinstance(filesystem, pafs.LocalFileSystem):
            self.write_json(location, payload)
            return

        content = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        destination = Path(normalized_path).resolve()
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(f".{destination.name}.{uuid4().hex}.tmp")
        handle = temporary.open("xb")
        try:
            with handle:
                with suppress(FileNotFoundError):
                    temporary.chmod(S_IMODE(destination.stat().st_mode))
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, destination)
        except BaseException:
            with suppress(OSError):
                temporary.unlink(missing_ok=True)
            raise

    def copy_file(self, source: str | Path, destination: str | Path) -> None:
        """Copy a file between locations handled by ``pyarrow.fs``."""

        source_fs, source_path = self.resolve(source)
        destination_fs, destination_path = self.resolve(destination)
        parent = destination_path.rsplit("/", 1)[0] if "/" in destination_path else ""
        if parent:
            destination_fs.create_dir(parent, recursive=True)
        with source_fs.open_input_file(source_path) as source_handle:
            with destination_fs.open_output_stream(destination_path) as destination_handle:
                destination_handle.write(cast(bytes, source_handle.read()))

    def delete_file(self, location: str | Path) -> None:
        """Delete a single file at ``location``."""

        filesystem, normalized_path = self.resolve(location)
        filesystem.delete_file(normalized_path)

    def delete_dir(self, location: str | Path) -> None:
        """Delete a directory tree rooted at ``location``."""

        filesystem, normalized_path = self.resolve(location)
        filesystem.delete_dir_contents(normalized_path, missing_dir_ok=True)
        filesystem.delete_dir(normalized_path)

    def list_paths(self, location: str | Path, *, recursive: bool = False) -> tuple[str, ...]:
        """List file and directory paths below ``location``."""

        filesystem, normalized_path = self.resolve(location)
        selector = pafs.FileSelector(
            normalized_path,
            recursive=recursive,
            allow_not_found=True,
        )
        infos = filesystem.get_file_info(selector)
        paths = [info.path for info in infos if info.type != pafs.FileType.NotFound]

        if isinstance(location, Path):
            return tuple(str(Path(path)) for path in paths)

        mount_prefix = self._matching_mount_prefix(str(location))
        if mount_prefix is not None:
            mounted_base = self._mounts[mount_prefix][1]
            return tuple(
                self._mount_path_to_uri(mount_prefix, mounted_base, path) for path in paths
            )

        prefix = str(location).rstrip("/")
        return tuple(f"{prefix}/{path.removeprefix(normalized_path).lstrip('/')}" for path in paths)

    def _resolve_mount(self, location: str) -> tuple[pafs.FileSystem, str] | None:
        prefix = self._matching_mount_prefix(location)
        if prefix is None:
            return None

        filesystem, base_path = self._mounts[prefix]
        suffix = location.removeprefix(prefix).lstrip("/")
        if base_path and suffix:
            return filesystem, f"{base_path}/{suffix}"
        if base_path:
            return filesystem, base_path
        return filesystem, suffix

    def _matching_mount_prefix(self, location: str) -> str | None:
        for prefix in sorted(self._mounts, key=len, reverse=True):
            if location == prefix or location.startswith(f"{prefix}/"):
                return prefix
        return None

    def _mount_path_to_uri(self, prefix: str, base_path: str, normalized_path: str) -> str:
        suffix = normalized_path
        if base_path:
            suffix = normalized_path.removeprefix(base_path).lstrip("/")
        return f"{prefix}/{suffix}" if suffix else prefix

    def _normalize_remote_path(self, parsed: ParseResult) -> str:
        netloc = parsed.netloc.strip("/")
        path = parsed.path.lstrip("/")
        if netloc and path:
            return f"{netloc}/{path}"
        return netloc or path


__all__ = ["FileSystemAdapter"]
