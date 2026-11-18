"""Bounded sequential checksums for immutable graph objects."""

from __future__ import annotations

import hashlib
from pathlib import Path

from grapharrow.adapters.filesystem import FileSystemAdapter

_CHECKSUM_BLOCK_BYTES = 1024 * 1024


def sha256_file(path: str | Path, *, filesystem: FileSystemAdapter) -> str:
    """Hash stored bytes without issuing a random-access GET for every small block."""
    resolved, normalized = filesystem.resolve(path)
    digest = hashlib.sha256()
    # Disable extension-based decompression: manifests checksum the stored object.
    with resolved.open_input_stream(normalized, compression=None) as stream:
        for block in iter(lambda: stream.read(_CHECKSUM_BLOCK_BYTES), b""):
            digest.update(block)
    return digest.hexdigest()
