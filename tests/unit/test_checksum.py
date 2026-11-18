"""Checksum readers preserve raw bytes and bound memory while using sequential I/O."""

import gzip
import hashlib
from io import BytesIO
from pathlib import Path
from typing import Any

import pytest

from grapharrow.adapters.checksum import sha256_file
from grapharrow.adapters.filesystem import FileSystemAdapter


@pytest.mark.parametrize("size", [0, 1, 1024 * 1024, 2 * 1024 * 1024 + 17])
def test_streaming_checksum_matches_sha256(tmp_path: Path, size: int) -> None:
    payload = bytes(range(256)) * (size // 256) + bytes(range(size % 256))
    path = tmp_path / "object.parquet"
    path.write_bytes(payload)
    assert sha256_file(path, filesystem=FileSystemAdapter()) == hashlib.sha256(payload).hexdigest()


def test_checksum_hashes_compressed_object_bytes_without_decompression(tmp_path: Path) -> None:
    payload = gzip.compress(b"stored bytes, not their decompressed contents")
    path = tmp_path / "object.gz"
    path.write_bytes(payload)
    assert sha256_file(path, filesystem=FileSystemAdapter()) == hashlib.sha256(payload).hexdigest()


def test_checksum_uses_one_sequential_stream_and_bounded_blocks() -> None:
    payload = b"x" * (3 * 1024 * 1024 + 1)
    reads = []
    opened = []

    class Stream(BytesIO):
        def read(self, size: int = -1) -> bytes:
            reads.append(size)
            assert 0 < size <= 1024 * 1024
            return super().read(size)

    stream = Stream(payload)

    class Remote:
        def open_input_stream(self, path: str, *, compression: Any) -> Stream:
            opened.append(path)
            assert compression is None
            return stream

    class Filesystem:
        def resolve(self, path: str) -> tuple[Remote, str]:
            return Remote(), path

    assert sha256_file("object", filesystem=Filesystem()) == hashlib.sha256(payload).hexdigest()  # type: ignore[arg-type]
    assert opened == ["object"]
    assert len(reads) == 5
    assert stream.closed
