"""Validate release identity and checksum the artifacts that were tested."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RELEASE_VERSION = re.compile(
    r"(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)(?:(?:a|b|rc)(?:0|[1-9]\d*))?"
)


def project_version(root: Path = ROOT) -> str:
    """Use project metadata as the only source of the application version."""
    version = tomllib.loads((root / "pyproject.toml").read_text())["project"]["version"]
    if not isinstance(version, str) or RELEASE_VERSION.fullmatch(version) is None:
        raise ValueError("Expected canonical X.Y.Z, optionally followed by aN, bN or rcN")
    return version


def release_metadata(tag: str, repository: str, root: Path = ROOT) -> dict[str, str]:
    """Reject mismatched tags and stale project entries in the lockfile."""
    version = project_version(root)
    if tag != f"v{version}":
        raise ValueError(f"Tag {tag!r} must match project version v{version}")
    lock = tomllib.loads((root / "uv.lock").read_text())
    packages = [p for p in lock["package"] if p["name"] == "grapharrow"]
    if len(packages) != 1 or packages[0]["version"] != version:
        raise ValueError("Update the GraphArrow version in uv.lock before tagging")
    if re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository) is None:
        raise ValueError("Expected an owner/repository GitHub repository name")
    return {
        "version": version,
        "prerelease": str(re.search(r"(?:a|b|rc)\d+$", version) is not None).lower(),
        "image": f"ghcr.io/{repository.lower()}",
    }


def artifact_files(directory: Path) -> list[Path]:
    """Keep manifests flat and reject symlinks or unexpected artifact names."""
    files = []
    for path in sorted(directory.iterdir()):
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"Artifact must be a regular file: {path.name}")
        if path.name in {"SHA256SUMS", ".gitignore"}:
            continue
        if not (
            path.name == "runtime-requirements.txt"
            or re.fullmatch(r"grapharrow-[A-Za-z0-9_.+-]+\.(?:whl|tar\.gz)", path.name)
        ):
            raise ValueError(f"Unexpected distribution artifact: {path.name}")
        files.append(path)
    if not files:
        raise ValueError("No distribution artifacts found")
    return files


def write_checksums(directory: Path) -> None:
    """Record the bytes that installation and image checks consume."""
    lines = []
    for path in artifact_files(directory):
        with path.open("rb") as stream:
            lines.append(f"{hashlib.file_digest(stream, 'sha256').hexdigest()}  {path.name}\n")
    (directory / "SHA256SUMS").write_text("".join(lines))


def verify_checksums(directory: Path) -> None:
    """Reject changed, missing, extra or duplicate assets before publication."""
    expected = {}
    for line in (directory / "SHA256SUMS").read_text().splitlines():
        match = re.fullmatch(r"([a-f0-9]{64})  ([A-Za-z0-9_.+-]+)", line)
        if match is None or match[2] in expected:
            raise ValueError("Invalid or duplicate checksum entry")
        expected[match[2]] = match[1]
    actual = {}
    for path in artifact_files(directory):
        with path.open("rb") as stream:
            actual[path.name] = hashlib.file_digest(stream, "sha256").hexdigest()
    if actual != expected:
        raise ValueError("Distribution checksums do not match the tested artifacts")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", action="store_true")
    parser.add_argument("--tag")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--checksums", type=Path)
    args = parser.parse_args()
    if args.checksums:
        write_checksums(args.checksums)
    elif args.tag:
        values = release_metadata(args.tag, os.environ["GITHUB_REPOSITORY"])
        if args.output:
            with args.output.open("a") as stream:
                stream.writelines(f"{key}={value}\n" for key, value in values.items())
        print(json.dumps(values))
    else:
        print(project_version())


if __name__ == "__main__":
    main()
