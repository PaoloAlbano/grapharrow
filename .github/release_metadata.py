"""Validate release identity and checksum the artifacts that were tested."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import tomllib
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
FINAL_VERSION = r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)"
DEVELOPMENT_VERSION = re.compile(rf"{FINAL_VERSION}\.dev[1-9][0-9]*")
NIGHTLY_TAG = re.compile(r"nightly-([a-f0-9]{40})")


def project_version(root: Path = ROOT) -> str:
    """Read the committed or runner-local package version."""
    version = tomllib.loads((root / "pyproject.toml").read_text())["project"]["version"]
    if not isinstance(version, str) or not re.fullmatch(FINAL_VERSION, version):
        raise ValueError("Expected canonical X.Y.Z")
    return version


def release_metadata(tag: str, repository: str, root: Path = ROOT) -> dict[str, str]:
    """Reject mismatched tags and stale project entries in the lockfile."""
    version = project_version(root)
    if tag != f"v{version}":
        raise ValueError(f"Tag {tag!r} must match project version v{version}")
    require_locked_version(version, root)
    return publication_metadata(version, repository, prerelease=False)


def publication_metadata(version: str, repository: str, *, prerelease: bool) -> dict[str, str]:
    if re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository) is None:
        raise ValueError("Expected an owner/repository GitHub repository name")
    return {
        "version": version,
        "prerelease": str(prerelease).lower(),
        "image": f"ghcr.io/{repository.lower()}",
    }


def release_covers_commit(releases: list[dict[str, Any]], revision: str, root: Path = ROOT) -> bool:
    covered_by = set(
        subprocess.check_output(
            ["git", "tag", "--contains", revision, "--list", "v*", "nightly-*"],
            cwd=root,
            text=True,
        ).splitlines()
    )
    return any(
        release["tag_name"] in covered_by
        and not release.get("draft")
        and (
            NIGHTLY_TAG.fullmatch(release["tag_name"])
            or (
                not release.get("prerelease")
                and re.fullmatch(rf"v{FINAL_VERSION}", release["tag_name"])
            )
        )
        for release in releases
    )


def nightly_metadata(
    repository: str, revision: str, run_number: int, root: Path = ROOT
) -> dict[str, str]:
    """Select an unpublished main commit and its development version."""
    tag = f"nightly-{revision}"
    if not NIGHTLY_TAG.fullmatch(tag) or run_number < 1:
        raise ValueError("Expected a full Git revision and a positive run number")
    version = project_version(root)
    require_locked_version(version, root)
    pages = subprocess.check_output(
        ["gh", "api", "--paginate", "--slurp", f"repos/{repository}/releases?per_page=100"],
        text=True,
    )
    published = [release for page in json.loads(pages) for release in page]
    if release_covers_commit(published, revision, root):
        return {}
    highest = (0, 0, 0)
    for release in published:
        stable = re.fullmatch(rf"v({FINAL_VERSION})", release["tag_name"])
        if stable and not release.get("prerelease") and not release.get("draft"):
            highest = max(highest, tuple(map(int, stable[1].split("."))))
    if tuple(map(int, version.split("."))) <= highest:
        major, minor, patch = highest
        version = f"{major}.{minor}.{patch + 1}"
    return {
        "tag": tag,
        **publication_metadata(f"{version}.dev{run_number}", repository, prerelease=True),
    }


def require_locked_version(version: str, root: Path = ROOT) -> None:
    """Check that the lockfile records the requested project version."""
    lock = tomllib.loads((root / "uv.lock").read_text())
    packages = [p for p in lock["package"] if p["name"] == "grapharrow"]
    if len(packages) != 1 or packages[0]["version"] != version:
        raise ValueError("Update the GraphArrow version in uv.lock before tagging")


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
    parser.add_argument("--nightly", action="store_true")
    parser.add_argument("--build-nightly", metavar="VERSION")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--checksums", type=Path)
    args = parser.parse_args()
    if args.build_nightly:
        if not DEVELOPMENT_VERSION.fullmatch(args.build_nightly):
            parser.error("Expected a nightly version in the form X.Y.Z.devN")
        subprocess.run(["uv", "version", "--no-sync", args.build_nightly], cwd=ROOT, check=True)
        require_locked_version(args.build_nightly)
    elif args.checksums:
        write_checksums(args.checksums)
    elif args.tag or args.nightly:
        repository = os.environ["GITHUB_REPOSITORY"]
        if args.nightly:
            values = nightly_metadata(
                repository, os.environ["GITHUB_SHA"], int(os.environ["GITHUB_RUN_NUMBER"])
            )
        else:
            values = {"tag": args.tag, **release_metadata(args.tag, repository)}
        if args.output:
            with args.output.open("a") as stream:
                stream.writelines(f"{key}={value}\n" for key, value in values.items())
        print(json.dumps(values))
    else:
        print(project_version())


if __name__ == "__main__":
    main()
