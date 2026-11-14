"""Publish only the artifacts verified by the release workflow."""

from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any

from release_metadata import (
    DEVELOPMENT_VERSION,
    NIGHTLY_TAG,
    artifact_files,
    publication_metadata,
    release_covers_commit,
    release_metadata,
    verify_checksums,
)


def run(*args: str, capture: bool = False, **kwargs: Any) -> str:
    """Fail closed on unexpected GitHub or registry errors."""
    result = subprocess.run(args, check=True, text=True, capture_output=capture, **kwargs)
    return result.stdout.strip() if capture else ""


def require_unused_image(image: str) -> None:
    """Never replace an existing versioned image, including partial runs."""
    result = subprocess.run(
        ["docker", "buildx", "imagetools", "inspect", image], text=True, capture_output=True
    )
    if result.returncode == 0:
        raise ValueError(f"Container tag already exists: {image}; versions are immutable")
    message = (result.stdout + result.stderr).lower()
    if not (
        any(s in message for s in ("404 not found", "manifest unknown", "name unknown"))
        or f"{image.lower()}: not found" in message
    ):
        raise RuntimeError(f"Could not verify container tag availability: {message}")


def container_package_exists(repository: str) -> bool:
    """Distinguish a new GHCR package from registry authorization failures."""
    owner, package = repository.split("/")
    owner_type = run("gh", "api", f"repos/{repository}", "--jq", ".owner.type", capture=True)
    if owner_type not in {"User", "Organization"}:
        raise ValueError("Unknown GitHub package owner type")
    route = "orgs" if owner_type == "Organization" else "users"
    result = subprocess.run(
        ["gh", "api", "--include", f"{route}/{owner}/packages/container/{package.lower()}"],
        text=True,
        capture_output=True,
    )
    if result.returncode == 0:
        return True
    # GHCR can return 403 for a package that does not exist yet. Only GitHub's
    # explicit package-metadata 404 permits the first upload to skip tag lookup.
    status = re.findall(r"(?m)^HTTP/\S+\s+(\d{3})\b", result.stdout)
    if status and status[-1] == "404":
        return False
    raise RuntimeError(f"Could not check GitHub package availability: {result.stderr.strip()}")


def main() -> None:
    tag = os.environ["RELEASE_TAG"]
    repository = os.environ["GITHUB_REPOSITORY"]
    revision = os.environ["GITHUB_SHA"]
    nightly = os.environ.get("GITHUB_EVENT_NAME") == "schedule"
    if nightly:
        version = os.environ["RELEASE_VERSION"]
        if (
            not DEVELOPMENT_VERSION.fullmatch(version)
            or not NIGHTLY_TAG.fullmatch(tag)
            or tag != f"nightly-{revision}"
            or os.environ["GITHUB_REF"] != "refs/heads/main"
        ):
            raise ValueError("Unexpected nightly release identity")
        metadata = publication_metadata(version, repository, prerelease=True)
    else:
        metadata = release_metadata(tag, repository)
    version, image = metadata["version"], metadata["image"]
    verify_checksums(Path("dist"))
    if nightly:
        package_names = {p.name for p in artifact_files(Path("dist"))}
        if package_names != {
            f"grapharrow-{version}-py3-none-any.whl",
            f"grapharrow-{version}.tar.gz",
            "runtime-requirements.txt",
        }:
            raise ValueError("Nightly distributions have an unexpected version")
    if nightly:
        run("git", "merge-base", "--is-ancestor", revision, "origin/main")
    else:
        remote = json.loads(run("gh", "api", f"repos/{repository}/commits/{tag}", capture=True))
        if remote["sha"] != revision:
            raise ValueError("The remote tag moved after validation")
    pages = json.loads(
        run(
            "gh",
            "api",
            "--paginate",
            "--slurp",
            f"repos/{repository}/releases?per_page=100",
            capture=True,
        )
    )
    releases = [release for page in pages for release in page]
    if nightly and release_covers_commit(releases, revision):
        print("This commit is already covered by a published release.")
        return
    if any(r["tag_name"] == tag for r in releases):
        raise ValueError(f"Release {tag} already exists; inspect any previous partial run")
    package_exists = container_package_exists(repository)
    latest = metadata["prerelease"] == "false"
    if latest:
        current = tuple(int(part) for part in version.split("."))
        for release in releases:
            match = re.fullmatch(r"v(\d+)\.(\d+)\.(\d+)", release["tag_name"])
            if (
                match
                and not release.get("draft")
                and not release.get("prerelease")
                and tuple(map(int, match.groups())) > current
            ):
                latest = False
    run(
        "docker",
        "login",
        "ghcr.io",
        "--username",
        os.environ["GITHUB_ACTOR"],
        "--password-stdin",
        input=os.environ["GH_TOKEN"] + "\n",
    )
    try:
        targets = [f"{image}:{version}", *(f"{image}:{version}-{a}" for a in ("amd64", "arm64"))]
        if package_exists:
            for target in targets:
                require_unused_image(target)
        for arch in ("amd64", "arm64"):
            run("docker", "load", "--input", str(Path("images") / f"image-{arch}.tar.gz"))
            info = json.loads(
                run("docker", "image", "inspect", f"grapharrow:check-{arch}", capture=True)
            )[0]
            labels = info["Config"]["Labels"]
            if (
                info["Architecture"] != arch
                or info["Os"] != "linux"
                or labels.get("org.opencontainers.image.revision") != revision
                or labels.get("org.opencontainers.image.version") != version
            ):
                raise ValueError(f"Downloaded {arch} image has unexpected release identity")
        if nightly:
            # Creating the ref is atomic and fails if the tag already exists.
            # This uses a separate namespace from the owner's manual release tags.
            run(
                "gh",
                "api",
                "--method",
                "POST",
                f"repos/{repository}/git/refs",
                "-f",
                f"ref=refs/tags/{tag}",
                "-f",
                f"sha={revision}",
                capture=True,
            )
        args = [
            "gh",
            "release",
            "create",
            tag,
            "--verify-tag",
            "--draft",
            "--generate-notes",
            "--title",
            f"GraphArrow {version} (Nightly)" if nightly else tag,
            *[str(p) for p in artifact_files(Path("dist"))],
            "dist/SHA256SUMS",
        ]
        if metadata["prerelease"] == "true":
            args.extend(("--prerelease", "--latest=false"))
        run(*args)
        for arch in ("amd64", "arm64"):
            target = f"{image}:{version}-{arch}"
            run("docker", "tag", f"grapharrow:check-{arch}", target)
            run("docker", "push", target)
        run(
            "docker",
            "buildx",
            "imagetools",
            "create",
            "--tag",
            f"{image}:{version}",
            f"{image}:{version}-amd64",
            f"{image}:{version}-arm64",
        )
        if latest:
            run(
                "docker",
                "buildx",
                "imagetools",
                "create",
                "--tag",
                f"{image}:latest",
                f"{image}:{version}",
            )
        run(
            "gh",
            "release",
            "edit",
            tag,
            "--draft=false",
            "--latest=" + str(latest).lower(),
        )
    finally:
        run("docker", "logout", "ghcr.io")


if __name__ == "__main__":
    main()
