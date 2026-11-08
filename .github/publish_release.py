"""Publish only the artifacts verified by the tag workflow; never rebuild them."""

from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any

from release_metadata import artifact_files, release_metadata, verify_checksums


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
    metadata = release_metadata(tag, repository)
    version, image = metadata["version"], metadata["image"]
    revision = os.environ["GITHUB_SHA"]
    verify_checksums(Path("dist"))
    remote = json.loads(run("gh", "api", f"repos/{repository}/commits/{tag}", capture=True))
    if remote["sha"] != revision:
        raise ValueError("The remote tag moved after validation")
    releases = json.loads(
        run(
            "gh",
            "api",
            "--paginate",
            "--slurp",
            f"repos/{repository}/releases?per_page=100",
            capture=True,
        )
    )
    if any(r["tag_name"] == tag for page in releases for r in page):
        raise ValueError(f"Release {tag} already exists; inspect any previous partial run")
    package_exists = container_package_exists(repository)
    latest = metadata["prerelease"] == "false"
    if latest:
        current = tuple(int(part) for part in version.split("."))
        for page in releases:
            for release in page:
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
        args = [
            "gh",
            "release",
            "create",
            tag,
            "--verify-tag",
            "--draft",
            "--generate-notes",
            "--title",
            tag,
            *[str(p) for p in artifact_files(Path("dist"))],
            "dist/SHA256SUMS",
        ]
        if metadata["prerelease"] == "true":
            args.append("--prerelease")
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
