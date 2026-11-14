"""Show the checkout version or publish a GraphArrow release tag."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

from release_metadata import ROOT, STABLE_TAG, project_version

METADATA_FILES = ("pyproject.toml", "uv.lock")
BUMP_COMPONENTS = ("major", "minor", "patch")


def run(root: Path, *command: str) -> str:
    return subprocess.check_output(command, cwd=root, text=True).strip()


def require_clean_metadata(root: Path) -> None:
    changes = run(root, "git", "status", "--porcelain", "--", *METADATA_FILES)
    if changes:
        raise ValueError("Commit or restore changes to pyproject.toml and uv.lock first.")


def publish_version(root: Path, action: str, version: str = "") -> str:
    if run(root, "git", "branch", "--show-current") != "main":
        raise ValueError("Check out main before creating a release tag.")
    require_clean_metadata(root)
    run(root, "git", "fetch", "--tags", "origin", "refs/heads/main")
    revision = run(root, "git", "rev-parse", "HEAD")
    if revision != run(root, "git", "rev-parse", "FETCH_HEAD"):
        raise ValueError("Local main must match origin/main before publishing a release tag.")
    if action in BUMP_COMPONENTS:
        if version:
            raise ValueError("Use BUMP=tag to specify VERSION explicitly.")
        tags = run(root, "git", "tag", "--merged", revision, "--list", "v*").splitlines()
        latest = max(
            (
                tuple(map(int, match[1].split(".")))
                for tag in tags
                if (match := STABLE_TAG.fullmatch(tag))
            ),
            default=(0, 0, 0),
        )
        index = BUMP_COMPONENTS.index(action)
        parts = (*latest[:index], latest[index] + 1, *((0,) * (2 - index)))
        version = ".".join(map(str, parts))
    tag = f"v{version}"
    if STABLE_TAG.fullmatch(tag) is None:
        raise ValueError("Specify a stable release as BUMP=tag VERSION=X.Y.Z.")
    if run(root, "git", "tag", "--list", tag):
        raise ValueError(f"Tag {tag} already exists locally.")
    run(root, "git", "tag", "-a", tag, "-m", f"GraphArrow {version}", revision)
    ref = f"refs/tags/{tag}"
    try:
        run(root, "git", "push", "--no-follow-tags", "origin", f"{ref}:{ref}")
    except subprocess.CalledProcessError as error:
        raise ValueError(
            f"Push failed for {tag}. Retry with: git push --no-follow-tags origin {ref}:{ref}"
        ) from error
    return tag


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        nargs="?",
        default="show",
        choices=("show", *BUMP_COMPONENTS, "tag"),
    )
    parser.add_argument("--version", default="", help="Explicit X.Y.Z for the tag action.")
    args = parser.parse_args()
    try:
        if args.action == "show":
            print(project_version())
        else:
            tag = publish_version(ROOT, args.action, args.version)
            print(f"Pushed {tag} to origin. GitHub Actions will run the release workflow.")
    except (ValueError, subprocess.CalledProcessError, OSError) as error:
        parser.exit(1, f"version: {error}\n")


if __name__ == "__main__":
    main()
