"""Prepare a GraphArrow release or tag the committed version locally."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

from release_metadata import ROOT, project_version, require_locked_version

METADATA_FILES = ("pyproject.toml", "uv.lock")
BUMP_COMPONENTS = ("major", "minor", "patch")


def run(root: Path, *command: str) -> str:
    return subprocess.check_output(command, cwd=root, text=True).strip()


def require_clean_metadata(root: Path) -> None:
    changes = run(root, "git", "status", "--porcelain", "--", *METADATA_FILES)
    if changes:
        raise ValueError("Commit or restore changes to pyproject.toml and uv.lock first.")


def require_unused_tag(root: Path, tag: str) -> None:
    if run(root, "git", "tag", "--list", tag):
        raise ValueError(f"Tag {tag} already exists locally.")


def bump_version(root: Path, uv: str, bump: str) -> str:
    require_clean_metadata(root)
    current = project_version(root)
    require_locked_version(current, root)
    version = run(root, uv, "version", "--dry-run", "--short", "--bump", bump)
    require_unused_tag(root, f"v{version}")

    original = {root / name: (root / name).read_bytes() for name in METADATA_FILES}
    try:
        run(root, uv, "version", "--no-sync", version)
        if project_version(root) != version:
            raise ValueError(f"Expected uv to write version {version}.")
        require_locked_version(version, root)
    except (Exception, KeyboardInterrupt):
        for path, contents in original.items():
            if not path.exists() or path.read_bytes() != contents:
                path.write_bytes(contents)
        raise
    return version


def tag_version(root: Path) -> str:
    if run(root, "git", "branch", "--show-current") != "main":
        raise ValueError("Check out main before creating a release tag.")
    require_clean_metadata(root)
    version = project_version(root)
    require_locked_version(version, root)
    tag = f"v{version}"
    require_unused_tag(root, tag)
    run(root, "git", "tag", "-a", tag, "-m", f"GraphArrow {version}")
    return tag


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        nargs="?",
        default="show",
        choices=("show", *BUMP_COMPONENTS, "tag"),
    )
    parser.add_argument("--uv", default="uv", help="Path to the uv executable.")
    args = parser.parse_args()
    try:
        if args.action == "show":
            print(project_version())
        elif args.action == "tag":
            tag = tag_version(ROOT)
            print(f"Created local tag {tag}. Publish explicitly with: git push origin {tag}")
        else:
            version = bump_version(ROOT, args.uv, args.action)
            print(
                f"Prepared {version} in pyproject.toml and uv.lock. Review and commit the changes."
            )
    except (ValueError, subprocess.CalledProcessError, OSError) as error:
        parser.exit(1, f"version: {error}\n")


if __name__ == "__main__":
    main()
