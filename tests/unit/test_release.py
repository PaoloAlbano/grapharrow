"""Release identity, artifact integrity and publication ordering checks."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]


def load(name: str, filename: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, ROOT / ".github" / filename)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def metadata() -> Any:
    return load("release_test_metadata", "release_metadata.py")


@pytest.mark.parametrize(
    "version,prerelease", [("0.1.0", "false"), ("0.1.0b1", "true"), ("1.2.3rc0", "true")]
)
def test_release_tag_matches_single_project_version(
    metadata: Any, tmp_path: Path, version: str, prerelease: str
) -> None:
    (tmp_path / "pyproject.toml").write_text(f'[project]\nversion = "{version}"\n')
    (tmp_path / "uv.lock").write_text(f'[[package]]\nname="grapharrow"\nversion="{version}"\n')
    result = metadata.release_metadata("v" + version, "Owner/GraphArrow", tmp_path)
    assert result == {
        "version": version,
        "prerelease": prerelease,
        "image": "ghcr.io/owner/grapharrow",
    }
    with pytest.raises(ValueError, match="must match"):
        metadata.release_metadata("v9.9.9", "Owner/GraphArrow", tmp_path)
    (tmp_path / "uv.lock").write_text('[[package]]\nname="grapharrow"\nversion="0.0.0"\n')
    with pytest.raises(ValueError, match="uv.lock"):
        metadata.release_metadata("v" + version, "Owner/GraphArrow", tmp_path)


@pytest.mark.parametrize("version", ["01.2.3", "0.1", "0.1.0; echo unsafe", "0.1.0\nimage=bad"])
def test_release_rejects_noncanonical_versions(metadata: Any, tmp_path: Path, version: str) -> None:
    (tmp_path / "pyproject.toml").write_text("[project]\nversion = " + json.dumps(version) + "\n")
    with pytest.raises(ValueError, match="canonical"):
        metadata.project_version(tmp_path)


@pytest.fixture
def artifacts(metadata: Any, tmp_path: Path) -> Path:
    directory = tmp_path / "dist"
    directory.mkdir()
    for name in [
        "grapharrow-0.1.0-py3-none-any.whl",
        "grapharrow-0.1.0.tar.gz",
        "runtime-requirements.txt",
    ]:
        (directory / name).write_bytes(b"tested bytes")
    metadata.write_checksums(directory)
    return directory


@pytest.mark.parametrize("change", ["modified", "missing", "extra", "duplicate", "traversal"])
def test_checksums_reject_changed_assets(metadata: Any, artifacts: Path, change: str) -> None:
    metadata.verify_checksums(artifacts)
    if change == "modified":
        (artifacts / "runtime-requirements.txt").write_text("different")
    elif change == "missing":
        (artifacts / "runtime-requirements.txt").unlink()
    elif change == "extra":
        (artifacts / "grapharrow-extra.whl").write_text("different")
    elif change == "duplicate":
        path = artifacts / "SHA256SUMS"
        path.write_text(path.read_text() * 2)
    else:
        (artifacts / "SHA256SUMS").write_text("a" * 64 + "  ../private\n")
    with pytest.raises(ValueError):
        metadata.verify_checksums(artifacts)


@pytest.fixture
def publication(metadata: Any, artifacts: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setitem(sys.modules, "release_metadata", metadata)
    publisher = load("release_test_publisher", "publish_release.py")
    monkeypatch.chdir(artifacts.parent)
    for key, value in {
        "RELEASE_TAG": "v0.1.0",
        "GITHUB_REPOSITORY": "Owner/GraphArrow",
        "GITHUB_SHA": "a" * 40,
        "GITHUB_ACTOR": "owner",
        "GH_TOKEN": "test-placeholder",
    }.items():
        monkeypatch.setenv(key, value)
    calls = []

    def run(*args: str, capture: bool = False, **kwargs: Any) -> str:
        calls.append(args)
        if args[:2] == ("gh", "api"):
            return "[[]]" if "--paginate" in args else json.dumps({"sha": "a" * 40})
        if args[:3] == ("docker", "image", "inspect"):
            arch = args[3].split("-")[-1]
            return json.dumps(
                [
                    {
                        "Architecture": arch,
                        "Os": "linux",
                        "Config": {
                            "Labels": {
                                "org.opencontainers.image.version": "0.1.0",
                                "org.opencontainers.image.revision": "a" * 40,
                            }
                        },
                    }
                ]
            )
        return ""

    monkeypatch.setattr(publisher, "run", run)
    monkeypatch.setattr(publisher, "container_package_exists", lambda repository: True)
    monkeypatch.setattr(publisher, "require_unused_image", lambda image: None)
    return publisher, calls


def test_publication_promotes_tested_images_and_finalizes_last(publication: Any) -> None:
    publisher, calls = publication
    publisher.main()
    assert [c[-1] for c in calls if c[:2] == ("docker", "push")] == [
        "ghcr.io/owner/grapharrow:0.1.0-amd64",
        "ghcr.io/owner/grapharrow:0.1.0-arm64",
    ]
    assert not any("build" in c for c in calls)
    creation = next(c for c in calls if c[:3] == ("gh", "release", "create"))
    assert "--draft" in creation and "--verify-tag" in creation
    assert any(c[:4] == ("docker", "buildx", "imagetools", "create") for c in calls)
    assert calls[-2][:3] == ("gh", "release", "edit") and "--draft=false" in calls[-2]
    assert calls[-1] == ("docker", "logout", "ghcr.io")


def test_changed_tag_cannot_publish(publication: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    publisher, calls = publication
    monkeypatch.setenv("GITHUB_SHA", "b" * 40)
    with pytest.raises(ValueError, match="tag moved"):
        publisher.main()
    assert len(calls) == 1


def test_existing_release_cannot_be_overwritten(
    publication: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    publisher, calls = publication
    original = publisher.run

    def run(*args: str, **kwargs: Any) -> str:
        if "--paginate" in args:
            return '[[{"tag_name":"v0.1.0"}]]'
        return original(*args, **kwargs)

    monkeypatch.setattr(publisher, "run", run)
    with pytest.raises(ValueError, match="already exists"):
        publisher.main()
    assert not any(c[0] == "docker" for c in calls)


@pytest.mark.parametrize(
    "code,message,raises",
    [
        (0, "", ValueError),
        (1, "manifest unknown", None),
        (1, "unauthorized", RuntimeError),
        (1, "connection timeout", RuntimeError),
    ],
)
def test_registry_check_fails_closed(
    metadata: Any, monkeypatch: pytest.MonkeyPatch, code: int, message: str, raises: Any
) -> None:
    monkeypatch.setitem(sys.modules, "release_metadata", metadata)
    publisher = load("release_registry_test", "publish_release.py")
    monkeypatch.setattr(
        publisher.subprocess,
        "run",
        lambda *a, **k: subprocess.CompletedProcess(a, code, "", message),
    )
    if raises:
        with pytest.raises(raises):
            publisher.require_unused_image("ghcr.io/owner/grapharrow:0.1.0")
    else:
        publisher.require_unused_image("ghcr.io/owner/grapharrow:0.1.0")


def test_changed_assets_prevent_all_external_calls(publication: Any) -> None:
    publisher, calls = publication
    Path("dist/runtime-requirements.txt").write_text("unverified bytes")
    with pytest.raises(ValueError, match="checksums"):
        publisher.main()
    assert not calls


def test_wrong_image_identity_prevents_creating_release(
    publication: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    publisher, calls = publication
    original = publisher.run

    def run(*args: str, **kwargs: Any) -> str:
        value = original(*args, **kwargs)
        if args[:3] == ("docker", "image", "inspect"):
            data = json.loads(value)
            data[0]["Config"]["Labels"]["org.opencontainers.image.revision"] = "wrong"
            return json.dumps(data)
        return value

    monkeypatch.setattr(publisher, "run", run)
    with pytest.raises(ValueError, match="unexpected release identity"):
        publisher.main()
    assert not any(c[:3] == ("gh", "release", "create") for c in calls)
    assert calls[-1] == ("docker", "logout", "ghcr.io")


def test_push_failure_keeps_the_release_unpublished(
    publication: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    publisher, calls = publication
    original = publisher.run

    def run(*args: str, **kwargs: Any) -> str:
        value = original(*args, **kwargs)
        if args[:2] == ("docker", "push"):
            raise subprocess.CalledProcessError(1, args)
        return value

    monkeypatch.setattr(publisher, "run", run)
    with pytest.raises(subprocess.CalledProcessError):
        publisher.main()
    assert any(c[:3] == ("gh", "release", "create") for c in calls)
    assert not any(c[:3] == ("gh", "release", "edit") for c in calls)
    assert calls[-1] == ("docker", "logout", "ghcr.io")


def test_prereleases_do_not_change_latest(
    publication: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    publisher, calls = publication
    monkeypatch.setattr(
        publisher,
        "release_metadata",
        lambda *args: {
            "version": "0.1.0b1",
            "prerelease": "true",
            "image": "ghcr.io/owner/grapharrow",
        },
    )
    original = publisher.run

    def run(*args: str, **kwargs: Any) -> str:
        value = original(*args, **kwargs)
        if args[:3] == ("docker", "image", "inspect"):
            data = json.loads(value)
            data[0]["Config"]["Labels"]["org.opencontainers.image.version"] = "0.1.0b1"
            return json.dumps(data)
        return value

    monkeypatch.setattr(publisher, "run", run)
    publisher.main()
    assert "--prerelease" in next(c for c in calls if c[:3] == ("gh", "release", "create"))
    assert "--latest=false" in calls[-2]
    assert not any("ghcr.io/owner/grapharrow:latest" in c for c in calls)


def test_backfilled_stable_release_does_not_replace_newer_latest(
    publication: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    publisher, calls = publication
    original = publisher.run

    def run(*args: str, **kwargs: Any) -> str:
        if "--paginate" in args:
            return '[[{"tag_name":"v0.2.0","draft":false,"prerelease":false}]]'
        return original(*args, **kwargs)

    monkeypatch.setattr(publisher, "run", run)
    publisher.main()
    assert "--latest=false" in calls[-2]
    assert not any("ghcr.io/owner/grapharrow:latest" in c for c in calls)


@pytest.mark.parametrize("owner_type,route", [("User", "users"), ("Organization", "orgs")])
@pytest.mark.parametrize("status,exists", [(200, True), (404, False), (403, None), (500, None)])
def test_package_lookup_distinguishes_missing_and_forbidden(
    metadata: Any,
    monkeypatch: pytest.MonkeyPatch,
    owner_type: str,
    route: str,
    status: int,
    exists: bool | None,
) -> None:
    monkeypatch.setitem(sys.modules, "release_metadata", metadata)
    publisher = load("package_lookup_test", "publish_release.py")
    monkeypatch.setattr(publisher, "run", lambda *args, **kwargs: owner_type)
    calls = []

    def query(args: Any, **kwargs: Any) -> Any:
        calls.append(args)
        return subprocess.CompletedProcess(
            args,
            0 if status == 200 else 1,
            f"HTTP/2.0 {status}\ncontent-type: application/json\n\n{{}}",
            "request failed",
        )

    monkeypatch.setattr(publisher.subprocess, "run", query)
    if exists is None:
        with pytest.raises(RuntimeError, match="package availability"):
            publisher.container_package_exists("Owner/GraphArrow")
    else:
        assert publisher.container_package_exists("Owner/GraphArrow") is exists
    assert calls[0][-1] == f"{route}/Owner/packages/container/grapharrow"


def test_first_container_upload_does_not_require_an_existing_pull_token(
    publication: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    publisher, calls = publication
    monkeypatch.setattr(publisher, "container_package_exists", lambda repository: False)

    def unexpected_lookup(image: str) -> None:
        raise AssertionError("New package must not require a preexisting registry pull scope")

    monkeypatch.setattr(publisher, "require_unused_image", unexpected_lookup)
    publisher.main()
    assert len([c for c in calls if c[:2] == ("docker", "push")]) == 2
