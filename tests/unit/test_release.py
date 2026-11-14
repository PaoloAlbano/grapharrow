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


def test_release_version_comes_from_the_tag(metadata: Any) -> None:
    result = metadata.release_metadata("v0.2.0", "Owner/GraphArrow")
    assert result == {
        "version": "0.2.0",
        "prerelease": "false",
        "image": "ghcr.io/owner/grapharrow",
    }


@pytest.mark.parametrize("tag", ["0.2.0", "v0.02.0", "v0.2.0.dev1", "v0.2.0b1"])
def test_stable_release_rejects_invalid_tags(metadata: Any, tag: str) -> None:
    with pytest.raises(ValueError, match="canonical release tag"):
        metadata.release_metadata(tag, "Owner/GraphArrow")


@pytest.mark.parametrize(
    "version", ["01.2.3", "0.1", "0.1.0b1", "0.1.0.dev1", "0.1.0; echo unsafe", "0.1.0\nimage=bad"]
)
def test_release_rejects_noncanonical_versions(metadata: Any, tmp_path: Path, version: str) -> None:
    (tmp_path / "pyproject.toml").write_text("[project]\nversion = " + json.dumps(version) + "\n")
    with pytest.raises(ValueError, match="canonical"):
        metadata.project_version(tmp_path)


@pytest.fixture
def version_repo(
    metadata: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Any, Path]:
    monkeypatch.setitem(sys.modules, "release_metadata", metadata)
    version = load("release_version", "version.py")
    version.run(tmp_path, "git", "init", "-q", "-b", "main")
    (tmp_path / "pyproject.toml").write_text('[project]\nversion = "0.1.0"\n')
    (tmp_path / "uv.lock").write_text('[[package]]\nname = "grapharrow"\nversion = "0.1.0"\n')
    for key, value in {
        "user.name": "GraphArrow Tests",
        "user.email": "grapharrow@example.test",
        "commit.gpgsign": "false",
        "tag.gpgsign": "false",
        "core.hooksPath": str(tmp_path / "no-hooks"),
    }.items():
        version.run(tmp_path, "git", "config", "--local", key, value)
    version.run(tmp_path, "git", "add", "pyproject.toml", "uv.lock")
    version.run(tmp_path, "git", "commit", "-qm", "Release metadata")
    return version, tmp_path


def test_version_tag_requires_committed_metadata_and_refuses_duplicates(
    version_repo: tuple[Any, Path],
) -> None:
    version, root = version_repo
    path = root / "uv.lock"
    original = path.read_text()
    path.write_text(original.replace("0.1.0", "0.2.0"))
    with pytest.raises(ValueError, match="Commit or restore"):
        version.tag_version(root, "tag", "0.2.0")
    assert not version.run(root, "git", "tag", "--list")
    path.write_text(original)
    tag = version.tag_version(root, "tag", "0.2.0")
    assert tag == "v0.2.0"
    assert version.run(root, "git", "cat-file", "-t", tag) == "tag"
    assert version.run(root, "git", "rev-parse", f"{tag}^{{commit}}") == version.run(
        root, "git", "rev-parse", "HEAD"
    )
    with pytest.raises(ValueError, match="already exists"):
        version.tag_version(root, "tag", "0.2.0")


@pytest.mark.parametrize("checkout", [("-b", "feature"), ("--detach", "HEAD")])
def test_release_tag_requires_main(
    version_repo: tuple[Any, Path], checkout: tuple[str, str]
) -> None:
    version, root = version_repo
    version.run(root, "git", "checkout", "-q", *checkout)
    with pytest.raises(ValueError, match="Check out main"):
        version.tag_version(root, "minor")
    assert not version.run(root, "git", "tag", "--list")


@pytest.mark.parametrize(
    ("bump", "expected"), [("patch", "v0.3.11"), ("minor", "v0.4.0"), ("major", "v1.0.0")]
)
def test_bump_tags_from_highest_stable_version_without_editing_the_checkout(
    version_repo: tuple[Any, Path], bump: str, expected: str
) -> None:
    version, root = version_repo
    run = version.run
    for tag in ("v0.3.9", "v0.3.10", "v0.8.0b1", "nightly-" + "a" * 40):
        run(root, "git", "tag", tag)
    assert version.tag_version(root, bump) == expected
    assert not run(root, "git", "status", "--porcelain")
    assert version.project_version(root) == "0.1.0"


@pytest.mark.parametrize("version", ["0.2.0", "0.2.1.dev42"])
@pytest.mark.parametrize("mismatch", [None, "pyproject.toml", "uv.lock"])
def test_apply_build_version_checks_both_files(
    metadata: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    version: str,
    mismatch: str | None,
) -> None:
    def update(command: list[str], *, cwd: Path, check: bool) -> None:
        assert command == ["uv", "version", "--no-sync", version]
        assert cwd == tmp_path and check
        project_version = "0.1.0" if mismatch == "pyproject.toml" else version
        lock_version = "0.1.0" if mismatch == "uv.lock" else version
        (cwd / "pyproject.toml").write_text(f'[project]\nversion = "{project_version}"\n')
        (cwd / "uv.lock").write_text(f'[[package]]\nname="grapharrow"\nversion="{lock_version}"\n')

    monkeypatch.setattr(metadata.subprocess, "run", update)
    if mismatch:
        with pytest.raises(ValueError, match=mismatch):
            metadata.apply_build_version(version, tmp_path)
    else:
        assert metadata.apply_build_version(version, tmp_path) is None


@pytest.mark.parametrize(
    ("kind", "draft", "covers"),
    [
        ("stable", False, True),
        ("nightly", False, True),
        ("nightly", True, False),
        ("untagged", False, False),
        ("older", False, False),
    ],
)
def test_nightly_skips_only_commits_covered_by_a_published_release(
    version_repo: tuple[Any, Path],
    metadata: Any,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    draft: bool,
    covers: bool,
) -> None:
    version, root = version_repo
    previous = version.run(root, "git", "rev-parse", "HEAD")
    version.run(root, "git", "commit", "--allow-empty", "-qm", "Next change")
    revision = version.run(root, "git", "rev-parse", "HEAD")
    tag = f"nightly-{revision}" if kind in {"nightly", "untagged"} else "v0.1.0"
    if kind != "untagged":
        version.run(root, "git", "tag", tag, previous if kind == "older" else revision)
    check_output = subprocess.check_output

    def api(command: list[str], **kwargs: Any) -> str:
        if command[0] == "gh":
            release = {"tag_name": tag, "draft": draft, "prerelease": kind == "nightly"}
            return json.dumps([[release]])
        return check_output(command, **kwargs)

    monkeypatch.setattr(metadata.subprocess, "check_output", api)
    result = metadata.nightly_metadata("Owner/GraphArrow", revision, 42, root)
    if covers:
        assert result == {}
    else:
        expected = "0.1.1.dev42" if kind == "older" else "0.1.0.dev42"
        assert result == {
            "tag": f"nightly-{revision}",
            "version": expected,
            "prerelease": "true",
            "image": "ghcr.io/owner/grapharrow",
        }


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
    monkeypatch.delenv("GITHUB_EVENT_NAME", raising=False)
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


def test_stable_release_rejects_assets_from_another_version(
    publication: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    publisher, calls = publication
    monkeypatch.setenv("RELEASE_TAG", "v0.2.0")
    with pytest.raises(ValueError, match="unexpected version"):
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


@pytest.mark.parametrize("covered_during_build", [False, True])
def test_nightly_creates_only_its_own_tag_after_image_validation(
    publication: Any, monkeypatch: pytest.MonkeyPatch, covered_during_build: bool
) -> None:
    publisher, calls = publication
    tag, revision = "nightly-" + "a" * 40, "a" * 40
    for path in Path("dist").glob("grapharrow-0.1.0*"):
        path.rename(path.with_name(path.name.replace("0.1.0", "0.1.1.dev42")))
    metadata = sys.modules["release_metadata"]
    metadata.write_checksums(Path("dist"))
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.setenv("GITHUB_REF", "refs/heads/main")
    monkeypatch.setenv("RELEASE_VERSION", "0.1.1.dev42")
    monkeypatch.setenv("RELEASE_TAG", tag)
    monkeypatch.setattr(publisher, "release_covers_commit", lambda *args: covered_during_build)
    original = publisher.run

    def run(*args: str, **kwargs: Any) -> str:
        value = original(*args, **kwargs)
        if args[:3] == ("docker", "image", "inspect"):
            data = json.loads(value)
            data[0]["Config"]["Labels"]["org.opencontainers.image.version"] = "0.1.1.dev42"
            return json.dumps(data)
        return value

    monkeypatch.setattr(publisher, "run", run)
    publisher.main()
    if covered_during_build:
        assert all(c[0] != "docker" and "POST" not in c for c in calls)
        return
    created_ref = next(c for c in calls if c[:4] == ("gh", "api", "--method", "POST"))
    assert created_ref[-4:] == ("-f", f"ref=refs/tags/{tag}", "-f", f"sha={revision}")
    last_inspect = max(i for i, c in enumerate(calls) if c[:3] == ("docker", "image", "inspect"))
    assert calls.index(created_ref) > last_inspect
    creation = next(c for c in calls if c[:3] == ("gh", "release", "create"))
    assert creation[3] == tag and "--prerelease" in creation
    assert "GraphArrow 0.1.1.dev42 (Nightly)" in creation
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
