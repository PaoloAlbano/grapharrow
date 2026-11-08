"""Atomic ANA calls must scale with their own changes and preserve admission state."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from threading import Event
from typing import Any

import pytest

from grapharrow.use_cases.ana import GraphArrowAgentBackend
from grapharrow.use_cases.ana_resources import AnaQuotaExceeded, AnaResourceBudget, retained_size
from tests.ana_support import load_grapharrow


@pytest.fixture
def application(tmp_path: Path) -> Any:
    return load_grapharrow(tmp_path / "journal")


def assert_accounting(backend: Any) -> None:
    usage = backend.resource_usage
    charges = backend._resources.handles
    assert usage["handles"] == len(backend._handle_store.handles) == len(charges)
    assert usage["rows"] == sum(charge[0] for charge in charges.values())
    total = usage["snapshot_bytes"] + sum(charge[2] for charge in charges.values())
    if not charges and backend.last_fetch is None:
        # A freshly admitted workflow has not finalized its first None page yet.
        assert usage["bytes"] in {total, total + retained_size(None)}
    else:
        assert usage["bytes"] == total + retained_size(backend.last_fetch)
    assert set(backend._handle_store.lineage) <= set(charges)


def test_successful_calls_keep_counters_and_visible_results_consistent(application: Any) -> None:
    budget = AnaResourceBudget()
    with GraphArrowAgentBackend(application, resource_budget=budget) as backend:
        source = backend.call("node_scan", {"label": "Person", "limit": 6})["handle"]
        page = backend.call("fetch", {"from": source, "limit": 2})
        assert page["rows"] == [["Alice"], ["Bob"]]
        assert_accounting(backend)
        for tool, args in [
            ("schema_overview", {}),
            ("summarize_handle", {"from": source}),
            ("handle_recap", {"from": source}),
            ("count_handle", {"from": source}),
            (
                "project",
                {"from": source, "select": [{"var": "person", "property": "name"}], "limit": 2},
            ),
        ]:
            backend.call(tool, args)
            assert_accounting(backend)
            assert budget.used_bytes == backend.resource_usage["bytes"]
    assert budget.used_bytes == 0
    assert backend.resource_usage == {"handles": 0, "rows": 0, "bytes": 0, "snapshot_bytes": 0}


@pytest.mark.parametrize("error", [ValueError, KeyboardInterrupt])
def test_failure_after_multiple_publications_restores_only_the_call(
    application: Any, monkeypatch: pytest.MonkeyPatch, error: type[BaseException]
) -> None:
    budget = AnaResourceBudget()
    with GraphArrowAgentBackend(application, resource_budget=budget) as backend:
        first = backend.call("count_nodes", {"label": "Person"})["handle"]
        backend.call("fetch", {"from": first})
        before = backend.resource_usage
        old_handle = backend._handle_store.handles[first]
        old_lineage = backend._handle_store.lineage[first]
        previous_page = backend.last_fetch
        counter = backend._handle_store.counter

        def fail(args: Any) -> Any:
            backend._store([{"temporary": 1}], kind="table", columns=["temporary"])
            backend._store([{"temporary": 2}], kind="table", columns=["temporary"])
            backend.last_fetch = [["temporary"]]
            raise error("Interrupted after publication")

        with monkeypatch.context() as patch:
            patch.setattr(backend, "_count_nodes", fail)
            with pytest.raises(error, match="Interrupted after publication"):
                backend.call("count_nodes", {"label": "Person"})
        assert backend.resource_usage == before
        assert budget.used_bytes == before["bytes"]
        assert backend._handle_store.handles == {first: old_handle}
        assert backend._handle_store.lineage[first] is old_lineage
        assert backend.last_fetch is previous_page
        assert backend._handle_store.counter == counter + 2
        assert_accounting(backend)
        successful = backend.call("count_nodes", {"label": "Person"})
        assert successful["handle"] == f"h{counter + 3}"
    assert budget.used_bytes == 0


@pytest.mark.parametrize("had_lineage", [True, False])
def test_rejected_existing_handle_lineage_is_restored_by_identity(
    application: Any, had_lineage: bool
) -> None:
    with GraphArrowAgentBackend(application) as backend:
        handle = backend.call("node_scan", {"label": "Person", "limit": 6})["handle"]
        backend.call("fetch", {"from": handle, "limit": 1})
        if not had_lineage:
            backend._handle_store.lineage.pop(handle)
            backend.call("handle_recap", {"from": handle})
        old_lineage = backend._handle_store.lineage.get(handle)
        before, previous_page = backend.resource_usage, backend.last_fetch
        charge = backend._resources.handles[handle]
        backend._resources.limits = replace(backend._resources.limits, max_bytes=before["bytes"])
        with pytest.raises(AnaQuotaExceeded, match="byte quota"):
            backend.call("summarize_handle", {"from": handle})
        assert backend.resource_usage == before
        assert backend._handle_store.lineage.get(handle) is old_lineage
        assert (handle in backend._handle_store.lineage) is had_lineage
        assert backend._resources.handles[handle] is charge
        assert backend.last_fetch is previous_page
        assert_accounting(backend)
        assert backend.call("fetch", {"from": handle, "limit": 1})["rows"] == [["Alice"]]


def test_summary_failure_during_store_removes_unregistered_handle(
    application: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    with GraphArrowAgentBackend(application) as backend:
        first = backend.call("count_nodes", {"label": "Person"})["handle"]
        before = backend.resource_usage
        original = backend._handle_store.summary

        def fail(handle: Any) -> Any:
            if handle.id != first:
                raise RuntimeError("Cannot summarize new handle")
            return original(handle)

        with monkeypatch.context() as patch:
            patch.setattr(backend._handle_store, "summary", fail)
            with pytest.raises(RuntimeError, match="Cannot summarize"):
                backend.call("count_nodes", {"label": "Person"})
        assert backend.resource_usage == before
        assert list(backend._handle_store.handles) == [first]
        assert_accounting(backend)
        assert backend.call("count_nodes", {"label": "Person"})["handle"] == "h3"


def test_failure_and_validation_do_not_leave_an_active_journal(application: Any) -> None:
    with GraphArrowAgentBackend(application) as backend:
        before = backend.resource_usage
        for tool, args in [
            ("fetch", {"from": "missing"}),
            ("node_scan", {"label": "Person", "limit": -1}),
        ]:
            with pytest.raises(ValueError):
                backend.call(tool, args)
            assert backend.resource_usage == before
        assert backend.call("count_nodes", {"label": "Person"})["handle"] == "h1"
        assert_accounting(backend)


def test_failed_call_does_not_revert_another_workflows_budget(
    application: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    budget = AnaResourceBudget()
    with GraphArrowAgentBackend(application) as probe:
        initial = probe.resource_usage["bytes"]
        probe.call("count_nodes", {"label": "Person"})
        full_charge = probe.resource_usage["bytes"] - initial
    with (
        GraphArrowAgentBackend(application, resource_budget=budget) as first,
        GraphArrowAgentBackend(application, resource_budget=budget) as second,
    ):
        before = first.resource_usage
        reserved, release = Event(), Event()
        original = first._count_nodes

        def pause(args: Any) -> Any:
            result = original(args)
            reserved.set()
            assert release.wait(timeout=15)
            return result

        monkeypatch.setattr(first, "_count_nodes", pause)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(first.call, "count_nodes", {"label": "Person"})
            try:
                assert reserved.wait(timeout=15)
                budget.max_bytes = budget.used_bytes + full_charge
                second_result = second.call("count_nodes", {"label": "Person"})
            finally:
                release.set()
            with pytest.raises(AnaQuotaExceeded, match="server byte capacity"):
                future.result(timeout=15)
        assert first.resource_usage == before
        assert_accounting(first)
        assert_accounting(second)
        assert budget.used_bytes == first.resource_usage["bytes"] + second.resource_usage["bytes"]
        assert second.call("fetch", {"from": second_result["handle"]})["rows"] == [[6]]
    assert budget.used_bytes == 0


def test_small_calls_do_not_enumerate_unrelated_handle_maps(application: Any) -> None:
    class NoWalk(dict):
        def __iter__(self):
            raise AssertionError("Unexpected workflow-wide iteration")

        def keys(self):
            raise AssertionError("Unexpected workflow-wide keys")

        def values(self):
            raise AssertionError("Unexpected workflow-wide values")

        def items(self):
            raise AssertionError("Unexpected workflow-wide items")

        def copy(self):
            raise AssertionError("Unexpected workflow-wide copy")

    with GraphArrowAgentBackend(application) as backend:
        first = backend.call("count_nodes", {"label": "Person"})["handle"]
        for _ in range(100):
            backend.call("count_nodes", {"label": "Person"})
        backend._handle_store.handles = NoWalk(backend._handle_store.handles)
        backend._handle_store.lineage = NoWalk(backend._handle_store.lineage)
        backend._resources.handles = NoWalk(backend._resources.handles)
        assert backend.call("fetch", {"from": first})["rows"] == [[6]]
        backend.call("schema_overview", {})
        backend.call("count_nodes", {"label": "Person"})
        backend.call("summarize_handle", {"from": first})
        assert backend.resource_usage["handles"] == 102
