"""Optional read caching must preserve handles, snapshot isolation, and bounded admission."""

from pathlib import Path

import pyarrow as pa
import pytest

from grapharrow.config import GraphArrowSettings
from grapharrow.use_cases.ana import GraphArrowAgentBackend
from grapharrow.use_cases.ana_read_cache import AnaReadCache
from grapharrow.use_cases.ana_resources import AnaQuotaExceeded, AnaResourceBudget, retained_size
from tests.ana_support import load_grapharrow


def test_read_cache_environment_switch_is_optional(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GRAPHARROW_ANA_READ_CACHE_ENABLED", raising=False)
    assert GraphArrowSettings(_env_file=None).ana_read_cache_enabled is False
    monkeypatch.setenv("GRAPHARROW_ANA_READ_CACHE_ENABLED", "true")
    monkeypatch.setenv("GRAPHARROW_ANA_READ_CACHE_MAX_BYTES_PER_SESSION", "65536")
    settings = GraphArrowSettings(_env_file=None)
    assert settings.ana_read_cache_enabled is True
    assert settings.ana_read_cache_max_bytes_per_session == 65536


def test_read_cache_evicts_lru_and_stays_within_its_reserved_capacity() -> None:
    cache = AnaReadCache(4096)
    calls = []

    def read(key: str) -> tuple[str]:
        calls.append(key)
        return (key * 800,)

    cache.get_or_create(("a",), lambda: read("a"))
    cache.get_or_create(("b",), lambda: read("b"))
    cache.get_or_create(("a",), lambda: read("a"))
    cache.get_or_create(("c",), lambda: read("c"))
    cache.get_or_create(("a",), lambda: read("a"))
    cache.get_or_create(("b",), lambda: read("b"))
    assert calls == ["a", "b", "c", "b"]
    assert retained_size(cache._entries) <= cache.used_bytes <= cache.max_bytes
    for size in range(100, 3600, 47):
        cache.get_or_create((size,), lambda size=size: ("x" * size,))
        assert retained_size(cache._entries) <= cache.used_bytes <= cache.max_bytes
    before = list(cache._entries)
    cache.get_or_create(("too-large",), lambda: ("x" * 10000,))
    assert list(cache._entries) == before
    cache.clear()
    assert not cache._entries
    assert cache.used_bytes <= cache.max_bytes


def test_cache_errors_are_recomputed() -> None:
    cache = AnaReadCache(4096)
    attempts = 0

    def fail() -> None:
        nonlocal attempts
        attempts += 1
        raise ValueError("Invalid scalar value")

    for _ in range(2):
        with pytest.raises(ValueError, match="scalar"):
            cache.get_or_create(("error",), fail)
    assert attempts == 2 and not cache._entries


def test_value_cache_keys_keep_modes_properties_labels_and_limits_separate(tmp_path: Path) -> None:
    app = load_grapharrow(tmp_path / "keys")
    app.settings.ana_read_cache_enabled = True
    with GraphArrowAgentBackend(app) as backend:
        queries = [
            {
                "label": "Person",
                "property": "name",
                "text": "A",
                "match_mode": "prefix",
                "limit": 1,
            },
            {
                "label": "Person",
                "property": "name",
                "text": "A",
                "match_mode": "prefix",
                "limit": 2,
            },
            {"label": "Person", "property": "name", "text": "A", "match_mode": "exact", "limit": 1},
            {"label": "Person", "property": "age", "text": "50", "match_mode": "exact", "limit": 1},
            {
                "label": "Company",
                "property": "name",
                "text": "Acme",
                "match_mode": "exact",
                "limit": 1,
            },
        ]
        for query, count in zip(queries, [1, 2, 0, 1, 1], strict=True):
            assert backend.call("value_search", query)["matched_count"] == count
        assert backend.resource_usage["cache_hits"] == 0
        assert backend.resource_usage["cache_misses"] == 5
        backend.call("value_search", queries[0])
        assert backend.resource_usage["cache_hits"] == 1


@pytest.mark.parametrize("enabled", [False, True])
def test_discovery_calls_create_fresh_handles_and_preserve_lineage(
    tmp_path: Path, enabled: bool
) -> None:
    app = load_grapharrow(tmp_path / str(enabled))
    app.settings.ana_read_cache_enabled = enabled
    with GraphArrowAgentBackend(app) as backend:
        query = {"label": "Person", "property": "name", "text": "Alice", "match_mode": "exact"}
        first = backend.call("value_search", query)
        second = backend.call("value_search", {**query, "as": "other", "keep_entities": False})
        assert first["handle"] != second["handle"]
        assert backend.call("fetch", {"from": first["handle"]})["rows"] == [["Alice"]]
        assert backend.call("fetch", {"from": second["handle"]})["rows"] == [["Alice", 0]]
        recap = backend.call("handle_recap", {"from": second["handle"]})
        assert recap["lineage"][-1]["args"]["as"] == "other"
        if enabled:
            assert backend.resource_usage["cache_hits"] == 1
            assert backend.resource_usage["cache_misses"] == 1
            assert (
                backend.resource_usage["cache_used_bytes"]
                <= backend.resource_usage["cache_reserved_bytes"]
            )
        else:
            assert backend._read_cache is None


def test_entity_cache_keys_include_context_and_preserve_alias_independence(tmp_path: Path) -> None:
    app = load_grapharrow(tmp_path / "context")
    app.settings.ana_read_cache_enabled = True
    app.use_cases.graph.create_nodes_bulk(
        table=pa.table({"name": ["Acme", "Acme"], "description": ["North", "South"]}),
        default_labels=("Company",),
    )
    with GraphArrowAgentBackend(app) as backend:
        north = backend.call(
            "entity_resolve", {"label": "Company", "text": "Acme", "context_terms": ["North"]}
        )
        again = backend.call(
            "entity_resolve",
            {"label": "Company", "text": "Acme", "context_terms": ["North"], "as": "other"},
        )
        south = backend.call(
            "entity_resolve", {"label": "Company", "text": "Acme", "context_terms": ["South"]}
        )
        assert (
            backend._handle(north["handle"]).rows[0]["company"]["properties"]["description"]
            == "North"
        )
        assert (
            backend._handle(again["handle"]).rows[0]["other"]["properties"]["description"]
            == "North"
        )
        assert (
            backend._handle(south["handle"]).rows[0]["company"]["properties"]["description"]
            == "South"
        )
        assert backend.resource_usage["cache_hits"] == 1
        assert backend.resource_usage["cache_misses"] == 2


def test_cached_missing_entity_stays_pinned_and_new_workflow_sees_commit(tmp_path: Path) -> None:
    app = load_grapharrow(tmp_path / "snapshots")
    app.settings.ana_read_cache_enabled = True
    budget = AnaResourceBudget()
    with GraphArrowAgentBackend(app, resource_budget=budget) as old:
        query = {"label": "Person", "text": "Zelda"}
        assert old.call("entity_resolve", query)["matched_count"] == 0
        app.use_cases.graph.create_node(labels=("Person",), properties={"name": "Zelda"})
        with GraphArrowAgentBackend(app, resource_budget=budget) as current:
            assert old.call("entity_resolve", query)["matched_count"] == 0
            hit = current.call("entity_resolve", query)
            assert hit["matched_count"] == 1
            assert current.resource_usage["cache_hits"] == 0
            assert (
                current._handle(hit["handle"]).rows[0]["person"].reference.owner == current._owner
            )
        assert old.resource_usage["cache_hits"] == 1
    assert budget.used_bytes == 0
    assert old._read_cache is None


def test_read_cache_capacity_is_reserved_at_admission_and_does_not_grow_on_failure(
    tmp_path: Path,
) -> None:
    app = load_grapharrow(tmp_path / "budget")
    app.settings.ana_read_cache_enabled = False
    with GraphArrowAgentBackend(app) as baseline:
        uncached = baseline.resource_usage["snapshot_bytes"]
    app.settings.ana_read_cache_enabled = True
    app.settings.ana_read_cache_max_bytes_per_session = 65536
    app.settings.ana_max_handles_per_session = 1
    too_small = AnaResourceBudget(uncached + 32768)
    with pytest.raises(AnaQuotaExceeded, match="server byte capacity"):
        GraphArrowAgentBackend(app, resource_budget=too_small)
    assert too_small.used_bytes == 0
    budget = AnaResourceBudget()
    with GraphArrowAgentBackend(app, resource_budget=budget) as backend:
        assert backend.resource_usage["snapshot_bytes"] >= uncached + 65536 - 1024
        backend.call("count_nodes", {"label": "Person"})
        before = budget.used_bytes
        with pytest.raises(AnaQuotaExceeded, match="handle quota"):
            backend.call("entity_resolve", {"label": "Person", "text": "Alice"})
        assert budget.used_bytes == before
        assert len(backend._handle_store.handles) == 1
        assert backend.resource_usage["cache_used_bytes"] <= 65536
    assert budget.used_bytes == 0
