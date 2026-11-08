"""Common snapshot adjacency keeps frontend and transaction semantics intact."""

import gc
import weakref
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from grapharrow.use_cases.ana import GraphArrowAgentBackend
from grapharrow.use_cases.ana_resources import AnaResourceBudget, retained_size
from grapharrow.use_cases.cypher import CypherQueryUseCase
from grapharrow.use_cases.graph import GraphTransactionUseCase
from tests.ana_support import load_grapharrow


def test_queries_release_canonical_tables_without_cyclic_gc(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = load_grapharrow(tmp_path / "db")
    original = app.use_cases.graph.load_tabular_state
    tables = []

    def load(**kwargs: Any) -> Any:
        state = original(**kwargs)
        tables.append(weakref.ref(state.edge_table))
        return state

    monkeypatch.setattr(app.use_cases.graph, "load_tabular_state", load)
    enabled = gc.isenabled()
    gc.collect()
    gc.disable()
    try:
        for query in (QUERIES[0], "MATCH (c)-[:CEO]->(p) RETURN count(*) AS count"):
            app.use_cases.cypher.execute(query)
            assert tables and all(reference() is None for reference in tables)
    finally:
        if enabled:
            gc.enable()


QUERIES = [
    "MATCH (c:Company {name:'Acme'})-[:CEO]->(p:Person) RETURN p.name AS name ORDER BY name",
    "MATCH (p:Person {name:'Alice'})<-[:CEO]-(c:Company) RETURN c.name AS name ORDER BY name",
    "MATCH (c:Company {name:'Acme'})-[:CEO|FOUNDER]->(p:Person) "
    "RETURN p.name AS name ORDER BY name",
    "MATCH (c:Company {name:'Acme'})-[r]->(p:Person) "
    "RETURN type(r) AS kind, p.name AS name ORDER BY kind, name",
    "MATCH (c:Company {name:'Acme'})-[r:CEO]->(p:Person) "
    "WHERE r.since > 2000 RETURN p.name AS name, r.since AS since",
    "MATCH (c:Company {name:'missing'})-[r:CEO]->(p:Person) RETURN r.since AS since",
    "MATCH (p:Person {name:'Alice'})<-[:MISSING]-(c:Company) RETURN c.name AS name",
    "MATCH (p:Person {name:'Alice'})<-[:MISSING|CEO]-(c:Company) "
    "RETURN c.name AS name ORDER BY c.name",
    "MATCH (p:Person {name:'Dana'}) OPTIONAL MATCH (p)-[:WORKS]->(c:Company) RETURN c.name AS name",
    "MATCH (c:Company {name:'Acme'})-[:CEO]->(p:Person) WITH c "
    "MATCH (c)-[:CEO]->(q:Person) RETURN q.name AS name ORDER BY q.name",
    "MATCH (c:Company {name:'Acme'})-[r:CEO]->(p:Person)<-[s:CEO]-(c) RETURN count(*) AS count",
    "MATCH (c:Company {name:'Acme'})-[r:CEO]->(p:Person) RETURN r ORDER BY r.since",
]


@pytest.mark.parametrize("query", QUERIES)
@pytest.mark.parametrize("planner", [True, False])
def test_cypher_results_match_scan_with_and_without_planner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, query: str, planner: bool
) -> None:
    monkeypatch.setenv("GRAPHARROW_DISABLE_CYPHER_PLANNER", "false" if planner else "true")
    app = load_grapharrow(tmp_path / "db")
    store = app.use_cases.graph.adjacency_indexes
    store.max_bytes = 0
    expected = app.use_cases.cypher.execute(query)
    store.max_bytes = 32 * 1024 * 1024
    assert app.use_cases.cypher.execute(query) == expected


def test_ana_and_cypher_share_one_build_and_keep_handles_separate(tmp_path: Path) -> None:
    app = load_grapharrow(tmp_path / "db")
    budget = AnaResourceBudget()
    with (
        GraphArrowAgentBackend(app, resource_budget=budget) as first,
        GraphArrowAgentBackend(app, resource_budget=budget) as second,
    ):
        assert first._adjacency is second._adjacency
        index = first._adjacency
        assert index is not None and index.build_count == 0
        assert app.use_cases.cypher.execute(QUERIES[0]).rows == ({"name": "Alice"},) * 2
        assert index.build_count == 1
        for backend in (first, second):
            seed = backend.call("node_search", {"label": "Company", "value": "Acme"})
            result = backend.call(
                "expand",
                {
                    "from": seed["handle"],
                    "source": "company",
                    "relationship_type": "CEO",
                    "target_label": "Person",
                },
            )
            assert seed["handle"] == "h1"
            assert backend.call("fetch", {"from": result["handle"]})["rows"] == [
                ["Alice"],
                ["Alice"],
            ]
            assert all(
                row["person"].reference.owner == backend._owner
                for row in backend._handle_store.handles[result["handle"]].rows
            )
        assert index.build_count == 1
        assert retained_size(index) <= index.reserved_bytes
    assert budget.used_bytes == 0
    # The application LRU can retain a prepared index after the last ANA owner closes.
    assert app.use_cases.graph.adjacency_indexes.used_bytes > 0
    with GraphArrowAgentBackend(app) as later:
        assert later._adjacency is index and index.build_count == 1


def test_concurrent_frontends_build_once(tmp_path: Path) -> None:
    app = load_grapharrow(tmp_path / "db")
    barrier = Barrier(4)
    budget = AnaResourceBudget()

    def query(worker: int) -> Any:
        if worker % 2:
            barrier.wait(timeout=10)
            return app.use_cases.cypher.execute(QUERIES[0]).rows
        with GraphArrowAgentBackend(app, resource_budget=budget) as backend:
            seed = backend.call("node_search", {"label": "Company", "value": "Acme"})
            barrier.wait(timeout=10)
            result = backend.call(
                "expand",
                {
                    "from": seed["handle"],
                    "source": "company",
                    "relationship_type": "CEO",
                    "target_label": "Person",
                },
            )
            return backend.call("fetch", {"from": result["handle"]})["rows"]

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(query, range(4)))
    assert results == [[["Alice"], ["Alice"]], ({"name": "Alice"},) * 2] * 2
    entries = app.use_cases.graph.adjacency_indexes._entries
    assert len(entries) == 1
    assert next(iter(entries.values()))[0].build_count == 1
    assert budget.used_bytes == 0


def test_transaction_overlay_never_enters_the_published_cache(tmp_path: Path) -> None:
    app = load_grapharrow(tmp_path / "db")
    with GraphArrowAgentBackend(app) as old:
        published = old._adjacency
        transaction = GraphTransactionUseCase(app.use_cases.graph)
        cypher = CypherQueryUseCase(transaction)  # type: ignore[arg-type]
        assert cypher.execute(QUERIES[0]).rows == ({"name": "Alice"},) * 2
        assert transaction.adjacency_for_state(transaction.load_state()) is published
        cypher.execute("MATCH (c:Company {name:'Acme'})-[r:CEO]->(p:Person) DELETE r")
        assert cypher.execute(QUERIES[0]).rows == ()
        private = transaction._adjacency_index
        assert private is not None and private is not published
        cypher.execute(
            "MATCH (c:Company {name:'Acme'}), (p:Person {name:'Bob'}) CREATE (c)-[:CEO]->(p)"
        )
        assert cypher.execute(QUERIES[0]).rows == ({"name": "Bob"},)
        assert transaction._adjacency_index is not private
        assert app.use_cases.cypher.execute(QUERIES[0]).rows == ({"name": "Alice"},) * 2
        assert app.use_cases.graph.adjacency_indexes._entries
        assert all(
            index is not transaction._adjacency_index
            for index, _ in app.use_cases.graph.adjacency_indexes._entries.values()
        )
        transaction.rollback()
        assert transaction._adjacency_index is None and transaction._adjacency_table is None


def test_unchanged_edge_files_reuse_index_and_evicted_owners_stay_valid(tmp_path: Path) -> None:
    app = load_grapharrow(tmp_path / "db")
    store = app.use_cases.graph.adjacency_indexes
    with GraphArrowAgentBackend(app) as old:
        index = old._adjacency
        assert index is not None
        seed = old.call("node_search", {"label": "Company", "value": "Acme"})
        args = {
            "from": seed["handle"],
            "source": "company",
            "relationship_type": "CEO",
            "target_label": "Person",
        }
        assert old.call("expand", args)["matched_count"] == 2
        app.use_cases.graph.create_node(labels=("Unused",), properties={"name": "new"})
        with GraphArrowAgentBackend(app) as same_edges:
            assert same_edges._adjacency is index
        store.max_bytes = store.used_bytes + 1024
        app.use_cases.cypher.execute(
            "MATCH (c:Company {name:'Acme'}), (p:Person {name:'Bob'}) CREATE (c)-[:CEO]->(p)"
        )
        with GraphArrowAgentBackend(app) as new:
            assert new._adjacency is not None and new._adjacency is not index
            assert len(store._entries) == 1 and store.used_bytes <= store.max_bytes
        app.use_cases.admin.compact()
        app.use_cases.admin.vacuum()
        store.clear()
        assert store.used_bytes == 0
        assert old.call("expand", args)["matched_count"] == 2
        assert index.build_count == 1


def test_zero_or_insufficient_cache_capacity_uses_existing_paths(tmp_path: Path) -> None:
    app = load_grapharrow(tmp_path / "db")
    store = app.use_cases.graph.adjacency_indexes
    for capacity in (0, 1):
        store.max_bytes = capacity
        with GraphArrowAgentBackend(app) as backend:
            assert backend._adjacency is None
            assert app.use_cases.cypher.execute(QUERIES[0]).rows == ({"name": "Alice"},) * 2
        assert store.used_bytes == 0


def test_ambiguous_imported_type_names_preserve_projection_fallback(tmp_path: Path) -> None:
    app = load_grapharrow(tmp_path / "db")
    nodes = app.use_cases.graph.load_state().nodes
    source = next(node.node_id for node in nodes if node.properties.get("name") == "Acme")
    target = next(node.node_id for node in nodes if node.properties.get("name") == "Alice")
    app.use_cases.graph.create_edges_bulk(
        table=pa.table(
            {
                "src_id": [source],
                "dst_id": [target],
                "rel_type": ["`CEO`"],
                "since": [2026],
            }
        )
    )
    store = app.use_cases.graph.adjacency_indexes
    for query in (QUERIES[0], QUERIES[4]):
        store.max_bytes = 0
        expected = app.use_cases.cypher.execute(query)
        store.max_bytes = 32 * 1024 * 1024
        assert app.use_cases.cypher.execute(query) == expected


def test_oversized_reservation_is_remembered_without_repeated_scans(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = load_grapharrow(tmp_path / "db")
    state = app.use_cases.graph.load_tabular_state()
    store = app.use_cases.graph.adjacency_indexes
    store.max_bytes = 12000
    original = pc.unique
    calls = []

    def unique(values: Any) -> Any:
        calls.append(True)
        return original(values)

    monkeypatch.setattr(pc, "unique", unique)
    for _ in range(3):
        assert app.use_cases.graph.adjacency_for_state(state) is None
    assert calls == [True]
    assert 0 < store.used_bytes <= store.max_bytes
    assert len(store._entries) == 1 and next(iter(store._entries.values()))[0] is None
