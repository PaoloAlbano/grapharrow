"""Adjacency lookups preserve canonical traversal semantics and bounded decoding."""

import random
from dataclasses import replace
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from grapharrow.app import build_application
from grapharrow.use_cases.ana import GraphArrowAgentBackend
from grapharrow.use_cases.ana_adjacency import AnaAdjacency
from grapharrow.use_cases.ana_resources import AnaQuotaExceeded, AnaResourceBudget, retained_size
from tests.ana_support import load_grapharrow


@pytest.mark.parametrize("columns", [("src_id",), ("dst_id",), ("src_id", "dst_id")])
def test_index_matches_scan_in_canonical_order(columns: tuple[str, ...]) -> None:
    rng = random.Random(371)
    rows = [
        {
            "edge_id": 10000 - i,
            "src_id": rng.randrange(12),
            "dst_id": rng.randrange(12),
            "rel_type": rng.choice(["LINK", "OTHER", "é", ""]),
            "properties_json": str(i),
        }
        for i in range(700)
    ]
    # Chunk boundaries and edge IDs have no relationship to canonical row order.
    edges = pa.concat_tables([pa.Table.from_pylist(rows[:239]), pa.Table.from_pylist(rows[239:])])
    index = AnaAdjacency()
    sources = [1, 11, 100, 2, 1, 0, 7, 9, 2, 3, 4, 5, 6]
    for rel_type in ["LINK", "OTHER", "é", "", "missing"]:
        groups = index.rows_for_sources(edges, rel_type, sources, columns, properties=True)
        actual = [list(group) for group in groups]
        expected = [
            [
                row
                for row in rows
                if row["rel_type"] == rel_type and any(row[column] == node for column in columns)
            ]
            for node in sources
        ]
        assert actual == expected
        allowed = {0, 1, 2, 7, 8, 11}
        targets = set()
        for column in columns:
            target = "dst_id" if column == "src_id" else "src_id"
            targets.update(
                row[target]
                for row in rows
                if row["rel_type"] == rel_type and row[column] in sources and row[target] in allowed
            )
        assert index.targets(edges, rel_type, sources, columns, allowed) == targets
    reservation = index.reservation(edges.num_rows, {row["rel_type"] for row in rows}, 1 << 30)
    assert retained_size(index) <= reservation
    assert retained_size(index) <= index.reservation(
        edges.num_rows, {row["rel_type"] for row in rows}, 1 << 30, node_count=12
    )


def test_high_degree_decodes_only_one_bounded_batch() -> None:
    edges = pa.table(
        {
            "edge_id": range(10000),
            "src_id": [1] * 10000,
            "dst_id": range(10000),
            "rel_type": ["LINK"] * 10000,
        }
    )
    index = AnaAdjacency()
    decoded = []

    class Batch:
        def __init__(self, batch: Any) -> None:
            self.batch = batch

        def to_pylist(self) -> Any:
            assert self.batch.num_rows <= 256
            decoded.append(self.batch.num_rows)
            return self.batch.to_pylist()

    class Selected:
        def __init__(self, table: Any) -> None:
            self.table = table

        def to_batches(self, **kwargs: Any) -> Any:
            return (Batch(batch) for batch in self.table.to_batches(**kwargs))

    class LimitedTable:
        def take(self, positions: Any) -> Any:
            return Selected(edges.select(["edge_id", "src_id", "dst_id"]).take(positions))

    class EdgeTable:
        def select(self, columns: Any) -> Any:
            return LimitedTable()

    index._prepare(edges)
    for sources in [[1], [100, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1]]:
        decoded.clear()
        groups = index.rows_for_sources(EdgeTable(), "LINK", sources, ("src_id",))  # type: ignore[arg-type]
        if sources[0] != 1:
            assert list(next(groups)) == []
        assert next(next(groups))["edge_id"] == 0
        assert decoded == [256]


@pytest.mark.parametrize("cap", [0, 1, 8 * 1024 * 1024])
def test_index_is_lazy_bounded_and_released(tmp_path: Path, cap: int) -> None:
    app = load_grapharrow(tmp_path / "db")
    app.settings.ana_adjacency_index_max_bytes_per_session = cap
    budget = AnaResourceBudget()
    with GraphArrowAgentBackend(app, resource_budget=budget) as backend:
        index = backend._adjacency
        assert (index is not None) == (cap > 1)
        assert index is None or index.outgoing is None
        backend.call("schema_overview", {})
        assert index is None or index.outgoing is None
        seed = backend.call("node_search", {"label": "Company", "value": "Acme"})
        result = backend.call(
            "expand",
            {
                "from": seed["handle"],
                "source": "company",
                "relationship_type": "CEO",
                "target_label": "Person",
                "limit": 1,
            },
        )
        assert backend.call("fetch", {"from": result["handle"]})["rows"] == [["Alice"]]
        assert index is None or index.outgoing is not None and index.incoming is not None
        assert budget.used_bytes == backend.resource_usage["bytes"]
    assert backend._adjacency is None
    assert budget.used_bytes == 0


@pytest.mark.parametrize("direction", ["out", "in", "both"])
def test_expand_matches_fallback_with_loops_parallel_edges_and_exclusions(
    tmp_path: Path, direction: str
) -> None:
    app = build_application(tmp_path / "db")
    _, nodes = app.use_cases.graph.create_nodes_bulk(
        table=pa.table({"name": ["one", "two", "three"]}), default_labels=("Node",)
    )
    a, b, c = [node.node_id for node in nodes]
    app.use_cases.graph.create_edges_bulk(
        table=pa.table(
            {
                "src_id": [a, b, a, a, c, a, c],
                "dst_id": [a, a, b, b, a, c, b],
                "rel_type": ["LINK", "LINK", "LINK", "LINK", "LINK", "OTHER", "LINK"],
            }
        )
    )
    signatures = []
    for cap in [0, 8 * 1024 * 1024]:
        app.settings.ana_adjacency_index_max_bytes_per_session = cap
        with GraphArrowAgentBackend(app) as backend:
            seed = backend.call("node_scan", {"label": "Node", "limit": 3})
            args = {
                "from": seed["handle"],
                "source": "node",
                "relationship_type": "LINK",
                "direction": direction,
                "target_label": "Node",
                "as": "target",
                "limit": 2,
            }
            first = backend.call("expand", args)
            second = backend.call(
                "expand",
                args | {"from": first["handle"], "source": "target", "as": "next", "limit": 100},
            )
            signatures.append(
                [
                    {key: value for key, value in first.items() if key != "handle"},
                    backend.call("fetch", {"from": first["handle"]})["rows"],
                    backend.call("fetch", {"from": second["handle"]})["rows"],
                    backend._handle_store.handles[first["handle"]].metadata,
                    backend._handle_store.handles[second["handle"]].metadata,
                ]
            )
    assert signatures[0] == signatures[1]


def test_index_pins_snapshot_and_failed_calls_keep_accounting(tmp_path: Path) -> None:
    app = load_grapharrow(tmp_path / "db")
    budget = AnaResourceBudget()
    with GraphArrowAgentBackend(app, resource_budget=budget) as old:
        seed = old.call("node_search", {"label": "Company", "value": "Acme"})
        args = {
            "from": seed["handle"],
            "source": "company",
            "relationship_type": "CEO",
            "target_label": "Person",
        }
        old.call("expand", args)
        assert old._adjacency is not None
        retained = old._adjacency.outgoing
        before = old.resource_usage
        limits = old._resources.limits
        old._resources.limits = replace(limits, max_bytes=before["bytes"])
        with pytest.raises(AnaQuotaExceeded):
            old.call("expand", args)
        assert old.resource_usage == before
        assert old._adjacency.outgoing is retained
        old._resources.limits = limits
        app.use_cases.cypher.execute(
            "MATCH (c:Company {name: 'Acme'}) CREATE (c)-[:CEO]->(n:Person {name: 'New'})"
        )
        app.use_cases.admin.compact()
        app.use_cases.admin.vacuum()
        assert old.call("expand", args)["matched_count"] == 2
        with GraphArrowAgentBackend(app, resource_budget=budget) as new:
            seed = new.call("node_search", {"label": "Company", "value": "Acme"})
            assert new.call("expand", args | {"from": seed["handle"]})["matched_count"] == 3
            assert new._adjacency is not old._adjacency
    assert budget.used_bytes == 0


def test_reservation_covers_many_type_ranges() -> None:
    edges = pa.table(
        {
            "edge_id": range(1000),
            "src_id": range(1000),
            "dst_id": range(1000),
            "rel_type": [f"type-{i}-é" for i in range(1000)],
        }
    )
    index = AnaAdjacency()
    index._prepare(edges)
    assert retained_size(index) <= index.reservation(1000, edges["rel_type"].to_pylist(), 1 << 30)
    assert AnaAdjacency.reservation(0, (), 1 << 30) == 0
    assert AnaAdjacency.reservation(1000, ["LINK"], 1) == 0


@pytest.mark.parametrize("shared", [False, True])
def test_index_reservation_falls_back_under_tight_quotas(tmp_path: Path, shared: bool) -> None:
    app = load_grapharrow(tmp_path / "db")
    app.settings.ana_adjacency_index_max_bytes_per_session = 0
    with GraphArrowAgentBackend(app) as probe:
        base_bytes = probe.resource_usage["bytes"]
    app.settings.ana_adjacency_index_max_bytes_per_session = 8 * 1024 * 1024
    budget = AnaResourceBudget(base_bytes + 128 if shared else 1 << 30)
    if not shared:
        app.settings.ana_max_bytes_per_session = base_bytes + 128
    with GraphArrowAgentBackend(app, resource_budget=budget) as backend:
        assert backend._adjacency is None
        assert backend.resource_usage["bytes"] == base_bytes
        assert budget.used_bytes == base_bytes
        assert backend.call("schema_overview", {})["labels"]
    assert budget.used_bytes == 0


def test_interrupted_build_publishes_neither_direction(monkeypatch: pytest.MonkeyPatch) -> None:
    edges = pa.table(
        {"edge_id": [4, 1, 7], "src_id": [1, 2, 1], "dst_id": [2, 1, 1], "rel_type": ["LINK"] * 3}
    )
    index = AnaAdjacency()
    original = pc.sort_indices
    calls = 0

    def interrupt(*args: Any, **kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise KeyboardInterrupt("Interrupted while building incoming adjacency")
        return original(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(pc, "sort_indices", interrupt)
        with pytest.raises(KeyboardInterrupt):
            index._prepare(edges)
    assert index.outgoing is None and index.incoming is None
    groups = index.rows_for_sources(edges, "LINK", [1], ("src_id", "dst_id"))
    assert [edge["edge_id"] for edge in next(groups)] == [4, 1, 7]


def test_failed_first_traversal_keeps_only_reserved_neutral_index(tmp_path: Path) -> None:
    app = load_grapharrow(tmp_path / "db")
    budget = AnaResourceBudget()
    with GraphArrowAgentBackend(app, resource_budget=budget) as backend:
        seed = backend.call("node_search", {"label": "Company", "value": "Acme"})
        assert backend._adjacency is not None and backend._adjacency.outgoing is None
        before = backend.resource_usage
        limits = backend._resources.limits
        backend._resources.limits = replace(limits, max_bytes=before["bytes"])
        args = {
            "from": seed["handle"],
            "source": "company",
            "relationship_type": "CEO",
            "target_label": "Person",
        }
        with pytest.raises(AnaQuotaExceeded):
            backend.call("expand", args)
        assert backend._adjacency.outgoing is not None
        assert backend.resource_usage == before
        assert set(backend._handle_store.handles) == {seed["handle"]}
        assert budget.used_bytes == before["bytes"]
        backend._resources.limits = limits
        assert backend.call("expand", args)["matched_count"] == 2
    assert budget.used_bytes == 0
