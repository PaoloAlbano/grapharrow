"""Prefiltering preserves exact ANA predicates, ranking, errors and ownership."""

import json
import random
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from grapharrow.app import build_application
from grapharrow.repositories.graph import NodeRecord
from grapharrow.use_cases import ana_patterns, ana_search
from grapharrow.use_cases.ana import GraphArrowAgentBackend


class ApplicationString(str):
    pass


class ApplicationInteger(int):
    pass


def graph(path: Path, rows: list[dict[str, Any]]) -> Any:
    app = build_application(path)
    app.settings.ana_read_cache_enabled = False
    app.use_cases.graph.publish_records(
        previous_state=app.use_cases.graph.load_state(),
        new_nodes=tuple(NodeRecord(i, ("Item",), props) for i, props in enumerate(rows)),
        new_edges=(),
    )
    return app


def response(backend: Any, tool: str, args: dict[str, Any]) -> Any:
    result = backend.call(tool, args)
    return (
        {key: value for key, value in result.items() if key != "handle"},
        backend.call("fetch", {"from": result["handle"], "limit": 1000}),
    )


@pytest.mark.parametrize(
    "op,value",
    [
        ("eq", 17),
        ("neq", 17),
        ("gt", 17),
        ("gte", 17),
        ("lt", 17),
        ("lte", 17),
        ("gte_or_null", 17),
        ("lte_or_null", 17),
        ("is_null", None),
        ("is_not_null", None),
        ("eq", None),
        ("neq", None),
        ("gte_or_null", None),
    ],
)
def test_native_seed_predicates_match_materialized_filter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, op: str, value: Any
) -> None:
    rows = [{"name": f"item-{i}", "value": i if i % 4 else None} for i in range(80)]
    rows[0].pop("value")
    app = graph(tmp_path / "db", rows)
    with GraphArrowAgentBackend(app) as backend:
        args = {
            "start_label": "Item",
            "hops": [],
            "start_as": "x",
            "filters": [{"var": "x", "property": "value", "op": op, "value": value}],
            "select": [{"var": "x", "property": "name", "alias": "name"}],
            "limit": 1000,
        }
        actual = response(backend, "pattern_query", args)
        with monkeypatch.context() as patch:
            patch.setattr(
                ana_patterns, "select_node_ids", lambda backend, label, nodes, *args: nodes
            )
            expected = response(backend, "pattern_query", args)
        assert actual == expected


@pytest.mark.parametrize(
    "properties,flt",
    [
        ([{"value": True}, {"value": False}], {"property": "value", "value": 1}),
        ([{"value": True}, {"value": False}], {"property": "value", "op": "neq", "value": 1}),
        ([{"value": 1}, {"value": 1.5}], {"property": "value", "value": 1}),
        ([{"value": [1, True]}, {"value": [True]}], {"property": "value", "value": 1}),
        ([{"value": {"x": 1}}, {"value": {"x": True}}], {"property": "value", "value": {"x": 1}}),
        ([{"node_id": "app-id"}, {"node_id": "other"}], {"property": "node_id", "value": "app-id"}),
        (
            [{"value": "İstanbul"}, {"value": "ISTANBUL"}],
            {"property": "value", "op": "eq_ci", "value": "i̇stanbul"},
        ),
        (
            [{"value": "a,b"}, {"value": "abc"}],
            {"property": "value", "op": "contains", "value": ","},
        ),
        (
            [{"value": "a,b"}, {"value": "abc"}],
            {"property": "value", "op": "not_contains", "value": ","},
        ),
        (
            [{"value": 1}, {"value": 2}],
            {"property": "value", "value": "1", "value_type": "integer"},
        ),
        ([{"value": 1}, {"value": 2}], {"property": "value", "op": "lt", "value": 2**80}),
        ([{"value": ApplicationString("north")}], {"property": "value", "value": "north"}),
        ([{"value": ApplicationString("7")}], {"property": "value", "value": "7"}),
        ([{"value": ApplicationString('"quoted"')}], {"property": "value", "value": '"quoted"'}),
        ([{"value": ApplicationInteger(7)}], {"property": "value", "value": 7}),
        (
            [{"value": float("nan")}, {"value": 1.5}, {"value": float("inf")}],
            {"property": "value", "op": "neq", "value": 1.5},
        ),
        (
            [{"value": float("nan")}, {"value": 1.5}, {"value": float("inf")}],
            {"property": "value", "op": "lte", "value": 1.5},
        ),
        (
            [{"value": 2**53 + 1}, {"value": 1.5}],
            {"property": "value", "op": "gt", "value": 2**53},
        ),
    ],
)
def test_complex_seed_filters_keep_canonical_semantics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    properties: list[dict[str, Any]],
    flt: dict[str, Any],
) -> None:
    app = graph(tmp_path / "db", [{"name": str(i), **props} for i, props in enumerate(properties)])
    with GraphArrowAgentBackend(app) as backend:
        args = {
            "start_label": "Item",
            "hops": [],
            "start_as": "x",
            "filters": [{"var": "x", **flt}],
        }
        actual = response(backend, "pattern_query", args)
        with monkeypatch.context() as patch:
            patch.setattr(
                ana_patterns, "select_node_ids", lambda backend, label, nodes, *args: nodes
            )
            assert response(backend, "pattern_query", args) == actual


@pytest.mark.parametrize("layout", ["current", "reordered", "missing_rows", "legacy"])
def test_seed_materialization_scope_and_projection_fallbacks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, layout: str
) -> None:
    app = graph(tmp_path / "db", [{"name": f"item-{i}", "value": i} for i in range(1000)])
    app.use_cases.indexes.create_index(
        name="item_name",
        kind="range",
        owner_kind="label",
        owner_name="Item",
        property_names=("name",),
    )
    with GraphArrowAgentBackend(app) as backend:
        table = backend._property_tables["Item"]
        if layout == "reordered":
            table = table.take(pa.array(list(reversed(range(table.num_rows)))))
        elif layout == "missing_rows":
            table = table.slice(1)
        elif layout == "legacy":
            table = pa.Table.from_arrays(
                table.columns,
                schema=pa.schema([pa.field(field.name, field.type) for field in table.schema]),
            )
        backend._property_tables["Item"] = table
        seed = backend.call("node_scan", {"label": "Item", "as": "x", "limit": 10})
        materialized = []
        original = backend._materialize_nodes

        def materialize(ids: list[int]) -> Any:
            materialized.extend(ids)
            return original(ids)

        monkeypatch.setattr(backend, "_materialize_nodes", materialize)
        args = {
            "from": seed["handle"],
            "start_label": "Item",
            "hops": [],
            "start_as": "x",
            "filters": [{"var": "x", "property": "name", "value": "item-0"}],
        }
        assert response(backend, "pattern_query", args)[1]["rows"] == [["item-0"]]
        assert materialized == [0]
        args["filters"][0]["value"] = "item-42"
        assert response(backend, "pattern_query", args)[1]["rows"] == []
        assert materialized == [0]
        if layout in {"current", "reordered"}:
            assert app.use_cases.graph.property_indexes.hits >= 1


def test_seed_filter_error_order_and_short_circuit_are_preserved(tmp_path: Path) -> None:
    app = graph(tmp_path / "db", [{"name": "one", "value": 1}])
    with GraphArrowAgentBackend(app) as backend:
        invalid = {"var": "x", "property": "value", "value": "bad", "value_type": "integer"}
        miss = {"var": "x", "property": "name", "value": "missing"}
        args = {"start_label": "Item", "hops": [], "start_as": "x", "filters": [invalid, miss]}
        with pytest.raises(ValueError, match="invalid literal"):
            backend.call("pattern_query", args)
        assert (
            response(backend, "pattern_query", args | {"filters": [miss, invalid]})[1]["rows"] == []
        )
        with pytest.raises(ValueError, match="Unknown relationship_type"):
            backend.call(
                "multi_hop_query",
                args | {"hops": [{"relationship_type": "MISSING", "target_label": "Item"}]},
            )


@pytest.fixture
def discovery_backend(tmp_path: Path) -> Any:
    rng = random.Random(71)
    names = ["Acme", "ACME", "acme holdings", "", None, "İstanbul", "Straße", "Kelvin", "ΟΣ"]
    aliases = [[], None, ["Acme north"], [""], ["A\\B", 'A"B'], ["İSTANBUL"], ["south"]]
    descriptions = ["north division", "south group", "neutral", None, "Ελλάδα"]
    rows = [
        {"name": f"device-{i:06d}", "aliases": [f"alias-{i}"], "description": "north"}
        for i in range(180)
    ]
    rows += [
        {
            "name": rng.choice(names),
            "aliases": rng.choice(aliases),
            "description": rng.choice(descriptions),
        }
        for _ in range(120)
    ]
    app = graph(tmp_path / "db", rows)
    with GraphArrowAgentBackend(app) as backend:
        yield backend


@pytest.mark.parametrize(
    "text,terms,limit",
    [
        ("device-000042", [], 5),
        ("device-000042", ["north"], 5),
        ("Acme", ["south"], 5),
        ("acme", ["north", "group"], 20),
        ("Acme north group", [], 5),
        ("İSTANBUL", [], 5),
        ("i̇stanbul", [], 5),
        ("STRASSE", [], 5),
        ("kelvin", [], 5),
        ("ος", [], 5),
        ("A\\B", [], 5),
        ('A"B', [], 5),
        ("", [], 5),
        (" ", [], 5),
        ("north", ["a,b", "Ελλάδα"], 5),
        ("x" * 100, [], 5),
        ("missing", [], 5),
    ],
)
def test_entity_ranking_matches_full_python_scoring(
    discovery_backend: Any,
    monkeypatch: pytest.MonkeyPatch,
    text: str,
    terms: list[str],
    limit: int,
) -> None:
    args = {
        "label": "Item",
        "text": text,
        "context_terms": terms,
        "limit": limit,
        "allow_large": True,
    }
    actual = response(discovery_backend, "entity_resolve", args)
    with monkeypatch.context() as patch:
        patch.setattr(ana_search, "_entity_prefilter", lambda *args: None)
        assert response(discovery_backend, "entity_resolve", args) == actual


@pytest.mark.parametrize(
    "bad",
    [
        {"aliases": [{"invalid": True}]},
        {"aliases": True},
        {"name": ["invalid"]},
        {"description": {"invalid": True}},
    ],
)
def test_entity_prefilter_does_not_hide_invalid_scalar_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, bad: dict[str, Any]
) -> None:
    rows = [{"name": f"unrelated-{i}", "aliases": [], "description": "ok"} for i in range(70)]
    rows[0]["name"] = "target"
    rows[-1].update(bad)
    app = graph(tmp_path / "db", rows)
    with GraphArrowAgentBackend(app) as backend:
        before = backend.resource_usage
        with pytest.raises((ValueError, TypeError)) as actual:
            backend.call("entity_resolve", {"label": "Item", "text": "target"})
        with monkeypatch.context() as patch:
            patch.setattr(ana_search, "_entity_prefilter", lambda *args: None)
            with pytest.raises(type(actual.value)) as expected:
                backend.call("entity_resolve", {"label": "Item", "text": "target"})
        assert str(actual.value) == str(expected.value)
        assert backend.resource_usage == before


def test_entity_common_tokens_score_only_bounded_finalists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = graph(
        tmp_path / "db",
        [
            {"name": f"device-{i:06d}", "aliases": [f"alias-{i}"], "description": "device north"}
            for i in range(2000)
        ],
    )
    with GraphArrowAgentBackend(app) as backend:
        scored = []
        original = ana_search._search_rows

        def rows(*args: Any, **kwargs: Any) -> Any:
            for node, props in original(*args, **kwargs):
                scored.append(node)
                yield node, props

        monkeypatch.setattr(ana_search, "_search_rows", rows)
        result, fetched = response(
            backend, "entity_resolve", {"label": "Item", "text": "device-000042"}
        )
        assert fetched["rows"] == [["device-000042"]]
        assert len(scored) <= 13 and 42 in scored
        assert len(result["alternative_candidates"]) <= 5


@pytest.mark.parametrize("hops", [1, 2])
def test_seed_selection_keeps_limited_pattern_branch_membership(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, hops: int
) -> None:
    app = graph(tmp_path / "db", [{"name": f"item-{i}", "value": i} for i in range(100)])
    app.use_cases.graph.create_edges_bulk(
        table=pa.table(
            {
                "src_id": [i for i in range(100) for _ in range(2)],
                "dst_id": [(i + step) % 100 for i in range(100) for step in (1, 2)],
                "rel_type": ["LINK"] * 200,
            }
        )
    )
    with GraphArrowAgentBackend(app) as backend:
        materialized = []
        original = backend._materialize_nodes

        def materialize(ids: list[int]) -> Any:
            materialized.extend(ids)
            return original(ids)

        monkeypatch.setattr(backend, "_materialize_nodes", materialize)
        args = {
            "start_label": "Item",
            "start_as": "x",
            "limit": 1,
            "filters": [{"var": "x", "property": "value", "value": 42}],
            "hops": [
                {"relationship_type": "LINK", "target_label": "Item", "as": f"n{i}"}
                for i in range(hops)
            ],
        }
        first = backend.call("multi_hop_query", args)
        metadata = backend._handle_store.handles[first["handle"]].metadata
        actual = backend.call("fetch", {"from": first["handle"]})
        assert materialized == [42]
        assert len(metadata["native_entity_query"]["node_ids"]) == hops + 1
        with monkeypatch.context() as patch:
            patch.setattr(
                ana_patterns, "select_node_ids", lambda backend, label, nodes, *args: nodes
            )
            second = backend.call("multi_hop_query", args)
            assert backend.call("fetch", {"from": second["handle"]}) == actual
            assert backend._handle_store.handles[second["handle"]].metadata == metadata


@pytest.mark.parametrize("layout", ["reordered", "missing_rows", "legacy", "collision"])
def test_entity_projection_layouts_preserve_ranking(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, layout: str
) -> None:
    rows = [
        {"name": f"device-{i:03d}", "aliases": [f"alias-{i}"], "description": "north"}
        for i in range(100)
    ]
    if layout == "collision":
        rows = [row | {"node_id": f"application-{i}"} for i, row in enumerate(rows)]
    app = graph(tmp_path / "db", rows)
    with GraphArrowAgentBackend(app) as backend:
        table = backend._property_tables["Item"]
        if layout == "reordered":
            table = table.take(pa.array(list(reversed(range(table.num_rows)))))
        elif layout == "missing_rows":
            table = table.slice(1)
        elif layout == "legacy":
            table = pa.Table.from_arrays(
                table.columns,
                schema=pa.schema([pa.field(field.name, field.type) for field in table.schema]),
            )
        backend._property_tables["Item"] = table
        args = {"label": "Item", "text": "device-000", "context_terms": ["north"]}
        actual = response(backend, "entity_resolve", args)
        with monkeypatch.context() as patch:
            patch.setattr(ana_search, "_entity_prefilter", lambda *args: None)
            assert response(backend, "entity_resolve", args) == actual


def test_ties_and_indexed_seed_filters_keep_canonical_identity(tmp_path: Path) -> None:
    app = graph(
        tmp_path / "db",
        [
            {
                "name": "Acme",
                "aliases": [],
                "marker": i,
                "description": "south" if i % 10 == 0 else "north",
            }
            for i in range(100)
        ],
    )
    app.use_cases.indexes.create_index(
        name="region",
        kind="range",
        owner_kind="label",
        owner_name="Item",
        property_names=("description",),
    )
    with GraphArrowAgentBackend(app) as backend:
        table = backend._property_tables["Item"]
        backend._property_tables["Item"] = table.take(pa.array(list(reversed(range(100)))))
        found = backend.call(
            "entity_resolve",
            {"label": "Item", "text": "Acme", "context_terms": ["south"], "limit": 5},
        )
        rows = backend._handle_store.handles[found["handle"]].rows
        assert [row["item"].reference.key for row in rows] == [str(i) for i in (0, 10, 20, 30, 40)]
        filtered = response(
            backend,
            "pattern_query",
            {
                "start_label": "Item",
                "start_as": "x",
                "hops": [],
                "limit": 3,
                "filters": [{"var": "x", "property": "description", "value": "south"}],
                "select": [{"var": "x", "property": "marker", "alias": "marker"}],
            },
        )
        assert filtered[1]["rows"] == [[0], [10], [20]]


@pytest.mark.parametrize("raw", ['["bad\x00value"]', '\f["value"]'])
def test_entity_pruning_keeps_projection_json_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, raw: str
) -> None:
    app = graph(
        tmp_path / "db",
        [
            {"name": name, "aliases": ["value"], "description": "ok"}
            for name in ["target", *[f"unrelated-{i}" for i in range(69)]]
        ],
    )
    with GraphArrowAgentBackend(app) as backend:
        table = backend._property_tables["Item"]
        backend._property_tables["Item"] = table.set_column(
            table.schema.get_field_index("aliases"),
            table.schema.field("aliases"),
            pa.array(['["value"]'] * 69 + [raw]),
        )
        args = {"label": "Item", "text": "target"}
        with pytest.raises(json.JSONDecodeError) as first:
            backend.call("entity_resolve", args)
        with monkeypatch.context() as patch:
            patch.setattr(ana_search, "_entity_prefilter", lambda *args: None)
            with pytest.raises(json.JSONDecodeError) as second:
                backend.call("entity_resolve", args)
        assert str(first.value) == str(second.value)


def test_native_surrogate_queries_keep_python_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = graph(tmp_path / "db", [{"name": f"target-{i}", "aliases": []} for i in range(70)])
    with GraphArrowAgentBackend(app) as backend:
        args = {"label": "Item", "text": "\ud800"}
        actual = response(backend, "entity_resolve", args)
        with monkeypatch.context() as patch:
            patch.setattr(ana_search, "_entity_prefilter", lambda *args: None)
            assert response(backend, "entity_resolve", args) == actual
        args = {
            "start_label": "Item",
            "hops": [],
            "filters": [{"var": "item", "property": "name", "value": "\ud800"}],
        }
        actual = response(backend, "pattern_query", args)
        with monkeypatch.context() as patch:
            patch.setattr(
                ana_patterns, "select_node_ids", lambda backend, label, nodes, *args: nodes
            )
            assert response(backend, "pattern_query", args) == actual
