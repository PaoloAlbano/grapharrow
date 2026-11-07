"""Behavioral tests for native ANA discovery, pattern and optional operations."""

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from neo4j import GraphDatabase

from grapharrow.app import build_application
from grapharrow.use_cases.ana import GraphArrowAgentBackend
from grapharrow.use_cases.ana_patterns import edges_for_sources


@pytest.mark.parametrize(
    ("columns", "from_one", "from_two"),
    [
        (("src_id",), [5, 7, 9], [2]),
        (("dst_id",), [5, 2], [7, 9]),
        (("src_id", "dst_id"), [5, 7, 2, 9], [7, 2, 9]),
    ],
)
def test_edges_for_sources_preserves_parallel_edges_and_input_order(
    columns: tuple[str, ...], from_one: list[int], from_two: list[int]
) -> None:
    edges = pa.table({"edge_id": [5, 7, 2, 9], "src_id": [1, 1, 2, 1], "dst_id": [1, 2, 1, 2]})
    sources = [1, 3, 2, 1, 3, 2, 3, 1]
    expected = [from_one, [], from_two, from_one, [], from_two, [], from_one]
    for batch, max_batch_rows in ((False, None), (True, None), (True, 1)):
        groups = edges_for_sources(
            edges, sources, columns, batch=batch, max_batch_rows=max_batch_rows
        )
        assert [[edge["edge_id"] for edge in group] for group in groups] == expected


@pytest.fixture
def graph(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    def forbidden_driver(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("Native ANA reads must not create a Neo4j driver")

    monkeypatch.setattr(GraphDatabase, "driver", forbidden_driver)
    app = build_application(tmp_path / "native-patterns")
    _, devices = app.use_cases.graph.create_nodes_bulk(
        table=pa.Table.from_pylist(
            [
                {"name": "alpha", "reading": 1},
                {"name": "beta", "reading": 1},
                {"name": "gamma", "reading": 7},
                {"name": "delta", "reading": None},
            ]
        ),
        default_labels=("Device",),
    )
    _, zones = app.use_cases.graph.create_nodes_bulk(
        table=pa.Table.from_pylist([{"name": "North"}, {"name": "South"}, {"name": "Empty"}]),
        default_labels=("Zone",),
    )
    app.use_cases.graph.create_edges_bulk(
        table=pa.Table.from_pylist(
            [
                {
                    "src_id": devices[0].node_id,
                    "dst_id": zones[0].node_id,
                    "rel_type": "IN_ZONE",
                    "year": 1999,
                },
                {
                    "src_id": devices[0].node_id,
                    "dst_id": zones[0].node_id,
                    "rel_type": "IN_ZONE",
                    "year": 2020,
                },
                {
                    "src_id": devices[1].node_id,
                    "dst_id": zones[0].node_id,
                    "rel_type": "IN_ZONE",
                    "year": 2010,
                },
                {
                    "src_id": devices[2].node_id,
                    "dst_id": zones[1].node_id,
                    "rel_type": "IN_ZONE",
                    "year": 2022,
                },
            ]
        )
    )
    app.use_cases.graph.create_nodes_bulk(
        table=pa.Table.from_pylist(
            [
                {"name": "Acme", "aliases": ["ACME North"], "description": "North division"},
                {"name": "Acme", "aliases": ["ACME South"], "description": "South division"},
                {"name": "Acme Holdings", "aliases": [], "description": "Global group"},
            ]
        ),
        default_labels=("Company",),
    )
    with GraphArrowAgentBackend(app) as backend:
        yield backend


def result_rows(graph: Any, tool: str, args: dict[str, Any]) -> list[list[Any]]:
    handle = graph.call(tool, args)
    page = graph.call("fetch", {"from": handle["handle"]})
    assert page["returned_count"] == len(page["rows"])
    assert not any(key.startswith("internal-") for key in graph._handle_store.handles)
    return list(page["rows"])


@pytest.fixture
def large_pattern_graph(tmp_path: Path) -> Any:
    app = build_application(tmp_path / "streaming-pattern")
    _, start = app.use_cases.graph.create_node(labels=("Start",), properties={"name": "start"})
    _, devices = app.use_cases.graph.create_nodes_bulk(
        table=pa.table(
            {"name": [f"device-{i:03d}" for i in range(300)], "score": list(range(300))}
        ),
        default_labels=("Device",),
    )
    _, groups = app.use_cases.graph.create_nodes_bulk(
        table=pa.table({"name": [f"group-{i}" for i in range(3)]}), default_labels=("Group",)
    )
    edges = [
        {"src_id": start.node_id, "dst_id": device.node_id, "rel_type": "LINK"}
        for device in devices
        for _ in range(2)
    ]
    edges += [
        {"src_id": device.node_id, "dst_id": groups[i % 3].node_id, "rel_type": "IN_GROUP"}
        for i, device in enumerate(devices)
    ]
    app.use_cases.graph.create_edges_bulk(table=pa.Table.from_pylist(edges))
    with GraphArrowAgentBackend(app) as backend:
        yield backend


@pytest.mark.parametrize("distinct", [False, True])
def test_streamed_entity_pattern_materializes_limit_and_keeps_full_union(
    large_pattern_graph: Any, monkeypatch: pytest.MonkeyPatch, distinct: bool
) -> None:
    graph = large_pattern_graph
    materialized = []
    original_node = graph._node

    def node(node_id: int) -> Any:
        materialized.append(node_id)
        return original_node(node_id)

    monkeypatch.setattr(graph, "_node", node)
    result = graph.call(
        "multi_hop_query",
        {
            "start_label": "Start",
            "start_as": "s",
            "hops": [{"relationship_type": "LINK", "target_label": "Device", "as": "d"}],
            "limit": 10,
            "distinct": distinct,
        },
    )
    assert len(materialized) == 10
    expected = [f"device-{i if distinct else i // 2:03d}" for i in range(10)]
    assert graph.call("fetch", {"from": result["handle"]})["rows"] == [[name] for name in expected]
    combined = graph.call(
        "entity_set_operation",
        {
            "op": "union",
            "operands": [{"handle": result["handle"], "var": "d"}] * 2,
            "as": "d",
        },
    )
    assert result_rows(
        graph, "count_handle", {"from": combined["handle"], "var": "d", "distinct": True}
    ) == [[300]]


@pytest.mark.parametrize("distinct", [False, True])
def test_streamed_pattern_filters_and_sorts_before_limit(
    large_pattern_graph: Any, distinct: bool
) -> None:
    rows = result_rows(
        large_pattern_graph,
        "multi_hop_query",
        {
            "start_label": "Start",
            "hops": [{"relationship_type": "LINK", "target_label": "Device", "as": "d"}],
            "filters": [{"var": "d", "property": "score", "op": "gte", "value": 298}],
            "select": [{"var": "d", "property": "name", "alias": "name"}],
            "order_by": [{"var": "d", "property": "score", "direction": "desc"}],
            "distinct": distinct,
            "limit": 3,
        },
    )
    assert rows == (
        [["device-299"], ["device-298"]]
        if distinct
        else [["device-299"], ["device-299"], ["device-298"]]
    )


def test_streamed_multi_hop_pattern_aggregates_complete_groups(large_pattern_graph: Any) -> None:
    rows = result_rows(
        large_pattern_graph,
        "multi_hop_query",
        {
            "start_label": "Start",
            "hops": [
                {"relationship_type": "LINK", "target_label": "Device", "as": "d"},
                {"relationship_type": "IN_GROUP", "target_label": "Group", "as": "g"},
            ],
            "group_by": [{"var": "g", "property": "name", "alias": "group"}],
            "metrics": [
                {"var": "d", "op": "count", "alias": "count"},
                {"var": "d", "property": "score", "op": "avg", "alias": "average"},
            ],
            "order_by": [{"field": "group"}],
            "limit": 2,
        },
    )
    assert rows == [["group-0", 200, 148.5], ["group-1", 200, 149.5]]


@pytest.mark.parametrize(
    ("filters", "first_group", "distinct_groups"),
    [
        ([{"var": "d", "property": "score", "op": "gte", "value": 298}], "group-1", 2),
        ([{"var": "g", "property": "name", "value": "group-2"}], "group-2", 1),
    ],
)
def test_streamed_multi_hop_union_counts_filtered_branch(
    large_pattern_graph: Any,
    filters: list[dict[str, Any]],
    first_group: str,
    distinct_groups: int,
) -> None:
    graph = large_pattern_graph
    result = graph.call(
        "multi_hop_query",
        {
            "start_label": "Start",
            "hops": [
                {"relationship_type": "LINK", "target_label": "Device", "as": "d"},
                {"relationship_type": "IN_GROUP", "target_label": "Group", "as": "g"},
            ],
            "filters": filters,
            "limit": 1,
        },
    )
    assert graph.call("fetch", {"from": result["handle"]})["rows"] == [[first_group]]
    combined = graph.call(
        "entity_set_operation",
        {
            "op": "union",
            "operands": [{"handle": result["handle"], "var": "g"}] * 2,
            "as": "g",
        },
    )
    assert result_rows(
        graph, "count_handle", {"from": combined["handle"], "var": "g", "distinct": True}
    ) == [[distinct_groups]]


def test_streamed_pattern_preserves_bound_nodes_and_relationship_uniqueness(
    large_pattern_graph: Any,
) -> None:
    args = {
        "start_label": "Start",
        "start_as": "s",
        "hops": [
            {"relationship_type": "LINK", "target_label": "Device", "as": "d", "rel_as": "link"},
            {"relationship_type": "LINK", "target_label": "Start", "as": "s", "direction": "in"},
        ],
        "limit": 10,
    }
    assert result_rows(large_pattern_graph, "multi_hop_query", args) == [["start"]] * 10
    args["hops"][1]["rel_as"] = "link"
    assert result_rows(large_pattern_graph, "multi_hop_query", args) == []


def test_streamed_pattern_validates_later_hops_with_empty_input(large_pattern_graph: Any) -> None:
    with pytest.raises(ValueError, match="Unknown relationship_type"):
        large_pattern_graph.call(
            "multi_hop_query",
            {
                "start_label": "Start",
                "filters": [{"var": "start", "property": "name", "value": "missing"}],
                "hops": [
                    {"relationship_type": "LINK", "target_label": "Device"},
                    {"relationship_type": "UNKNOWN", "target_label": "Group"},
                ],
                "limit": 1,
            },
        )


def test_entity_resolution_context_aliases_and_best_match_class(graph: Any) -> None:
    hit = graph.call(
        "entity_resolve", {"label": "Company", "text": "Acme", "context_terms": ["South"]}
    )
    assert hit["matched_count"] == 1
    assert (
        graph._handle(hit["handle"]).rows[0]["company"]["properties"]["description"]
        == "South division"
    )
    assert hit["context_terms"] == ["south"]
    aliases = graph.call("entity_resolve", {"label": "Company", "text": "acme north"})
    assert aliases["matched_count"] == 1
    exact = graph.call("entity_resolve", {"label": "Company", "text": "Acme"})
    assert exact["matched_count"] == 2
    assert "ambiguity_hint" in exact
    assert result_rows(graph, "entity_resolve", {"label": "Company", "text": "missing"}) == []


@pytest.mark.parametrize(
    "mode,text,count",
    [
        ("exact", "Acme", 2),
        ("iexact", "acme", 2),
        ("contains", "hold", 1),
        ("prefix", "ac", 3),
        ("suffix", "ings", 1),
    ],
)
def test_value_search_scalar_pages(graph: Any, mode: str, text: str, count: int) -> None:
    handle = graph.call(
        "value_search",
        {
            "label": "Company",
            "property": "name",
            "text": text,
            "match_mode": mode,
            "keep_entities": False,
        },
    )
    page = graph.call("fetch", {"from": handle["handle"]})
    assert len(page["rows"]) == count
    assert page["columns"] == ["value", "match_score"]
    assert handle["matched_property"]["property"] == "name"


def test_value_search_preserves_entities_for_expansion(graph: Any) -> None:
    seed = graph.call("value_search", {"label": "Device", "property": "name", "text": "ALPHA"})
    assert seed["matched_count"] == 1
    zones = graph.call(
        "expand",
        {
            "from": seed["handle"],
            "source": "device",
            "relationship_type": "IN_ZONE",
            "direction": "out",
            "target_label": "Zone",
        },
    )
    assert zones["matched_count"] == 2


def test_relationship_filters_and_aggregate_order(graph: Any) -> None:
    rows = result_rows(
        graph,
        "relationship_query",
        {
            "source_label": "Device",
            "source_as": "device",
            "relationship_type": "IN_ZONE",
            "target_label": "Zone",
            "target_as": "zone",
            "rel_as": "placement",
            "filters": [{"var": "placement", "property": "year", "op": "gte", "value": 2020}],
            "group_by": [{"var": "zone", "property": "name", "alias": "zone"}],
            "metrics": [{"var": "device", "op": "count_distinct", "alias": "total"}],
            "order_by": [{"field": "zone", "direction": "asc"}],
        },
    )
    assert rows == [["North", 1], ["South", 1]]


def test_pattern_preserves_entity_distinctness_and_relationship_uniqueness(graph: Any) -> None:
    hop = {"relationship_type": "IN_ZONE", "target_label": "Zone", "as": "zone", "direction": "out"}
    args = {
        "start_label": "Device",
        "start_as": "device",
        "hops": [hop],
        "select": [{"var": "device", "property": "reading", "alias": "reading"}],
        "order_by": [{"field": "reading"}],
    }
    assert result_rows(graph, "pattern_query", args) == [[1], [1], [7]]
    assert result_rows(graph, "pattern_query", {**args, "distinct": False}) == [[1], [1], [1], [7]]
    peers = result_rows(
        graph,
        "multi_hop_query",
        {
            "start_label": "Device",
            "start_as": "device",
            "hops": [
                hop,
                {
                    "relationship_type": "IN_ZONE",
                    "direction": "in",
                    "target_label": "Device",
                    "as": "peer",
                },
            ],
            "filters": [{"var": "device", "property": "name", "op": "eq", "value": "beta"}],
            "metrics": [{"var": "peer", "op": "count_distinct", "alias": "peers"}],
        },
    )
    assert peers == [[1]]


def test_pattern_zero_aggregates_and_grouped_empty_results(graph: Any) -> None:
    args = {
        "start_label": "Device",
        "start_as": "device",
        "hops": [],
        "filters": [{"var": "device", "property": "name", "op": "eq", "value": "missing"}],
        "metrics": [
            {"var": "device", "op": "count", "alias": "count"},
            {"var": "device", "property": "reading", "op": "sum", "alias": "sum"},
            {"var": "device", "property": "reading", "op": "avg", "alias": "avg"},
        ],
    }
    assert result_rows(graph, "pattern_query", args) == [[0, 0, None]]
    assert (
        result_rows(
            graph,
            "pattern_query",
            {**args, "group_by": [{"var": "device", "property": "name", "alias": "name"}]},
        )
        == []
    )


def test_optional_count_keeps_zeros_and_distinguishes_parallel_edges(graph: Any) -> None:
    seed = graph.call("node_scan", {"label": "Device", "limit": 10, "as": "device"})
    args = {
        "from": seed["handle"],
        "source": "device",
        "relationship_type": "IN_ZONE",
        "direction": "out",
        "target_label": "Zone",
    }
    assert result_rows(graph, "optional_expand_count", args) == [
        ["alpha", 1],
        ["beta", 1],
        ["gamma", 1],
        ["delta", 0],
    ]
    assert result_rows(graph, "optional_expand_count", {**args, "distinct": False}) == [
        ["alpha", 2],
        ["beta", 1],
        ["gamma", 1],
        ["delta", 0],
    ]


def test_expand_aggregate_and_top_entities(graph: Any) -> None:
    seed = graph.call("node_search", {"label": "Zone", "value": "North", "as": "zone"})
    rows = result_rows(
        graph,
        "expand_aggregate",
        {
            "from": seed["handle"],
            "source": "zone",
            "relationship_type": "IN_ZONE",
            "direction": "in",
            "target_label": "Device",
            "as": "device",
            "metrics": [{"var": "device", "op": "count_distinct", "alias": "devices"}],
        },
    )
    assert rows == [[2]]
    top = graph.call(
        "top_entities_by_property",
        {
            "start_label": "Device",
            "property": "reading",
            "order": "asc",
            "limit": 1,
            "include_property": True,
        },
    )
    assert graph.call("fetch", {"from": top["handle"]})["rows"] == [["alpha", 1]]
    assert all(not col.startswith("__rank") for col in top["columns"])


def test_planning_and_empty_result_repair_are_local(graph: Any) -> None:
    assert graph.call("draft_tool_plan", {"question": "How many devices are in each zone?"})[
        "plan_outline"
    ]
    validation = graph.call(
        "validate_tool_plan", {"plan": [{"tool": "node_scan", "args": {"label": "Device"}}]}
    )
    assert isinstance(validation["warnings"], list)
    repaired = graph.call(
        "repair_empty_result",
        {"tool": "pattern_query", "args": {"start_label": "Device", "hops": []}},
    )
    assert repaired["suggestions"]


@pytest.mark.parametrize("distinct,north", [(True, 2), (False, 3)])
def test_optional_pattern_deduplicates_source_bindings_before_counting(
    graph: Any, distinct: bool, north: int
) -> None:
    args = {
        "start_label": "Zone",
        "start_as": "zone",
        "hops": [
            {
                "relationship_type": "IN_ZONE",
                "direction": "in",
                "target_label": "Device",
                "as": "device",
            }
        ],
        "optional_relationship": {
            "relationship_type": "IN_ZONE",
            "direction": "in",
            "target_label": "Device",
            "as": "optional_device",
        },
        "group_by": [{"var": "zone", "property": "name", "alias": "zone"}],
        "order_by": [{"field": "zone"}],
        "distinct": distinct,
    }
    assert result_rows(graph, "optional_count_by_pattern", args) == [["North", north], ["South", 1]]
    args["optional_relationship"]["relationship_filters"] = [
        {"property": "year", "op": "gte", "value": 2021}
    ]
    assert result_rows(graph, "optional_count_by_pattern", args) == [["North", 0], ["South", 1]]


def test_native_patterns_match_equivalent_cypher_queries(graph: Any) -> None:
    args = {
        "start_label": "Device",
        "start_as": "device",
        "hops": [{"relationship_type": "IN_ZONE", "target_label": "Zone", "as": "zone"}],
        "filters": [{"var": "device", "property": "reading", "op": "gte", "value": 1}],
        "group_by": [{"var": "zone", "property": "name", "alias": "zone"}],
        "metrics": [{"var": "device", "op": "count_distinct", "alias": "devices"}],
        "order_by": [{"field": "zone"}],
    }
    cypher = graph._application.use_cases.cypher.execute(
        "MATCH (d:Device)-[:IN_ZONE]->(z:Zone) WHERE d.reading >= 1 "
        "RETURN z.name AS zone, count(DISTINCT d) AS devices ORDER BY zone",
        {},
    )
    assert result_rows(graph, "pattern_query", args) == [
        [row["zone"], row["devices"]] for row in cypher.rows
    ]


def test_bad_filter_variable_does_not_publish_partial_handles(graph: Any) -> None:
    before = set(graph._handle_store.handles)
    with pytest.raises(ValueError, match="unknown pattern variable"):
        graph.call(
            "pattern_query",
            {
                "start_label": "Device",
                "hops": [],
                "filters": [{"var": "missing", "property": "name", "op": "eq", "value": "alpha"}],
            },
        )
    assert set(graph._handle_store.handles) == before


@pytest.mark.parametrize(
    ("extra", "error"),
    [
        (
            {"filters": [{"var": "device", "property": "reading", "op": "invented"}]},
            "Unsupported filter op",
        ),
        (
            {"filters": [{"var": "device", "property": "reading", "op": "in", "value": [1]}]},
            "scalar",
        ),
        (
            {"metrics": [{"var": "device", "op": "invented", "alias": "result"}]},
            "Unsupported metric op",
        ),
    ],
)
def test_invalid_pattern_operations_fail_on_empty_results(
    graph: Any, extra: dict[str, Any], error: str
) -> None:
    before = set(graph._handle_store.handles)
    args = {
        "start_label": "Device",
        "hops": [],
        **extra,
        "filters": [
            {"var": "device", "property": "name", "op": "eq", "value": "no-match"},
            *extra.get("filters", []),
        ],
    }
    with pytest.raises(ValueError, match=error):
        graph.call("pattern_query", args)
    assert set(graph._handle_store.handles) == before


def test_empty_grouped_pattern_has_reference_empty_columns(graph: Any) -> None:
    result = graph.call(
        "pattern_query",
        {
            "start_label": "Device",
            "hops": [],
            "filters": [{"var": "device", "property": "name", "op": "eq", "value": "missing"}],
            "group_by": [{"var": "device", "property": "name"}],
            "metrics": [{"var": "device", "op": "count", "alias": "count"}],
        },
    )
    assert result["columns"] == []
    page = graph.call("fetch", {"from": result["handle"]})
    assert page["columns"] == []
    assert page["rows"] == []
