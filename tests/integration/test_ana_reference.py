"""Differential ANA workflows against an explicitly configured empty Neo4j DB.

Set GRAPHARROW_TEST_NEO4J_URI, GRAPHARROW_TEST_NEO4J_PASSWORD,
GRAPHARROW_TEST_ANA_URL and GRAPHARROW_TEST_ANA_KEY to opt in.
The reference service must use this database in transaction identity mode.
The fixture refuses a nonempty database and deletes only the nodes it creates.
"""

import json
import os
from collections import Counter
from typing import Any

import httpx
import pytest
from neo4j import GraphDatabase

from grapharrow.use_cases.ana import GraphArrowAgentBackend
from tests.ana_support import EDGES, NODES, ROLE_ARGS, SHARED_ARGS, fetch, load_grapharrow

pytestmark = [pytest.mark.integration, pytest.mark.neo4j_reference]


@pytest.fixture(scope="module")
def reference_graph(tmp_path_factory: pytest.TempPathFactory) -> Any:
    uri = os.environ.get("GRAPHARROW_TEST_NEO4J_URI")
    url = os.environ.get("GRAPHARROW_TEST_ANA_URL")
    if not uri or not url:
        pytest.skip("Set GRAPHARROW_TEST_NEO4J_URI and GRAPHARROW_TEST_ANA_URL for parity tests")
    key = os.environ["GRAPHARROW_TEST_ANA_KEY"]
    password = os.environ["GRAPHARROW_TEST_NEO4J_PASSWORD"]
    user = os.environ.get("GRAPHARROW_TEST_NEO4J_USER", "neo4j")
    application = load_grapharrow(tmp_path_factory.mktemp("ana-reference") / "db")
    identifiers = []
    with GraphDatabase.driver(uri, auth=(user, password), connection_timeout=5) as driver:
        driver.verify_connectivity()
        with driver.session() as session:
            if session.run("MATCH (n) RETURN count(n) AS count").single()["count"]:
                pytest.fail("ANA reference tests require an empty, disposable Neo4j database")
            try:
                for label, properties in NODES:
                    record = session.run(
                        f"CREATE (n:`{label}`) SET n = $props RETURN elementId(n) AS id",
                        props=properties,
                    ).single()
                    identifiers.append(record["id"])
                for source, target, relationship, properties in EDGES:
                    session.run(
                        f"MATCH (a), (b) WHERE elementId(a) = $source AND elementId(b) = $target "
                        f"CREATE (a)-[r:`{relationship}`]->(b) SET r = $props",
                        source=identifiers[source],
                        target=identifiers[target],
                        props=properties,
                    ).consume()
                yield application, url, key
            finally:
                session.run(
                    "MATCH (n) WHERE elementId(n) IN $ids DETACH DELETE n", ids=identifiers
                ).consume()


class ReferenceWorkflow:
    """Call the published implementation through its HTTP boundary."""

    def __init__(self, client: httpx.Client) -> None:
        self.client = client

    def call(self, tool: str, args: dict[str, Any]) -> dict[str, Any]:
        response = self.client.post("/tools/call", json={"tool": tool, "args": args})
        if response.status_code == 400:
            raise ValueError(response.json()["detail"])
        response.raise_for_status()
        return dict(response.json()["result"])


@pytest.fixture
def backends(reference_graph: Any) -> Any:
    application, url, key = reference_graph
    with (
        GraphArrowAgentBackend(application) as native,
        httpx.Client(
            base_url=url, headers={"Authorization": f"Bearer {key}"}, timeout=45
        ) as client,
    ):
        session = client.post("/sessions")
        session.raise_for_status()
        client.headers["X-ANA-Session"] = session.json()["session_id"]
        try:
            yield native, ReferenceWorkflow(client)
        finally:
            client.delete("/sessions/current").raise_for_status()


def test_discovery_parameters_match_the_public_http_service(backends: Any) -> None:
    native, reference = backends

    def without_prose(value: Any) -> Any:
        if isinstance(value, dict):
            return {k: without_prose(v) for k, v in value.items() if k != "description"}
        if isinstance(value, list):
            return [without_prose(item) for item in value]
        return value

    for planning in (False, True):
        response = reference.client.get("/tools/specs", params={"enable_planning_tools": planning})
        response.raise_for_status()
        expected = response.json()["tools"]
        actual = native.tool_specs(enable_planning_tools=planning)
        assert without_prose(actual) == without_prose(expected)


def rows_key(rows: list[Any]) -> Counter[str]:
    return Counter(json.dumps(row, sort_keys=True, default=str) for row in rows)


def assert_result(backends: Any, tool: str, args: dict[str, Any], *, ordered: bool = False) -> None:
    native, reference = [fetch(backend, tool, args) for backend in backends]
    assert native["columns"] == reference["columns"]
    assert native["returned_count"] == reference["returned_count"]
    assert (
        native["rows"] == reference["rows"]
        if ordered
        else rows_key(native["rows"]) == rows_key(reference["rows"])
    )


@pytest.mark.parametrize("mode", ["names", "entities", "count"])
@pytest.mark.parametrize("distinct", [True, False])
def test_constraint_modes(backends: Any, mode: str, distinct: bool) -> None:
    assert_result(
        backends,
        "constraint_query",
        {
            "find": {"label": "Company"},
            "where": [
                {
                    "relationship_type": "CEO",
                    "target_label": "Person",
                    "property": "name",
                    "value": "Alice",
                },
                {
                    "relationship_type": "LOCATED",
                    "target_label": "Country",
                    "property": "name",
                    "value": "Italy",
                },
            ],
            "return_mode": mode,
            "distinct": distinct,
            "return_properties": ["year"],
        },
    )


@pytest.mark.parametrize("keep", [True, False])
@pytest.mark.parametrize("explode", [True, False])
def test_same_target_projection_and_identity(backends: Any, keep: bool, explode: bool) -> None:
    select = [
        {
            "var": "person",
            "property": "countries" if explode else "name",
            "alias": "value",
            "explode": explode,
        }
    ]
    assert_result(
        backends,
        "same_target_role_intersection",
        {
            **ROLE_ARGS,
            "select": select,
            "keep_entities": keep,
            "order_by": [{"var": "person", "property": "age", "direction": "desc"}],
        },
    )


@pytest.mark.parametrize("aggregate", [True, False])
@pytest.mark.parametrize("seed_from", [True, False])
def test_shared_role_workflows(backends: Any, aggregate: bool, seed_from: bool) -> None:
    results = []
    for backend in backends:
        args = dict(SHARED_ARGS)
        if seed_from:
            args.pop("seed_filters")
            args["seed_from"] = backend.call("node_search", {"label": "Person", "value": "Alice"})[
                "handle"
            ]
        if aggregate:
            args.update(
                group_by=[{"var": "peer", "property": "name", "alias": "name"}],
                metrics=[{"var": "shared", "op": "count_distinct", "alias": "count"}],
            )
        else:
            args["select"] = [{"var": "peer", "property": "name"}]
        results.append(fetch(backend, "shared_role_aggregate", args))
    assert results[0]["columns"] == results[1]["columns"]
    assert rows_key(results[0]["rows"]) == rows_key(results[1]["rows"])


READ_CASES = [
    ("entity_resolve", {"label": "Person", "text": "Alex"}),
    ("value_search", {"label": "Person", "property": "name", "text": "al", "keep_entities": False}),
    ("node_search", {"label": "Person", "value": "Alex"}),
    ("node_scan", {"label": "Person", "limit": 100}),
    ("count_nodes", {"label": "Person"}),
    (
        "pattern_query",
        {
            "start_label": "Person",
            "hops": [],
            "select": [{"var": "person", "property": "age"}],
            "order_by": [{"field": "age"}],
        },
    ),
    (
        "relationship_query",
        {
            "source_label": "Company",
            "target_label": "Person",
            "relationship_type": "CEO",
            "rel_as": "role",
            "filters": [{"var": "role", "property": "since", "op": "gte", "value": 2000}],
            "select": [{"var": "company", "property": "name"}],
            "distinct": True,
        },
    ),
    (
        "multi_hop_query",
        {
            "start_label": "Person",
            "hops": [
                {"relationship_type": "WORKS", "target_label": "Company", "as": "company"},
                {"relationship_type": "LOCATED", "target_label": "Country", "as": "country"},
            ],
            "group_by": [{"var": "country", "property": "name", "alias": "country"}],
            "metrics": [{"var": "person", "op": "count_distinct", "alias": "people"}],
        },
    ),
    (
        "top_entities_by_property",
        {
            "start_label": "Company",
            "hops": [],
            "property": "year",
            "limit": 2,
            "include_property": True,
        },
    ),
    (
        "optional_count_by_pattern",
        {
            "start_label": "Person",
            "hops": [{"relationship_type": "WORKS", "target_label": "Company", "as": "company"}],
            "source": "company",
            "optional_relationship": {"relationship_type": "LOCATED", "target_label": "Country"},
            "group_by": [{"var": "company", "property": "name", "alias": "name"}],
        },
    ),
]


@pytest.mark.parametrize(("tool", "args"), READ_CASES, ids=[item[0] for item in READ_CASES])
def test_native_read_results(backends: Any, tool: str, args: dict[str, Any]) -> None:
    assert_result(backends, tool, args, ordered=bool(args.get("order_by")))


def test_composed_handles_lineage_pagination_and_counts(backends: Any) -> None:
    results = []
    for backend in backends:
        seed = backend.call("node_search", {"label": "Person", "value": "Alice", "as": "person"})
        expanded = backend.call(
            "expand",
            {
                "from": seed["handle"],
                "source": "person",
                "relationship_type": "CEO",
                "direction": "in",
                "target_label": "Company",
                "as": "company",
            },
        )
        filtered = backend.call(
            "filter",
            {
                "from": expanded["handle"],
                "var": "company",
                "property": "year",
                "op": "gte",
                "value": 2000,
            },
        )
        joined = backend.call(
            "join_handles",
            {
                "left": expanded["handle"],
                "right": filtered["handle"],
                "left_var": "company",
                "right_var": "company",
                "op": "left_semi",
                "as": "company",
            },
        )
        union = backend.call(
            "entity_set_operation",
            {
                "operands": [
                    {"handle": joined["handle"], "var": "company"},
                    {"handle": filtered["handle"], "var": "company"},
                ],
                "op": "union",
                "as": "company",
            },
        )
        projected = backend.call(
            "project",
            {
                "from": union["handle"],
                "select": [{"var": "company", "property": "name", "alias": "name"}],
                "order_by": [{"field": "name"}],
            },
        )
        pages = [
            backend.call("fetch", {"from": projected["handle"], "limit": 1, "offset": offset})
            for offset in (0, 1)
        ]
        recap = backend.call("handle_recap", {"from": projected["handle"]})
        results.append(
            {
                "pages": [(page["rows"], page["next_offset"], page["truncated"]) for page in pages],
                "lineage": [step["tool"] for step in recap["lineage"]],
                "count": fetch(
                    backend, "count_handle", {"from": union["handle"], "distinct": True}
                )["rows"],
            }
        )
    assert results[0] == results[1]


@pytest.mark.parametrize("tool", ["optional_expand_count", "expand_aggregate"])
def test_aggregate_from_handles(backends: Any, tool: str) -> None:
    results = []
    for backend in backends:
        seed = backend.call("node_scan", {"label": "Company", "as": "company", "limit": 100})
        args = {
            "from": seed["handle"],
            "source": "company",
            "relationship_type": "CEO",
            "direction": "out",
            "target_label": "Person",
            "as": "person",
            "group_by": [{"var": "company", "property": "name", "alias": "name"}],
        }
        if tool == "expand_aggregate":
            args["metrics"] = [{"var": "person", "op": "count_distinct", "alias": "count"}]
        results.append(fetch(backend, tool, args))
    assert results[0]["columns"] == results[1]["columns"]
    assert rows_key(results[0]["rows"]) == rows_key(results[1]["rows"])


@pytest.mark.parametrize(
    ("tool", "args"),
    [
        ("schema_overview", {}),
        ("schema_search", {"query": "Person Company CEO"}),
        ("schema_describe_label", {"label": "Person"}),
        ("schema_describe_relationship", {"relationship_type": "CEO"}),
        ("inspect_paths", {"source_label": "Person", "target_label": "Country", "max_hops": 3}),
        ("draft_tool_plan", {"question": "Count people working at Acme"}),
        (
            "validate_tool_plan",
            {"plan": [{"tool": "node_search", "args": {"label": "Person", "value": "Alice"}}]},
        ),
        (
            "repair_empty_result",
            {"tool": "node_search", "args": {"label": "Person", "value": "Missing"}},
        ),
    ],
)
def test_schema_planning_and_repair(backends: Any, tool: str, args: dict[str, Any]) -> None:
    native, reference = (backend.call(tool, args) for backend in backends)
    assert native.keys() == reference.keys()
    # Guidance is authored by each engine. Compare fields consumed by clients
    # and graph facts rather than the wording of hints or engine type names.
    if tool == "schema_overview":
        for key in ("relationship_types", "relationship_patterns"):
            assert native[key] == reference[key]
        assert len(native["labels"]) == len(reference["labels"])
        for local, remote in zip(native["labels"], reference["labels"], strict=True):
            for key in ("label", "count", "lookup_properties", "property_names"):
                assert local[key] == remote[key]
            assert local["safe_start_score"] == pytest.approx(remote["safe_start_score"])
    elif tool == "schema_search":
        for key in ("query", "labels", "node_properties", "relationship_properties"):
            assert native[key] == reference[key]
    elif tool in {"schema_describe_label", "schema_describe_relationship"}:
        fields = ("name", "mandatory") if tool == "schema_describe_label" else ("property",)
        fields += ("inferred_type", "operators", "examples")
        for result in (native, reference):
            result["properties"] = [
                {field: item[field] for field in fields} for item in result["properties"]
            ]
        assert native["properties"] == reference["properties"]
        if tool == "schema_describe_relationship":
            assert native["canonical_pattern"] == reference["canonical_pattern"]
            assert native["ready_to_use"] == reference["ready_to_use"]
    elif tool == "inspect_paths":
        for result in (native, reference):
            result["paths"] = [item["hops"] for item in result["paths"]]
        assert native["paths"] == reference["paths"]
    elif tool == "draft_tool_plan":
        assert native["features"] == reference["features"]
        assert native["recommended_tools"] == reference["recommended_tools"]
        assert native["plan_outline"]
    elif tool == "validate_tool_plan":
        assert native["valid"] == reference["valid"]
        assert native["plan_length"] == reference["plan_length"]
        assert [item["severity"] for item in native["warnings"]] == [
            item["severity"] for item in reference["warnings"]
        ]
    else:
        assert native["failed_tool"] == reference["failed_tool"]
        assert [item["action"] for item in native["suggestions"]] == [
            item["action"] for item in reference["suggestions"]
        ]


def test_local_composition_and_entity_equality(backends: Any) -> None:
    results = []
    for backend in backends:
        alice = backend.call("node_search", {"label": "Person", "value": "Alice", "as": "alice"})
        bob = backend.call("node_search", {"label": "Person", "value": "Bob", "as": "bob"})
        compared = fetch(
            backend,
            "compare",
            {
                "left": alice["handle"],
                "left_var": "alice",
                "right": bob["handle"],
                "right_var": "bob",
                "property": "age",
                "op": "gt",
            },
        )
        combined = backend.call(
            "combine",
            {
                "left": alice["handle"],
                "left_var": "alice",
                "right": bob["handle"],
                "right_var": "bob",
                "as": "person",
                "op": "union",
            },
        )
        grouped = fetch(
            backend,
            "group_handle",
            {
                "from": combined["handle"],
                "group_by": [{"var": "person", "property": "age", "alias": "age"}],
                "metrics": [{"var": "person", "op": "count_distinct", "alias": "count"}],
            },
        )
        count = backend.call("count_nodes", {"label": "Person", "alias": "count"})
        scalar = fetch(
            backend,
            "scalar_compute",
            {
                "inputs": [
                    {"handle": count["handle"], "column": "count"},
                    {"handle": count["handle"], "column": "count"},
                ],
                "op": "sum",
            },
        )
        paths = backend.call(
            "multi_hop_query",
            {
                "start_label": "Person",
                "start_as": "person",
                "hops": [
                    {"relationship_type": "WORKS", "target_label": "Company", "as": "company"},
                    {"relationship_type": "CEO", "target_label": "Person", "as": "director"},
                ],
            },
        )
        roles = backend.call("same_target_role_intersection", ROLE_ARGS)
        founders = backend.call(
            "expand",
            {
                "from": roles["handle"],
                "source": "company",
                "relationship_type": "FOUNDER",
                "direction": "out",
                "target_label": "Person",
                "as": "other",
            },
        )
        same = fetch(
            backend,
            "filter_same_node",
            {"from": founders["handle"], "left_var": "person", "right_var": "other"},
        )
        assert same["returned_count"] == 4
        summary = backend.call("summarize_handle", {"from": alice["handle"]})
        assert summary
        assert paths["handle"]
        results.append(
            {
                "compare": compared["rows"],
                "group": rows_key(grouped["rows"]),
                "scalar": scalar["rows"],
                "same": rows_key(same["rows"]),
            }
        )
    assert results[0] == results[1]


@pytest.mark.parametrize("branch_tool", ["constraint_query", "multi_hop_query", "expand"])
def test_union_count_uses_complete_entity_branches(backends: Any, branch_tool: str) -> None:
    results = []
    for backend in backends:
        branches = []
        for name in ("Alice", "Bob"):
            if branch_tool == "constraint_query":
                args = {
                    "find": {"label": "Company"},
                    "where": [
                        {
                            "relationship_type": "CEO",
                            "target_label": "Person",
                            "property": "name",
                            "value": name,
                        }
                    ],
                    "return_mode": "entities",
                    "limit": 1,
                }
            elif branch_tool == "multi_hop_query":
                args = {
                    "start_label": "Person",
                    "hops": [
                        {
                            "relationship_type": "CEO",
                            "direction": "in",
                            "target_label": "Company",
                            "as": "company",
                        }
                    ],
                    "filters": [{"var": "person", "property": "name", "op": "eq", "value": name}],
                    "limit": 1,
                }
            else:
                seed = backend.call("node_search", {"label": "Person", "value": name})
                args = {
                    "from": seed["handle"],
                    "source": "person",
                    "relationship_type": "CEO",
                    "direction": "in",
                    "target_label": "Company",
                    "as": "company",
                    "limit": 1,
                }
            branches.append(backend.call(branch_tool, args))
        union = backend.call(
            "entity_set_operation",
            {
                "op": "union",
                "operands": [{"handle": branch["handle"], "var": "company"} for branch in branches],
            },
        )
        results.append(
            fetch(
                backend,
                "count_handle",
                {"from": union["handle"], "var": "company", "distinct": True},
            )["rows"]
        )
    assert results == [[[3]], [[3]]]


@pytest.mark.parametrize("grouped", [True, False])
def test_empty_aggregate_results(backends: Any, grouped: bool) -> None:
    args = {
        "start_label": "Person",
        "hops": [],
        "filters": [{"var": "person", "property": "name", "op": "eq", "value": "Missing"}],
        "metrics": [
            {"var": "person", "op": "count_distinct", "alias": "count"},
            {"var": "person", "op": "sum", "property": "age", "alias": "total"},
        ],
    }
    if grouped:
        args["group_by"] = [{"var": "person", "property": "name", "alias": "name"}]
    assert_result(backends, "pattern_query", args)


def test_optional_expansion_keeps_zero_counts_and_entity_duplicates(backends: Any) -> None:
    results = []
    for backend in backends:
        source = backend.call("node_scan", {"label": "Person", "limit": 100})
        page = fetch(
            backend,
            "optional_expand_count",
            {
                "from": source["handle"],
                "source": "person",
                "relationship_type": "WORKS",
                "direction": "out",
                "target_label": "Company",
            },
        )
        assert page["returned_count"] == 6
        results.append(rows_key(page["rows"]))
    assert results[0] == results[1]


@pytest.mark.parametrize(
    ("tool", "args", "repair"),
    [
        (
            "constraint_query",
            {
                "find": {"label": "Company"},
                "where": [
                    {"relationship_type": "CEO", "target_label": "Person"},
                    {"relationship_type": "FOUNDER", "target_label": "Person"},
                ],
            },
            "same_target_role_intersection",
        ),
        (
            "same_target_role_intersection",
            {
                **ROLE_ARGS,
                "relationships": [{"relationship_type": "CEO", "direction": "in"}] * 2,
                "filters": [
                    {"var": "company", "property": "name", "op": "in", "value": ["Acme", "Beta"]}
                ],
            },
            "entity_set_operation",
        ),
    ],
)
def test_role_errors_provide_same_repair(
    backends: Any, tool: str, args: dict[str, Any], repair: str
) -> None:
    for backend in backends:
        with pytest.raises(ValueError, match=repair):
            backend.call(tool, args)
