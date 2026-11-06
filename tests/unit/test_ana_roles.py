"""Native role constraints must preserve identity and remain composable."""

import json
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from neo4j import GraphDatabase

from grapharrow.interfaces.rest import create_rest_app
from grapharrow.use_cases.ana import GraphArrowAgentBackend
from tests.ana_support import ROLE_ARGS, SHARED_ARGS, fetch, load_grapharrow


@pytest.fixture
def backend(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    def forbidden(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("Native ANA must not create a Neo4j driver")

    monkeypatch.setattr(GraphDatabase, "driver", forbidden)
    with GraphArrowAgentBackend(load_grapharrow(tmp_path / "roles")) as graph:
        yield graph


@pytest.mark.parametrize(
    ("mode", "distinct", "expected"),
    [
        ("names", True, [["Acme", 2000]]),
        ("names", False, [["Acme", 2000], ["Acme", 2000]]),
        ("count", True, [[1]]),
        ("count", False, [[2]]),
    ],
)
def test_independent_constraints_and_parallel_edges(
    backend: Any, mode: str, distinct: bool, expected: list[Any]
) -> None:
    args = {
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
    }
    assert fetch(backend, "constraint_query", args)["rows"] == expected


def test_constraint_entities_can_continue_to_expansion(backend: Any) -> None:
    result = backend.call(
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
            ],
            "return_mode": "entities",
            "order_by": [{"var": "company", "property": "name"}],
            "offset": 1,
            "limit": 1,
        },
    )
    assert fetch(
        backend,
        "expand",
        {
            "from": result["handle"],
            "source": "company",
            "relationship_type": "FOUNDER",
            "direction": "out",
            "target_label": "Person",
        },
    )["rows"] == [["Bob"]]


def test_role_intersection_keeps_same_target_and_scalar_distinct(backend: Any) -> None:
    result = fetch(
        backend,
        "same_target_role_intersection",
        {
            **ROLE_ARGS,
            "select": [
                {"var": "person", "property": "name", "alias": "name"},
                {"var": "company", "property": "name", "alias": "company"},
            ],
            "order_by": [{"field": "name"}],
        },
    )
    assert result["rows"] == [["Alex", "Empty"], ["Alice", "Acme"], ["Bob", "Gamma"]]


def test_role_projection_keeps_duplicate_entities_and_explodes_lists(backend: Any) -> None:
    result = backend.call(
        "same_target_role_intersection",
        {
            **ROLE_ARGS,
            "keep_entities": True,
            "select": [{"var": "person", "property": "name", "alias": "name"}],
        },
    )
    assert len(backend._handle(result["handle"]).rows) == 4
    counted = fetch(
        backend, "count_handle", {"from": result["handle"], "var": "person", "distinct": True}
    )
    assert counted["rows"] == [[4]]
    result = fetch(
        backend,
        "same_target_role_intersection",
        {
            **ROLE_ARGS,
            "select": [
                {"var": "person", "property": "countries", "alias": "country", "explode": True}
            ],
            "order_by": [{"var": "person", "property": "age", "direction": "desc"}],
        },
    )
    assert result["rows"] == [["IT"], ["FR"]]


@pytest.mark.parametrize("seed_from", [False, True])
def test_shared_roles_seed_handle_and_aggregate(backend: Any, seed_from: bool) -> None:
    args = dict(SHARED_ARGS)
    if seed_from:
        seed = backend.call("node_search", {"label": "Person", "value": "Alice"})
        args.pop("seed_filters")
        args["seed_from"] = seed["handle"]
    args.update(
        group_by=[{"var": "peer", "property": "name", "alias": "name"}],
        metrics=[{"var": "shared", "op": "count_distinct", "alias": "companies"}],
    )
    assert fetch(backend, "shared_role_aggregate", args)["rows"] == [["Bob", 1]]


def test_shared_roles_entities_continue_and_can_include_seed(backend: Any) -> None:
    result = backend.call("shared_role_aggregate", SHARED_ARGS)
    assert fetch(
        backend,
        "project",
        {"from": result["handle"], "select": [{"var": "peer", "property": "name"}]},
    )["rows"] == [["Bob"]]
    assert fetch(
        backend,
        "shared_role_aggregate",
        {
            **SHARED_ARGS,
            "exclude_seed": False,
            "select": [{"var": "peer", "property": "name"}],
            "order_by": [{"field": "name"}],
        },
    )["rows"] == [["Alice"], ["Bob"]]


def test_ambiguous_constraints_have_actionable_repair(backend: Any) -> None:
    with pytest.raises(ValueError, match="same_target_role_intersection"):
        backend.call(
            "constraint_query",
            {
                "find": {"label": "Company"},
                "where": [
                    {"relationship_type": "CEO", "target_label": "Person"},
                    {"relationship_type": "FOUNDER", "target_label": "Person"},
                ],
            },
        )


def test_complete_catalog_and_role_workflows_over_default_http(
    backend: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    auth_file = tmp_path / "clients.json"
    auth_file.write_text(
        json.dumps({"clients": [{"id": "reader", "api_key": "a" * 32, "profile": "readonly"}]})
    )
    monkeypatch.setenv("ANA_AUTH_FILE", str(auth_file))
    headers = {"Authorization": "Bearer " + "a" * 32}
    with TestClient(create_rest_app(backend._application.layout.root)) as client:
        response = client.get("/tools/specs", headers=headers)
        assert response.status_code == 200
        default = {tool["function"]["name"] for tool in response.json()["tools"]}
        assert len(default) == 35
        response = client.get("/tools/specs?enable_planning_tools=true", headers=headers)
        enabled = {tool["function"]["name"] for tool in response.json()["tools"]}
        assert enabled - default == {"draft_tool_plan", "validate_tool_plan"}
        response = client.post("/sessions", headers=headers)
        assert response.status_code == 201
        headers["X-ANA-Session"] = response.json()["session_id"]
        workflows = [
            (
                "constraint_query",
                {
                    "find": {"label": "Company"},
                    "where": [
                        {
                            "relationship_type": "CEO",
                            "target_label": "Person",
                            "property": "name",
                            "value": "Alice",
                        }
                    ],
                    "return_mode": "count",
                },
                [[2]],
            ),
            (
                "same_target_role_intersection",
                {
                    **ROLE_ARGS,
                    "select": [{"var": "person", "property": "name"}],
                    "filters": [
                        {"var": "company", "property": "name", "op": "eq", "value": "Acme"}
                    ],
                },
                [["Alice"]],
            ),
            (
                "shared_role_aggregate",
                {**SHARED_ARGS, "select": [{"var": "peer", "property": "name"}]},
                [["Bob"]],
            ),
        ]
        for tool, args, expected in workflows:
            response = client.post(
                "/tools/call", headers=headers, json={"tool": tool, "args": args}
            )
            assert response.status_code == 200, response.text
            handle = response.json()["result"]["handle"]
            response = client.post(
                "/tools/call", headers=headers, json={"tool": "fetch", "args": {"from": handle}}
            )
            assert response.status_code == 200, response.text
            assert response.json()["result"]["rows"] == expected
        assert client.delete("/sessions/current", headers=headers).status_code == 204


def test_union_count_keeps_full_branches_while_fetch_remains_limited(backend: Any) -> None:
    branches = [
        backend.call(
            "constraint_query",
            {
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
            },
        )
        for name in ("Alice", "Bob")
    ]
    union = backend.call(
        "entity_set_operation",
        {
            "op": "union",
            "operands": [{"handle": branch["handle"], "var": "company"} for branch in branches],
        },
    )
    assert fetch(
        backend, "count_handle", {"from": union["handle"], "var": "company", "distinct": True}
    )["rows"] == [[3]]
    page = backend.call("fetch", {"from": union["handle"]})
    assert page["returned_count"] == 2
    assert fetch(backend, "count_handle", {"from": union["handle"], "var": "company"})["rows"] == [
        [2]
    ]
    with pytest.raises(ValueError, match="entity_set_operation"):
        backend.call(
            "same_target_role_intersection",
            {
                **ROLE_ARGS,
                "relationships": [{"relationship_type": "CEO", "direction": "in"}] * 2,
                "filters": [
                    {"var": "company", "property": "name", "op": "in", "value": ["Acme", "Beta"]}
                ],
            },
        )
