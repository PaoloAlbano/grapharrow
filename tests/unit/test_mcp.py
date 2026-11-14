from pathlib import Path
from typing import Any, cast

import pytest

from grapharrow.core.errors import QueryError, TransactionError
from grapharrow.domain import (
    CatalogVersion,
    ConstraintDefinition,
    LabelDefinition,
    LabelId,
    PropertyDefinition,
    RelationshipTypeDefinition,
    RelationshipTypeId,
    VersionedCatalog,
)
from grapharrow.interfaces.mcp import create_mcp_interface
from grapharrow.interfaces.mcp_cypher import create_mcp_cypher_interface


def test_mcp_lists_the_initial_five_tools(tmp_path: Path) -> None:
    interface = create_mcp_interface(tmp_path / "example-db")

    assert [tool.name for tool in interface.list_tools()] == [
        "query",
        "write_cypher",
        "get_schema",
        "get_constraints",
        "get_indexes",
    ]


def test_mcp_cypher_interface_matches_backward_compatible_surface(tmp_path: Path) -> None:
    legacy_interface = create_mcp_interface(tmp_path / "legacy-db")
    cypher_interface = create_mcp_cypher_interface(tmp_path / "cypher-db")

    assert [tool.name for tool in cypher_interface.list_tools()] == [
        tool.name for tool in legacy_interface.list_tools()
    ]


def test_mcp_query_enforces_read_only_semantics(tmp_path: Path) -> None:
    interface = create_mcp_interface(tmp_path / "example-db")

    with pytest.raises(TransactionError, match="read-only"):
        interface.query(query="CREATE (n:Person {name: 'Alice'}) RETURN n")


def test_mcp_can_execute_read_and_write_cypher_tools(tmp_path: Path) -> None:
    interface = create_mcp_interface(tmp_path / "example-db")

    write_result = interface.write_cypher(
        query="CREATE (n:Person {name: $name}) RETURN n.name",
        params={"name": "Alice"},
    )
    read_result = interface.query(
        query="MATCH (n:Person) RETURN n.name ORDER BY n.name ASC",
        params={},
    )

    assert write_result == {
        "columns": ["n.name"],
        "rows": [{"n.name": "Alice"}],
        "row_count": 1,
        "returned_row_count": 1,
        "truncated": False,
        "summary": {
            "tool": "write_cypher",
            "column_count": 1,
            "row_count": 1,
            "returned_row_count": 1,
            "result_kind": "single_row",
        },
        "warnings": [],
    }
    assert read_result == {
        "columns": ["n.name"],
        "rows": [{"n.name": "Alice"}],
        "row_count": 1,
        "returned_row_count": 1,
        "truncated": False,
        "summary": {
            "tool": "query",
            "column_count": 1,
            "row_count": 1,
            "returned_row_count": 1,
            "result_kind": "single_row",
        },
        "warnings": [],
    }


def test_mcp_query_applies_row_guardrails_and_summary(tmp_path: Path) -> None:
    interface = create_mcp_interface(tmp_path / "example-db")
    interface.write_cypher(query="CREATE (n:Person {name: 'Alice'}) RETURN n.name")
    interface.write_cypher(query="CREATE (n:Person {name: 'Bob'}) RETURN n.name")
    interface.write_cypher(query="CREATE (n:Person {name: 'Carol'}) RETURN n.name")

    result = interface.query(
        query="MATCH (n:Person) RETURN n.name ORDER BY n.name ASC",
        max_rows=2,
    )

    assert result["columns"] == ["n.name"]
    assert result["rows"] == [{"n.name": "Alice"}, {"n.name": "Bob"}]
    assert result["row_count"] == 3
    assert result["returned_row_count"] == 2
    assert result["truncated"] is True
    assert result["summary"] == {
        "tool": "query",
        "column_count": 1,
        "row_count": 3,
        "returned_row_count": 2,
        "result_kind": "multi_row",
    }
    assert result["warnings"] == [
        "Result rows were truncated to the first 2 rows for MCP consumption."
    ]


@pytest.mark.parametrize(
    "query",
    [
        "MATCH (a)-[:KNOWS*1..5]->(b) RETURN a.name, b.name",
        "MATCH p=()-[:R*..4]-() RETURN p",
        "MATCH p=()-[:R*1..2]->()\n-[:S*0..2]->() RETURN p",
        "MATCH p=()--()--()--()--() RETURN p",
    ],
)
def test_mcp_query_enforces_hop_guardrail(tmp_path: Path, query: str) -> None:
    interface = create_mcp_interface(tmp_path / "example-db")

    with pytest.raises(QueryError, match="hop limit"):
        interface.query(
            query=query,
            max_hops=3,
        )


def test_mcp_returns_schema_constraints_and_indexes(tmp_path: Path) -> None:
    interface = create_mcp_interface(tmp_path / "example-db")
    interface.grapharrow.repositories.catalog.write(
        VersionedCatalog(
            version=CatalogVersion(2),
            labels=(
                LabelDefinition(id=LabelId(1), name="Person"),
                LabelDefinition(id=LabelId(2), name="Company"),
            ),
            relationship_types=(
                RelationshipTypeDefinition(id=RelationshipTypeId(1), name="WORKS_AT"),
            ),
            properties=(
                PropertyDefinition(
                    name="external_id",
                    owner_kind="label",
                    owner_name="Person",
                    data_type="string",
                    nullable=False,
                ),
                PropertyDefinition(
                    name="created_at",
                    owner_kind="relationship_type",
                    owner_name="WORKS_AT",
                    data_type="timestamp",
                    nullable=False,
                ),
            ),
            constraints=(
                ConstraintDefinition(
                    kind="unique",
                    owner_kind="label",
                    owner_name="Person",
                    property_name="external_id",
                ),
                ConstraintDefinition(
                    kind="endpoint",
                    owner_kind="relationship_type",
                    owner_name="WORKS_AT",
                    source_labels=("Person",),
                    target_labels=("Company",),
                ),
            ),
        )
    )
    interface.write_cypher(
        query=(
            "CREATE (a:Person {name: 'Alice', external_id: 'alice'}), "
            "(b:Company {name: 'Acme'}), "
            "(a)-[:WORKS_AT {created_at: '2026-04-01T10:00:00Z'}]->(b) "
            "RETURN a.name, b.name"
        )
    )

    schema = interface.get_schema()
    constraints = interface.get_constraints()
    indexes = interface.get_indexes()
    snapshot_id = cast(int, indexes["snapshot_id"])
    node_labels = cast(list[dict[str, Any]], indexes["node_labels"])
    node_properties = cast(list[dict[str, Any]], indexes["node_properties"])
    adjacency_by_source = cast(list[dict[str, Any]], indexes["adjacency_by_source"])
    adjacency_by_dest = cast(list[dict[str, Any]], indexes["adjacency_by_dest"])
    edge_properties = cast(list[dict[str, Any]], indexes["edge_properties"])

    assert schema == {
        "catalog_version": 2,
        "labels": [
            {"id": 1, "name": "Person"},
            {"id": 2, "name": "Company"},
        ],
        "relationship_types": [{"id": 1, "name": "WORKS_AT"}],
        "properties": [
            {
                "owner_kind": "label",
                "owner_name": "Person",
                "name": "external_id",
                "type": "string",
                "nullable": False,
                "deprecated": False,
            },
            {
                "owner_kind": "relationship_type",
                "owner_name": "WORKS_AT",
                "name": "created_at",
                "type": "timestamp",
                "nullable": False,
                "deprecated": False,
            },
        ],
    }
    assert constraints == {
        "catalog_version": 2,
        "constraints": [
            {
                "kind": "unique",
                "owner_kind": "label",
                "owner_name": "Person",
                "property_name": "external_id",
                "source_labels": [],
                "target_labels": [],
            },
            {
                "kind": "endpoint",
                "owner_kind": "relationship_type",
                "owner_name": "WORKS_AT",
                "property_name": None,
                "source_labels": ["Person"],
                "target_labels": ["Company"],
            },
        ],
    }
    assert indexes["catalog_version"] == 2
    assert snapshot_id >= 2
    assert node_labels[0]["label"] == "Company"
    assert node_labels[0]["row_count"] == 1
    assert node_labels[0]["checksum_algorithm"] == "sha256"
    assert node_labels[0]["checksum"] is not None
    assert cast(str, node_labels[0]["path"]).endswith(".parquet")
    assert node_labels[1]["label"] == "Person"
    assert node_labels[1]["row_count"] == 1
    assert node_labels[1]["checksum_algorithm"] == "sha256"
    assert node_labels[1]["checksum"] is not None
    assert node_properties == [
        {
            "label": "Company",
            "path": f"derived/node_properties/Company/snapshot-{snapshot_id:06d}.parquet",
            "row_count": 1,
            "checksum": node_properties[0]["checksum"],
            "checksum_algorithm": "sha256",
            "min_key": 2,
            "max_key": 2,
        },
        {
            "label": "Person",
            "path": f"derived/node_properties/Person/snapshot-{snapshot_id:06d}.parquet",
            "row_count": 1,
            "checksum": node_properties[1]["checksum"],
            "checksum_algorithm": "sha256",
            "min_key": 1,
            "max_key": 1,
        },
    ]
    assert node_properties[0]["checksum"] is not None
    assert adjacency_by_source == [
        {
            "relationship_type": "WORKS_AT",
            "path": (f"derived/adjacency/by_source/WORKS_AT/snapshot-{snapshot_id:06d}.parquet"),
            "row_count": 1,
            "checksum": adjacency_by_source[0]["checksum"],
            "checksum_algorithm": "sha256",
            "min_key": 1,
            "max_key": 1,
        }
    ]
    assert adjacency_by_source[0]["checksum"] is not None
    assert adjacency_by_dest == [
        {
            "relationship_type": "WORKS_AT",
            "path": (f"derived/adjacency/by_dest/WORKS_AT/snapshot-{snapshot_id:06d}.parquet"),
            "row_count": 1,
            "checksum": adjacency_by_dest[0]["checksum"],
            "checksum_algorithm": "sha256",
            "min_key": 2,
            "max_key": 2,
        }
    ]
    assert adjacency_by_dest[0]["checksum"] is not None
    assert edge_properties == [
        {
            "relationship_type": "WORKS_AT",
            "path": f"derived/edge_properties/WORKS_AT/snapshot-{snapshot_id:06d}.parquet",
            "row_count": 1,
            "checksum": edge_properties[0]["checksum"],
            "checksum_algorithm": "sha256",
            "min_key": 1,
            "max_key": 1,
        }
    ]
    assert edge_properties[0]["checksum"] is not None


def test_mcp_call_tool_dispatches_and_validates_inputs(tmp_path: Path) -> None:
    interface = create_mcp_interface(tmp_path / "example-db")

    with pytest.raises(QueryError, match="Unknown MCP tool"):
        interface.call_tool("unknown")
    with pytest.raises(QueryError, match="non-empty string"):
        interface.call_tool("query", {"query": ""})
    with pytest.raises(QueryError, match="JSON object"):
        interface.call_tool("query", {"query": "MATCH (n) RETURN n", "params": []})
    with pytest.raises(QueryError, match="must be an integer"):
        interface.call_tool("query", {"query": "MATCH (n) RETURN n", "max_rows": "10"})

    write_result = interface.call_tool(
        "write_cypher",
        {"query": "CREATE (n:Person {name: 'Alice'}) RETURN n.name"},
    )
    read_result = interface.call_tool(
        "query",
        {"query": "MATCH (n:Person) RETURN n.name"},
    )
    write_summary = cast(dict[str, Any], write_result["summary"])
    read_summary = cast(dict[str, Any], read_result["summary"])

    assert write_result["rows"] == [{"n.name": "Alice"}]
    assert write_summary["tool"] == "write_cypher"
    assert read_result["rows"] == [{"n.name": "Alice"}]
    assert read_summary["tool"] == "query"
