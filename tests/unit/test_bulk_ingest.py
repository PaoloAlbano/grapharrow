from pathlib import Path

import pandas as pd
import pyarrow as pa  # type: ignore[import-untyped]
import pytest

from grapharrow import Database
from grapharrow.adapters import BulkInputAdapter
from grapharrow.core.errors import QueryError, SchemaError


def test_bulk_input_adapter_normalizes_supported_inputs() -> None:
    adapter = BulkInputAdapter()

    from_table = adapter.to_table(pa.Table.from_pylist([{"name": "Alice"}]))
    from_dataframe = adapter.to_table(pd.DataFrame([{"name": "Bob"}]))
    from_pylist = adapter.to_table([{"name": "Carol"}])

    assert from_table.to_pylist() == [{"name": "Alice"}]
    assert from_dataframe.to_pylist() == [{"name": "Bob"}]
    assert from_pylist.to_pylist() == [{"name": "Carol"}]


def test_session_add_nodes_bulk_supports_table_dataframe_and_pylist(tmp_path: Path) -> None:
    database = Database(tmp_path / "bulk-nodes-db")

    with database.write_transaction() as session:
        from_table = session.add_nodes_bulk(
            pa.Table.from_pylist(
                [
                    {"external_id": "alice", "name": "Alice"},
                    {"external_id": "bob", "name": "Bob"},
                ]
            ),
            default_labels=["Person"],
        )
        from_dataframe = session.add_nodes_bulk(
            pd.DataFrame([{"labels": ["Company"], "name": "Acme"}])
        )
        from_pylist = session.add_nodes_bulk(
            [
                {
                    "labels": ["Person", "Employee"],
                    "properties": {"name": "Carol"},
                    "role": "Engineer",
                }
            ]
        )

    assert [node["id"] for node in from_table] == [1, 2]
    assert from_dataframe[0]["labels"] == ["Company"]
    assert from_pylist[0]["properties"] == {"name": "Carol", "role": "Engineer"}

    with database.read_transaction() as session:
        count = session.query("MATCH (n) RETURN count(n) as count")

    assert count.rows == ({"count": 4},)


def test_session_add_edges_bulk_supports_internal_ids(tmp_path: Path) -> None:
    database = Database(tmp_path / "bulk-edges-by-id-db")

    with database.write_transaction() as session:
        nodes = session.add_nodes_bulk(
            [
                {"labels": ["Person"], "external_id": "alice", "name": "Alice"},
                {"labels": ["Person"], "external_id": "bob", "name": "Bob"},
                {"labels": ["Person"], "external_id": "carol", "name": "Carol"},
            ]
        )
        created_edges = session.add_edges_bulk(
            pa.Table.from_pylist(
                [
                    {
                        "src_id": nodes[0]["id"],
                        "dst_id": nodes[1]["id"],
                        "rel_type": "KNOWS",
                        "since": 2024,
                    },
                    {
                        "src_id": nodes[1]["id"],
                        "dst_id": nodes[2]["id"],
                        "rel_type": "KNOWS",
                        "since": 2025,
                    },
                ]
            )
        )

    assert [edge["id"] for edge in created_edges] == [1, 2]
    assert created_edges[0]["properties"] == {"since": 2024}

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "RETURN a.name, b.name, r.since ORDER BY a.name ASC"
        )

    assert result.rows == (
        {"a.name": "Alice", "b.name": "Bob", "r.since": 2024},
        {"a.name": "Bob", "b.name": "Carol", "r.since": 2025},
    )


def test_session_add_edges_bulk_supports_business_keys(tmp_path: Path) -> None:
    database = Database(tmp_path / "bulk-edges-by-key-db")

    with database.write_transaction() as session:
        session.add_nodes_bulk(
            [
                {"labels": ["Person"], "external_id": "alice", "name": "Alice"},
                {"labels": ["Person"], "external_id": "bob", "name": "Bob"},
            ]
        )
        created_edges = session.add_edges_bulk(
            pd.DataFrame(
                [
                    {
                        "src_key": "alice",
                        "dst_key": "bob",
                        "rel_type": "KNOWS",
                        "properties": {"since": 2024},
                    }
                ]
            ),
            source_key=("Person", "external_id"),
            target_key=("Person", "external_id"),
        )

    assert created_edges == (
        {
            "id": 1,
            "type": "KNOWS",
            "source": 1,
            "target": 2,
            "properties": {"since": 2024},
        },
    )


def test_session_add_edges_bulk_requires_both_reference_keys(tmp_path: Path) -> None:
    database = Database(tmp_path / "bulk-edges-invalid-db")

    with database.write_transaction() as session:
        session.add_nodes_bulk(
            [
                {"labels": ["Person"], "external_id": "alice"},
                {"labels": ["Person"], "external_id": "bob"},
            ]
        )
        with pytest.raises(QueryError):
            session.add_edges_bulk(
                [{"src_key": "alice", "dst_key": "bob", "rel_type": "KNOWS"}],
                source_key=("Person", "external_id"),
            )


def test_session_add_nodes_bulk_requires_labels_or_defaults(tmp_path: Path) -> None:
    database = Database(tmp_path / "bulk-nodes-invalid-db")

    with database.write_transaction() as session:
        with pytest.raises(SchemaError):
            session.add_nodes_bulk([{"name": "Alice"}])
