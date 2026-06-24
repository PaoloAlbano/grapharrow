import random
from typing import cast

import pytest

from grapharrow import Database
from tests.integration.support import LocalDiskContext


@pytest.mark.integration
def test_local_disk_medium_graph_supports_three_hop_query_with_ordering(
    local_disk_database: LocalDiskContext,
) -> None:
    database = Database(local_disk_database.database_dir)
    rng = random.Random(42)

    database.initialize()

    with database.write_transaction() as session:
        controlled_nodes: list[dict[str, object]] = []
        for index in range(12):
            controlled_nodes.append(
                session.add_node(
                    labels=["Person"],
                    properties={
                        "name": f"Seed-{index:02d}",
                        "external_id": f"seed-{index:02d}",
                        "anchor": index in {0, 7},
                        "segment": "controlled",
                    },
                )
            )

        bulk_nodes: list[dict[str, object]] = []
        for index in range(120):
            bulk_nodes.append(
                session.add_node(
                    labels=["Person"],
                    properties={
                        "name": f"Bulk-{index:03d}",
                        "external_id": f"bulk-{index:03d}",
                        "anchor": False,
                        "segment": "bulk",
                    },
                )
            )

        controlled_edges = [
            (0, 1),
            (1, 2),
            (2, 3),
            (0, 4),
            (4, 5),
            (5, 6),
            (7, 8),
            (8, 9),
            (9, 10),
        ]
        for src_index, dst_index in controlled_edges:
            session.add_edge(
                src=controlled_nodes[src_index]["id"],
                rel_type="KNOWS",
                dst=controlled_nodes[dst_index]["id"],
                properties={},
            )

        bulk_ids = [cast(int, node["id"]) for node in bulk_nodes]
        random_edges: set[tuple[int, int]] = set()
        while len(random_edges) < 600:
            src_id = rng.choice(bulk_ids)
            dst_id = rng.choice(bulk_ids)
            if src_id == dst_id:
                continue
            random_edges.add((src_id, dst_id))

        for src_id, dst_id in sorted(random_edges):
            session.add_edge(
                src=src_id,
                rel_type="KNOWS",
                dst=dst_id,
                properties={},
            )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person) "
            "WHERE a.anchor = true "
            "RETURN DISTINCT a.name, d.name "
            "ORDER BY d.name ASC "
            "LIMIT 10"
        )
        variable_length_result = session.query(
            "MATCH (a:Person)-[:KNOWS*1..5]->(b:Person) "
            "WHERE a.anchor = true "
            "RETURN DISTINCT a.name, b.name "
            "ORDER BY a.name ASC, b.name DESC "
            "LIMIT 20"
        )
        connected_patterns_result = session.query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person), (b)-[:KNOWS]->(c:Person) "
            "WHERE a.anchor = true "
            "RETURN DISTINCT a.name, b.name, c.name "
            "ORDER BY a.name ASC, b.name ASC, c.name ASC "
            "LIMIT 20"
        )
        projected_ordered_result = session.query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person) "
            "WHERE a.anchor = true "
            "RETURN a.external_id, d.external_id "
            "ORDER BY a.external_id ASC, d.external_id ASC "
            "LIMIT 10"
        )
        relationship_count = session.query("MATCH ()-[r]->() RETURN count(r) as count")

    assert result.columns == ("a.name", "d.name")
    assert result.rows == (
        {"a.name": "Seed-00", "d.name": "Seed-03"},
        {"a.name": "Seed-00", "d.name": "Seed-06"},
        {"a.name": "Seed-07", "d.name": "Seed-10"},
    )
    assert variable_length_result.columns == ("a.name", "b.name")
    assert variable_length_result.rows == (
        {"a.name": "Seed-00", "b.name": "Seed-06"},
        {"a.name": "Seed-00", "b.name": "Seed-05"},
        {"a.name": "Seed-00", "b.name": "Seed-04"},
        {"a.name": "Seed-00", "b.name": "Seed-03"},
        {"a.name": "Seed-00", "b.name": "Seed-02"},
        {"a.name": "Seed-00", "b.name": "Seed-01"},
        {"a.name": "Seed-07", "b.name": "Seed-10"},
        {"a.name": "Seed-07", "b.name": "Seed-09"},
        {"a.name": "Seed-07", "b.name": "Seed-08"},
    )
    assert connected_patterns_result.columns == ("a.name", "b.name", "c.name")
    assert connected_patterns_result.rows == (
        {"a.name": "Seed-00", "b.name": "Seed-01", "c.name": "Seed-02"},
        {"a.name": "Seed-00", "b.name": "Seed-04", "c.name": "Seed-05"},
        {"a.name": "Seed-07", "b.name": "Seed-08", "c.name": "Seed-09"},
    )
    assert projected_ordered_result.columns == ("a.external_id", "d.external_id")
    assert projected_ordered_result.rows == (
        {"a.external_id": "seed-00", "d.external_id": "seed-03"},
        {"a.external_id": "seed-00", "d.external_id": "seed-06"},
        {"a.external_id": "seed-07", "d.external_id": "seed-10"},
    )
    assert relationship_count.columns == ("count",)
    assert relationship_count.rows == ({"count": 609},)
    assert database.verify(verify_checksums=True).ok is True
