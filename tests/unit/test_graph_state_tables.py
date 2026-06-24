from pathlib import Path

from grapharrow import Database


def test_graph_state_exposes_arrow_tables_alongside_records(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        alice = session.add_node(labels=["Person"], properties={"name": "Alice"})
        bob = session.add_node(labels=["Person"], properties={"name": "Bob"})
        session.add_edge(src=alice["id"], rel_type="KNOWS", dst=bob["id"], properties={})

    state = database._app.use_cases.graph.load_state()

    assert state.node_table.num_rows == 2
    assert state.edge_table.num_rows == 1
    assert tuple(state.node_table.column_names) == ("node_id", "labels", "properties_json")
    assert tuple(state.edge_table.column_names) == (
        "edge_id",
        "rel_type",
        "src_id",
        "dst_id",
        "properties_json",
    )
    assert len(state.nodes) == 2
    assert len(state.edges) == 1
