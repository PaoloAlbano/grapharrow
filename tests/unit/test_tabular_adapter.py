import pyarrow as pa  # type: ignore[import-untyped]

from grapharrow.adapters import PyArrowTabularAdapter


def test_tabular_adapter_supports_select_sort_and_join() -> None:
    adapter = PyArrowTabularAdapter()
    left = pa.table(
        {
            "node_id": [3, 1, 2],
            "age": [40, 20, 30],
        }
    )
    right = pa.table(
        {
            "node_id": [1, 2],
            "country": ["IT", "DE"],
        }
    )

    selected = adapter.select_columns(left, ["node_id", "age"])
    sorted_table = adapter.sort_table(selected, [("age", "descending")])
    joined = adapter.inner_join(sorted_table, right, left_keys="node_id")

    assert sorted_table.column_names == ["node_id", "age"]
    assert sorted_table["node_id"].to_pylist() == [3, 2, 1]
    assert joined.column_names == ["node_id", "age", "country"]
    assert joined["node_id"].to_pylist() == [2, 1]


def test_tabular_adapter_returns_empty_table_for_empty_input() -> None:
    adapter = PyArrowTabularAdapter()
    schema = pa.schema([("node_id", pa.int64()), ("value", pa.string())])

    table = adapter.concatenate_tables([], schema=schema)

    assert table.schema == schema
    assert table.num_rows == 0


def test_tabular_adapter_supports_left_join_with_null_extension() -> None:
    adapter = PyArrowTabularAdapter()
    left = pa.table(
        {
            "node_id": [1, 2],
            "name": ["Alice", "Bob"],
        }
    )
    right = pa.table(
        {
            "node_id": [1],
            "country": ["IT"],
        }
    )

    joined = adapter.left_join(left, right, left_keys="node_id")

    assert joined.column_names == ["node_id", "name", "country"]
    assert joined.to_pylist() == [
        {"node_id": 1, "name": "Alice", "country": "IT"},
        {"node_id": 2, "name": "Bob", "country": None},
    ]
