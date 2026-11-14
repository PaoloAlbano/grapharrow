"""Regression tests for list accessors and properties()."""

from pathlib import Path

import pytest

from grapharrow import Database
from grapharrow.core.errors import QueryError


def test_list_accessors_preserve_nulls_and_nested_values(tmp_path: Path) -> None:
    with Database(tmp_path / "db").read_transaction() as session:
        assert session.query(
            "RETURN [x IN $xs | [head(x), last(x), tail(x)]] AS values",
            {"xs": [None, [], [None], [1, [2], {"x": 3}]]},
        ).rows == (
            {
                "values": [
                    [None, None, None],
                    [None, None, []],
                    [None, None, []],
                    [1, {"x": 3}, [[2], {"x": 3}]],
                ]
            },
        )


def test_head_can_bind_a_node_and_properties_reads_live_values(tmp_path: Path) -> None:
    with Database(tmp_path / "db").write_transaction() as session:
        assert session.query(
            "CREATE (p:N {x:1, removed:0})-[r:R {y:2}]->(:N) "
            "WITH head([p]) AS n, last([r]) AS r "
            "SET n.x=3, r.y=4 REMOVE n.removed "
            "RETURN properties(n) AS n, properties(r) AS r"
        ).rows == ({"n": {"x": 3}, "r": {"y": 4}},)


def test_properties_of_map_and_optional_match(tmp_path: Path) -> None:
    with Database(tmp_path / "db").read_transaction() as session:
        assert session.query(
            "OPTIONAL MATCH (n) RETURN properties(n) AS n, properties($m) AS m",
            {"m": {"x": [None, 1]}},
        ).rows == ({"n": None, "m": {"x": [None, 1]}},)


@pytest.mark.parametrize("call", ["head(1)", "last('text')", "tail({})", "properties([true])"])
def test_static_invalid_operands(tmp_path: Path, call: str) -> None:
    with Database(tmp_path / "db").read_transaction() as session, pytest.raises(QueryError) as err:
        session.query(f"RETURN {call}")
    assert (err.value.phase, err.value.detail) == ("compile", "InvalidArgumentType")


@pytest.mark.parametrize("function", ["head", "last", "tail", "properties"])
def test_dynamic_invalid_operands(tmp_path: Path, function: str) -> None:
    with Database(tmp_path / "db").read_transaction() as session, pytest.raises(QueryError) as err:
        session.query(f"UNWIND [1] AS x RETURN {function}(x)")
    assert (err.value.phase, err.value.category) == ("runtime", "TypeError")
