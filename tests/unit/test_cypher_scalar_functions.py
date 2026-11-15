"""Regression tests for scalar Cypher functions."""

import math
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


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("split(',one,,two,', ',')", ["", "one", "", "two", ""]),
        ("split('a.b', '.')", ["a", "b"]),
        ("split('a😀é', '')", ["a", "😀", "é"]),
        ("split('', '')", ["", ""]),
        ("split('a', null)", None),
        ("substring('a😀é', 1, 1)", "😀"),
        ("substring('a😀é', 1)", "😀é"),
        ("substring('abc', 10)", ""),
        ("substring('abc', 1, 5)", "bc"),
        ("substring(null, null)", None),
    ],
)
def test_string_scalar_functions(tmp_path: Path, expression: str, expected: object) -> None:
    with Database(tmp_path / "db").read_transaction() as session:
        assert session.query(f"RETURN {expression} AS value").rows == ({"value": expected},)


def test_ceil_and_sqrt_return_ieee_floats(tmp_path: Path) -> None:
    with Database(tmp_path / "db").read_transaction() as session:
        row = session.query(
            "RETURN ceil(2) AS ceiling, sqrt(9) AS root, ceil(-0.5) AS zero, "
            "ceil(toFloat('Infinity')) AS infinity, sqrt(-1) AS nan, sqrt(null) AS missing"
        ).rows[0]
    assert type(row["ceiling"]) is float and row["ceiling"] == 2.0
    assert type(row["root"]) is float and row["root"] == 3.0
    assert row["zero"] == 0.0 and math.copysign(1, row["zero"]) == -1.0
    assert row["infinity"] == math.inf
    assert math.isnan(row["nan"])
    assert row["missing"] is None


@pytest.mark.parametrize(
    ("expression", "phase", "detail"),
    [
        ("split('a')", "compile", "InvalidNumberOfArguments"),
        ("substring('a')", "compile", "InvalidNumberOfArguments"),
        ("sqrt(1, 2)", "compile", "InvalidNumberOfArguments"),
        ("ceil(DISTINCT 1)", "compile", "InvalidArgumentPassingMode"),
        ("split('a', 1)", "runtime", "InvalidArgumentType"),
        ("substring('a', 0, null)", "runtime", "InvalidArgumentType"),
        ("substring('a', 1.0)", "runtime", "InvalidArgumentType"),
        ("substring('a', -1)", "runtime", "NumberOutOfRange"),
        ("sqrt(false)", "runtime", "InvalidArgumentType"),
    ],
)
def test_new_scalar_functions_reject_bad_arguments(
    tmp_path: Path, expression: str, phase: str, detail: str
) -> None:
    with Database(tmp_path / "db").read_transaction() as session, pytest.raises(QueryError) as err:
        session.query(f"RETURN {expression}")
    assert (err.value.phase, err.value.detail) == (phase, detail)
