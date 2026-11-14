"""Regression tests for scalar conversions and sign()."""

from pathlib import Path

import pytest

from grapharrow import Database
from grapharrow.core.errors import QueryError


@pytest.mark.parametrize(
    ("function", "values", "expected"),
    [
        (
            "toInteger",
            [True, False, -2.9, "+2.9", "1e3", "bad", None],
            [1, 0, -2, 2, 1000, None, None],
        ),
        ("toFloat", [3, -2.5, " 1.2e2 ", "bad", None], [3.0, -2.5, 120.0, None, None]),
        (
            "toBoolean",
            [True, False, -2, 0, " TRUE ", "FaLsE", "1", None],
            [True, False, True, False, True, False, None, None],
        ),
        ("toString", [True, False, 42, 2.3, "x", None], ["true", "false", "42", "2.3", "x", None]),
        ("sign", [-2.5, -1, 0, 3, None], [-1, -1, 0, 1, None]),
    ],
)
def test_scalar_conversion_of_parameterized_values(
    tmp_path: Path, function: str, values: list[object], expected: list[object]
) -> None:
    db = Database(tmp_path / "db")
    with db.read_transaction() as session:
        result = session.query(f"RETURN [x IN $xs | {function}(x)] AS result", {"xs": values})
    converted = result.rows[0]["result"]
    assert converted == expected
    assert [type(item) for item in converted] == [type(item) for item in expected]


@pytest.mark.parametrize(
    ("function", "value"),
    [("toInteger", "{}"), ("toFloat", "true"), ("toBoolean", "1.0"), ("toString", "[]")],
)
def test_conversion_of_unsupported_types_is_a_runtime_error(
    tmp_path: Path, function: str, value: str
) -> None:
    with Database(tmp_path / "db").read_transaction() as session:
        with pytest.raises(QueryError) as caught:
            session.query(f"RETURN [x IN [null,{value}] | {function}(x)]")
    assert (caught.value.category, caught.value.phase, caught.value.detail) == (
        "TypeError",
        "runtime",
        "InvalidArgumentValue",
    )


@pytest.mark.parametrize(
    "call", ["toInteger()", "toFloat(1,2)", "toBoolean()", "toString(1,2)", "sign()"]
)
def test_conversion_wrong_arity_is_a_compile_error(tmp_path: Path, call: str) -> None:
    with Database(tmp_path / "db").read_transaction() as session:
        with pytest.raises(QueryError) as caught:
            session.query(f"RETURN {call}")
    assert (caught.value.phase, caught.value.detail) == ("compile", "InvalidNumberOfArguments")


def test_conversions_are_used_in_writes_and_matching(tmp_path: Path) -> None:
    with Database(tmp_path / "db").write_transaction() as session:
        session.query("CREATE (:N {input:'42', parsed:toInteger('42')})")
        assert session.query(
            "MATCH (n:N {parsed:toInteger(n.input)}) "
            "SET n.text=toString(n.parsed) RETURN n.text AS text"
        ).rows == ({"text": "42"},)


@pytest.mark.parametrize("text", ["9223372036854775808", "-9223372036854775808.9", "1e999999"])
def test_to_integer_rejects_out_of_range_strings(tmp_path: Path, text: str) -> None:
    with Database(tmp_path / "db").read_transaction() as session:
        with pytest.raises(QueryError) as caught:
            session.query("RETURN toInteger($value) AS n", {"value": text})
    assert (caught.value.category, caught.value.detail) == ("TypeError", "NumberOutOfRange")


def test_numeric_formats_and_int64_boundaries(tmp_path: Path) -> None:
    with Database(tmp_path / "db").read_transaction() as session:
        assert session.query(
            "RETURN [x IN $ints | toInteger(x)] AS ints, [x IN $floats | toFloat(x)] AS floats, "
            "[x IN $strings | toString(x)] AS strings",
            {
                "ints": ["9223372036854775807", "0x10", "0o10", "1_000", "1__000", 1e40, -1e40],
                "floats": ["0x1.fp2", "1f", "1__000", "١٢"],
                "strings": [1e7, 1e-4, 1e-3, -0.0],
            },
        ).rows == (
            {
                "ints": [2**63 - 1, 16, 8, 1000, None, 2**63 - 1, -(2**63)],
                "floats": [7.75, 1.0, None, None],
                "strings": ["1.0E7", "1.0E-4", "0.001", "-0.0"],
            },
        )


def test_invalid_conversion_rolls_back_earlier_writes(tmp_path: Path) -> None:
    with Database(tmp_path / "db").write_transaction() as session:
        with pytest.raises(QueryError, match="toInteger"):
            session.query("CREATE (:Scratch) WITH toInteger([]) AS n CREATE (:N {n:n})")
        assert session.query("MATCH (n) RETURN count(n) AS n").rows == ({"n": 0},)
