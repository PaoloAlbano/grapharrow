"""Regression tests for the Cypher range() list function."""

from pathlib import Path

import pytest

from grapharrow import Database
from grapharrow.core.errors import QueryError


@pytest.mark.parametrize(
    ("arguments", "expected"),
    [
        ("-2, 2", [-2, -1, 0, 1, 2]),
        ("1, 6, 2", [1, 3, 5]),
        ("5, -1, -2", [5, 3, 1, -1]),
        ("3, 3, -1", [3]),
        ("3, 1", []),
        ("1, 3, -1", []),
        ("9223372036854775806, 9223372036854775807", [9223372036854775806, 9223372036854775807]),
        (
            "-9223372036854775807, -9223372036854775808, -1",
            [-9223372036854775807, -9223372036854775808],
        ),
    ],
)
def test_range_returns_an_inclusive_list(
    tmp_path: Path, arguments: str, expected: list[int]
) -> None:
    with Database(tmp_path / "db").read_transaction() as session:
        assert session.query(f"RETURN rAnGe({arguments}) AS xs").rows == ({"xs": expected},)


@pytest.mark.parametrize(
    ("arguments", "phase", "category", "detail"),
    [
        ("", "compile", "SyntaxError", "InvalidNumberOfArguments"),
        ("1", "compile", "SyntaxError", "InvalidNumberOfArguments"),
        ("1, 2, 3, 4", "compile", "SyntaxError", "InvalidNumberOfArguments"),
        ("DISTINCT 1, 3", "compile", "SyntaxError", "InvalidArgumentPassingMode"),
        ("1, 2, 0", "runtime", "ArgumentError", "NumberOutOfRange"),
        ("NULL, 1, 0", "runtime", "ArgumentError", "NumberOutOfRange"),
        ("NULL, 1", "runtime", "TypeError", "InvalidArgumentType"),
        ("0, NULL, 1", "runtime", "TypeError", "InvalidArgumentType"),
        ("0, 1, NULL", "runtime", "TypeError", "InvalidArgumentType"),
        ("false, 2", "runtime", "ArgumentError", "InvalidArgumentType"),
        ("0, 1.0", "runtime", "ArgumentError", "InvalidArgumentType"),
        ("0, 1, '1'", "runtime", "ArgumentError", "InvalidArgumentType"),
        ("0, 1, []", "runtime", "ArgumentError", "InvalidArgumentType"),
        ("0, 9223372036854775807+1", "runtime", "ArgumentError", "NumberOutOfRange"),
        ("0, 9223372036854775807", "runtime", "UnsupportedFeature", "RangeTooLarge"),
        ("0, 1048576", "runtime", "UnsupportedFeature", "RangeTooLarge"),
    ],
)
def test_range_errors_have_structured_diagnostics(
    tmp_path: Path, arguments: str, phase: str, category: str, detail: str
) -> None:
    with Database(tmp_path / "db").read_transaction() as session:
        with pytest.raises(QueryError) as caught:
            session.query(f"RETURN range({arguments})")
    assert (caught.value.phase, caught.value.category, caught.value.detail) == (
        phase,
        category,
        detail,
    )


def test_range_composes_with_unwind_projections_and_writes(tmp_path: Path) -> None:
    with Database(tmp_path / "db").write_transaction() as session:
        rows = session.query(
            "UNWIND range($lo, $hi) AS x CREATE (n:Range {x:x, xs:range(0,x)}) "
            "WITH n WHERE n.x IN range(2,3) RETURN n.x AS x,n.xs[-1] AS last ORDER BY x",
            {"lo": 1, "hi": 3},
        ).rows
        assert rows == ({"x": 2, "last": 2}, {"x": 3, "last": 3})
        assert session.query("MATCH (n:Range) RETURN count(n) AS n").rows == ({"n": 3},)


def test_range_failure_rolls_back_earlier_writes(tmp_path: Path) -> None:
    db = Database(tmp_path / "db")
    with db.write_transaction() as session:
        with pytest.raises(QueryError, match="nonzero"):
            session.query(
                "CREATE (:BeforeRange) WITH 0 AS step UNWIND range(1,2,step) AS x CREATE (:N {x:x})"
            )
        assert session.query("MATCH (n) RETURN count(n) AS n").rows == ({"n": 0},)


def test_range_filters_inline_node_and_relationship_properties(tmp_path: Path) -> None:
    with Database(tmp_path / "db").write_transaction() as session:
        session.query(
            "CREATE (a:N {lo:1, hi:2, xs:[1,2]})-[:R {xs:[1,2]}]->(:M), (a)-[:R {xs:[1,2,3]}]->(:M)"
        )
        assert session.query(
            "MATCH (n:N {xs:range(n.lo,n.hi)})-[r:R {xs:range(n.lo,n.hi)}]->(:M) "
            "RETURN count(r) AS n"
        ).rows == ({"n": 1},)


def test_range_accepts_the_maximum_supported_list_length(tmp_path: Path) -> None:
    with Database(tmp_path / "db").read_transaction() as session:
        assert session.query("RETURN size(range(1,1048576)) AS n").rows == ({"n": 1048576},)
