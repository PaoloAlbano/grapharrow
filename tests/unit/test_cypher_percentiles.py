"""Regression tests for Cypher percentile aggregation."""

from pathlib import Path

import pytest

from grapharrow import Database
from grapharrow.core.errors import QueryError


@pytest.fixture
def database(tmp_path: Path) -> Database:
    return Database(tmp_path / "db")


def test_percentiles_interpolate_or_select_a_sample(database: Database) -> None:
    with database.read_transaction() as session:
        row = session.query(
            "UNWIND [100, null, 10, 20, 10] AS n "
            "RETURN PeRcEnTiLeCoNt(n, 0.5) AS continuous, percentileDisc(n, 0.5) AS discrete, "
            "percentileCont(DISTINCT n, 0.5) AS distinctCont, "
            "percentileDisc(DISTINCT n, 0.5) AS distinctDisc"
        ).rows[0]
    assert row == {"continuous": 15.0, "discrete": 10, "distinctCont": 20.0, "distinctDisc": 20}
    assert type(row["continuous"]) is float
    assert type(row["distinctCont"]) is float
    assert type(row["discrete"]) is int
    assert type(row["distinctDisc"]) is int


def test_percentiles_use_the_first_contributing_row_of_each_group(database: Database) -> None:
    with database.read_transaction() as session:
        rows = session.query(
            "UNWIND [{g:'a', x:null, p:2}, {g:'a', x:10, p:0}, {g:'a', x:20, p:1}, "
            "{g:'b', x:10, p:1}, {g:'b', x:20, p:0}] AS item "
            "WITH item ORDER BY item.g, item.x IS NOT NULL, item.x "
            "RETURN item.g AS name, percentileDisc(item.x, item.p) AS ranked, "
            "percentileCont(item.x, item.p) AS interpolated ORDER BY name"
        ).rows
    assert rows == (
        {"name": "a", "ranked": 10, "interpolated": 10.0},
        {"name": "b", "ranked": 20, "interpolated": 20.0},
    )


def test_discrete_percentile_preserves_numeric_types_at_endpoints(database: Database) -> None:
    with database.read_transaction() as session:
        row = session.query(
            "UNWIND [10, 10.0] AS n "
            "RETURN percentileDisc(n, 0) AS low, percentileDisc(n, 1) AS high"
        ).rows[0]
    assert type(row["low"]) is int
    assert type(row["high"]) is float


@pytest.mark.parametrize("samples", ["[]", "[null, null]"])
def test_percentiles_return_null_without_contributing_samples(
    database: Database, samples: str
) -> None:
    with database.read_transaction() as session:
        rows = session.query(
            f"UNWIND {samples} AS n "
            "RETURN percentileCont(n, 0.5) AS continuous, percentileDisc(n, 0.5) AS discrete"
        ).rows
    assert rows == ({"continuous": None, "discrete": None},)


@pytest.mark.parametrize(
    ("percentile", "category", "detail"),
    [
        (float("nan"), "ArgumentError", "NumberOutOfRange"),
        (float("inf"), "ArgumentError", "NumberOutOfRange"),
        (-0.01, "ArgumentError", "NumberOutOfRange"),
        (1.01, "ArgumentError", "NumberOutOfRange"),
        ("0.5", "TypeError", "InvalidArgumentType"),
        (None, "TypeError", "InvalidArgumentType"),
        (True, "TypeError", "InvalidArgumentType"),
    ],
)
def test_invalid_percentile_is_a_runtime_error(
    database: Database, percentile: object, category: str, detail: str
) -> None:
    with database.read_transaction() as session, pytest.raises(QueryError) as caught:
        session.query("RETURN percentileDisc(10, $p) AS value", {"p": percentile})
    assert (caught.value.category, caught.value.phase, caught.value.detail) == (
        category,
        "runtime",
        detail,
    )


@pytest.mark.parametrize(
    ("aggregate", "detail"),
    [
        ("percentileCont(1)", "InvalidNumberOfArguments"),
        ("percentileDisc(1, 0.5, 2)", "InvalidNumberOfArguments"),
        ("percentileCont(count(*), 0.5)", "NestedAggregation"),
        ("percentileCont(false, 0.5)", "InvalidArgumentType"),
    ],
)
def test_invalid_percentile_invocations_fail_at_compile_time(
    database: Database, aggregate: str, detail: str
) -> None:
    with database.read_transaction() as session, pytest.raises(QueryError) as caught:
        session.query(f"RETURN {aggregate} AS value")
    assert (caught.value.phase, caught.value.detail) == ("compile", detail)


def test_invalid_sample_after_a_number_fails_at_runtime(database: Database) -> None:
    with database.read_transaction() as session, pytest.raises(QueryError) as caught:
        session.query("UNWIND [10, '20'] AS n RETURN percentileCont(n, 0.5) AS value")
    assert (caught.value.phase, caught.value.detail) == ("runtime", "InvalidArgumentType")


def test_percentile_range_failure_rolls_back_the_statement(database: Database) -> None:
    with database.write_transaction() as session:
        with pytest.raises(QueryError) as caught:
            session.query("CREATE (n:Sample {x:10}) RETURN percentileDisc(n.x, 2) AS p")
        assert (caught.value.phase, caught.value.detail) == ("runtime", "NumberOutOfRange")
        assert session.query("MATCH (n:Sample) RETURN count(n) AS total").rows == ({"total": 0},)
