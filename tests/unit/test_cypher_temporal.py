"""Regression tests for temporal Cypher expressions and persisted properties."""

from pathlib import Path
from typing import Any

import pytest

from grapharrow import Database
from grapharrow.core.errors import QueryError


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("date('2020-W01-1')", "2019-12-30"),
        ("date({year:2024, ordinalDay:60})", "2024-02-29"),
        ("date({year:2024, quarter:2, dayOfQuarter:2})", "2024-04-02"),
        ("date('-0001-12-31') + duration('P1D')", "0000-01-01"),
        ("date({year:10000,month:2,day:29})", "+10000-02-29"),
        (
            "duration.between(date('-999999999-01-01'), date('+999999999-12-31'))",
            "P1999999998Y11M30D",
        ),
        (
            "duration.inSeconds(localdatetime('-999999999-01-01'), "
            "localdatetime('+999999999-12-31T23:59:59'))",
            "PT17531639991215H59M59S",
        ),
        ("localtime('123456.123456789')", "12:34:56.123456789"),
        ("time({hour:12, timezone:'-02:05:07'})", "12:00-02:05:07"),
        ("localdatetime('2024-060T12:34')", "2024-02-29T12:34"),
        ("datetime.fromEpoch(-1,999999999)", "1969-12-31T23:59:59.999999999Z"),
        ("datetime.fromEpochMillis(-1).epochMillis", -1),
        ("datetime('2023-10-29T02:30+01:00[Europe/Rome]')", "2023-10-29T02:30+01:00[Europe/Rome]"),
        ("duration('P0.75M')", "P22DT19H51M49.5S"),
        ("duration({seconds:-0.1}).nanosecondsOfSecond", 900_000_000),
        ("date('2024-01-31') + duration('P1M')", "2024-02-29"),
        ("date('2024-03-01') - duration('PT23H')", "2024-03-01"),
        (
            "datetime('2024-03-30T12:00[Europe/Rome]') + duration('P1D')",
            "2024-03-31T12:00+02:00[Europe/Rome]",
        ),
        (
            "datetime('2024-03-30T12:00[Europe/Rome]') + duration('PT24H')",
            "2024-03-31T13:00+02:00[Europe/Rome]",
        ),
        ("duration('P1M') + duration('P2M')", "P3M"),
        ("duration('PT1S') / 2", "PT0.5S"),
        ("duration.inSeconds(date('2024-02-28'), date('2024-03-01'))", "PT48H"),
        ("duration.inDays(date('2024-02-28'), date('2024-03-01'))", "P2D"),
        ("duration.inMonths(date('2024-01-31'), date('2024-02-29'))", "PT0S"),
        ("duration.between(date('2024-01-01'), date('2025-04-05'))", "P1Y3M4D"),
        ("date.truncate('weekYear', date('2021-01-01'))", "2019-12-30"),
        (
            "datetime.truncate('day', datetime('2024-03-31T14:37Z'), {timezone:'Europe/Rome'})",
            "2024-03-31T00:00+01:00[Europe/Rome]",
        ),
        (
            "localtime.truncate('microsecond', localtime('12:34:56.123456789'), {nanosecond:2})",
            "12:34:56.123456002",
        ),
        ("localdatetime({date:date('2024-02-29'), time:localtime('12:34')}).ordinalDay", 60),
        ("time('10:00+02:00') < time('09:00Z')", True),
        ("datetime('2024-01-02T00:30+01:00') < datetime('2024-01-01T23:45Z')", True),
        ("date('2024-01-01') = '2024-01-01'", False),
        ("date('2024-01-01') < localdatetime('2024-01-01T00:00')", None),
        ("duration('P1D') = duration('PT24H')", False),
        ("duration.inDays(null, date('2024-01-01'))", None),
        ("toString(time('12:34:56.123456789+0200'))", "12:34:56.123456789+02:00"),
    ],
)
def test_temporal_cypher_expressions(tmp_path: Path, expression: str, expected: Any) -> None:
    with Database(tmp_path / "db").read_transaction() as session:
        assert session.query(f"RETURN {expression} AS result").rows == ({"result": expected},)


@pytest.mark.parametrize(
    "statement",
    [
        "RETURN date('2023-02-29')",
        "RETURN localtime('27:00')",
        "RETURN time('12:00+19:00')",
        "RETURN datetime('2023-10-29T02:30+03:00[Europe/Rome]')",
        "RETURN datetime({timezone:'Invalid/Zone'})",
        "RETURN duration('P')",
        "RETURN date({year:2024, ordinalDay:0})",
        "RETURN date({year:2024, quarter:1, dayOfQuarter:367})",
        "RETURN date({year:2024, unexpected:1})",
        "RETURN duration.between(date(), false)",
        "RETURN datetime.fromEpoch(0)",
    ],
)
def test_invalid_temporal_input_is_a_query_error(tmp_path: Path, statement: str) -> None:
    with Database(tmp_path / "db").read_transaction() as session, pytest.raises(QueryError) as err:
        session.query(statement)
    assert err.value.phase in {"compile", "runtime"}


def test_temporal_properties_preserve_types_after_reopen(tmp_path: Path) -> None:
    path = tmp_path / "db"
    with Database(path).write_transaction() as session:
        session.query("CREATE (:Event {v:'2024-01-02', name:'text'})")
        session.query(
            "CREATE (n:Event {v:date('2024-01-02'), name:'date', "
            "ts:[localtime('12:34:56.000000001'), localtime('12:34:56.000000002')], "
            "literal:['T1:[]']})"
            "-[:LASTS {v:duration('PT1.000000001S')}]->(:End)"
        )
        assert session.query(
            "MATCH (n:Event {name:'date'}) SET n.updated=date('2024-02-29') "
            "RETURN properties(n).updated.day AS day"
        ).rows == ({"day": 29},)

    with Database(path).read_transaction() as session:
        assert session.query(
            "MATCH (n:Event) WHERE n.v='2024-01-02' RETURN n.name AS name"
        ).rows == ({"name": "text"},)
        assert session.query(
            "MATCH (n:Event {v:date('2024-01-02')}) "
            "RETURN size(n.ts) AS count, last(n.ts).nanosecond AS nanos, n.literal AS literal"
        ).rows == ({"count": 2, "nanos": 2, "literal": ["T1:[]"]},)
        assert session.query("MATCH ()-[r:LASTS]->() RETURN r.v AS value").rows == (
            {"value": "PT1.000000001S"},
        )
        assert session.query("MATCH (n:Event {name:'date'}) RETURN n.updated AS date").rows == (
            {"date": "2024-02-29"},
        )


def test_temporal_values_survive_failed_statement(tmp_path: Path) -> None:
    with Database(tmp_path / "db").write_transaction() as session:
        session.query("CREATE (:Event {d:date('2024-01-02')})")
        with pytest.raises(QueryError):
            session.query("MATCH (n:Event) SET n.d=date('2024-02-29') RETURN date('invalid')")
        assert session.query("MATCH (n:Event) RETURN n.d AS d").rows == ({"d": "2024-01-02"},)


def test_order_by_persisted_zoned_datetime_uses_the_instant(tmp_path: Path) -> None:
    path = tmp_path / "db"
    with Database(path).write_transaction() as session:
        session.query(
            "CREATE (:Event {t:datetime('2024-01-01T00:30Z'), name:'later'}), "
            "(:Event {t:datetime('2024-01-01T01:00+01:00'), name:'earlier'})"
        )
    with Database(path).read_transaction() as session:
        assert session.query("MATCH (n:Event) RETURN n.name AS name ORDER BY n.t LIMIT 1").rows == (
            {"name": "earlier"},
        )


def test_statement_time_is_shared_by_rows_and_expressions(tmp_path: Path) -> None:
    with Database(tmp_path / "db").read_transaction() as session:
        assert session.query(
            "UNWIND [1,2,3] AS n WITH datetime.statement() AS t "
            "RETURN count(DISTINCT t) AS instants, t = datetime.statement() AS same"
        ).rows == ({"instants": 1, "same": True},)
