"""Registered procedures execute through the normal Cypher compiler and runtime."""

from pathlib import Path
from typing import Any

import pytest

from grapharrow.app import build_application
from grapharrow.core.errors import QueryError
from grapharrow.use_cases.cypher_procedures import ProcedureField, ReadProcedure


def test_read_procedure_binds_implicit_parameters_and_yield_aliases(tmp_path: Path) -> None:
    query_api = build_application(tmp_path / "graph").use_cases.cypher
    query_api.register_read_procedure(
        "sample.echo",
        ReadProcedure(
            (ProcedureField("input", "FLOAT", nullable=False),),
            (ProcedureField("value", "FLOAT"), ProcedureField("label", "STRING")),
            lambda args: [{"value": args[0], "label": "echo"}],
        ),
    )

    result = query_api.execute("CALL sample.echo", {"input": 4})
    assert result.columns == ("value", "label")
    assert result.rows == ({"value": 4.0, "label": "echo"},)
    assert type(result.rows[0]["value"]) is float

    result = query_api.execute(
        "UNWIND [1,2,3] AS x CALL sample.echo(x) "
        "YIELD value AS result WHERE result > 1.0 RETURN x,result ORDER BY x"
    )
    assert result.rows == ({"x": 2, "result": 2.0}, {"x": 3, "result": 3.0})


def test_void_procedure_runs_per_input_and_preserves_rows(tmp_path: Path) -> None:
    query_api = build_application(tmp_path / "graph").use_cases.cypher
    seen = []

    def record(args: tuple[Any, ...]) -> tuple[()]:
        seen.append(args[0])
        return ()

    query_api.register_read_procedure(
        "sample.record", ReadProcedure((ProcedureField("value", "INTEGER"),), (), record)
    )
    assert query_api.execute("CALL sample.record(0)").rows == ()
    assert query_api.execute(
        "UNWIND [1,2] AS value CALL sample.record(value) RETURN value"
    ).rows == ({"value": 1}, {"value": 2})
    assert seen == [0, 1, 2]


@pytest.mark.parametrize(
    ("statement", "detail"),
    [
        ("CALL sample.missing()", "ProcedureNotFound"),
        ("CALL sample.echo()", "InvalidNumberOfArguments"),
        ("CALL sample.echo(true)", "InvalidArgumentType"),
        ("CALL sample.echo", "MissingParameter"),
        ("CALL sample.echo YIELD value RETURN value", "InvalidArgumentPassingMode"),
        (
            "WITH 1 AS value CALL sample.echo(1) YIELD value RETURN value",
            "VariableAlreadyBound",
        ),
    ],
)
def test_procedure_compile_errors_precede_execution(
    tmp_path: Path, statement: str, detail: str
) -> None:
    query_api = build_application(tmp_path / "graph").use_cases.cypher

    def unexpected(_: tuple[Any, ...]) -> tuple[()]:
        pytest.fail("An invalid statement should be rejected before running the handler")

    query_api.register_read_procedure(
        "sample.echo",
        ReadProcedure(
            (ProcedureField("input", "INTEGER"),),
            (ProcedureField("value", "INTEGER"),),
            unexpected,
        ),
    )
    with pytest.raises(QueryError) as error:
        query_api.execute(statement)
    assert (error.value.phase, error.value.detail) == ("compile", detail)


def test_read_procedure_is_available_inside_an_updating_pipeline(tmp_path: Path) -> None:
    query_api = build_application(tmp_path / "graph").use_cases.cypher
    query_api.register_read_procedure(
        "sample.value",
        ReadProcedure((), (ProcedureField("value", "INTEGER"),), lambda _: [{"value": 7}]),
    )
    assert query_api.execute(
        "CALL sample.value() YIELD value CREATE (:Probe {value:value}) RETURN value"
    ).rows == ({"value": 7},)
    assert query_api.execute("MATCH (n:Probe) RETURN n.value AS value").rows == ({"value": 7},)
