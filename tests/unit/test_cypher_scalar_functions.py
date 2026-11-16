"""Regression tests for scalar Cypher functions."""

import math
from pathlib import Path

import pytest

from grapharrow import Database
from grapharrow.core.errors import QueryError


@pytest.mark.parametrize(
    ("query", "function"),
    [
        ("MATCH (a) RETURN foo(a)", "foo"),
        ("MATCH (n) WHERE Missing(n) RETURN n", "Missing"),
        ("RETURN CASE WHEN true THEN 1 ELSE missing() END", "missing"),
        ("RETURN math.missing(1)", "math.missing"),
    ],
)
def test_unknown_function_reports_a_compile_time_syntax_error(
    tmp_path: Path, query: str, function: str
) -> None:
    with (
        Database(tmp_path / "db").read_transaction() as session,
        pytest.raises(QueryError) as caught,
    ):
        session.query(query)
    assert (caught.value.category, caught.value.phase, caught.value.detail) == (
        "SyntaxError",
        "compile",
        "UnknownFunction",
    )
    assert str(caught.value) == f"Unknown function: {function}"


@pytest.mark.parametrize("call", ["floor(1.2)", "stDev(1)"])
def test_unimplemented_builtins_report_unsupported_feature(tmp_path: Path, call: str) -> None:
    with (
        Database(tmp_path / "db").read_transaction() as session,
        pytest.raises(QueryError) as caught,
    ):
        session.query(f"RETURN {call}")
    assert (caught.value.category, caught.value.phase, caught.value.detail) == (
        "UnsupportedFeature",
        "compile",
        "UnsupportedFeature",
    )


def test_unknown_function_rejects_the_statement_before_writing(tmp_path: Path) -> None:
    with Database(tmp_path / "db").write_transaction() as session:
        with pytest.raises(QueryError) as caught:
            session.query("CREATE (:Scratch {value:missing(1)})")
        assert (caught.value.phase, caught.value.detail) == ("compile", "UnknownFunction")
        assert session.query("MATCH (n:Scratch) RETURN n").rows == ()


def test_function_resolution_is_case_insensitive_for_builtins(tmp_path: Path) -> None:
    with Database(tmp_path / "db").read_transaction() as session:
        assert session.query("RETURN ToUpper('abc') AS text, SqRt(4) AS value").rows == (
            {"text": "ABC", "value": 2.0},
        )


def test_relationship_endpoints_use_stored_direction_and_the_write_overlay(tmp_path: Path) -> None:
    db = Database(tmp_path / "db")
    with db.write_transaction() as session:
        assert session.query(
            "CREATE (a:A {name:'source'}), (b:B {name:'target'}) "
            "MERGE (a)-[r:R]-(b) RETURN startNode(r).name AS s, endNode(r).name AS t"
        ).rows == ({"s": "source", "t": "target"},)
        assert session.query(
            "MATCH (a:A)-[r:R]->(:B) SET a.name = 'updated' RETURN startNode(r).name AS s"
        ).rows == ({"s": "updated"},)
    with db.read_transaction() as session:
        assert session.query(
            "MATCH (:B)<-[r:R]-(:A) RETURN startNode(r).name AS s, endNode(r).name AS t"
        ).rows == ({"s": "updated", "t": "target"},)
        assert session.query(
            "OPTIONAL MATCH ()-[r:Missing]->() RETURN startNode(r) AS s, endNode(null) AS t"
        ).rows == ({"s": None, "t": None},)


def test_relationship_endpoints_require_a_relationship(tmp_path: Path) -> None:
    with (
        Database(tmp_path / "db").read_transaction() as session,
        pytest.raises(QueryError) as caught,
    ):
        session.query("RETURN startNode(1)")
    assert (caught.value.phase, caught.value.detail) == ("compile", "InvalidArgumentType")


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


def test_labels_and_type_propagate_literal_parameter_and_optional_nulls(tmp_path: Path) -> None:
    with Database(tmp_path / "db").read_transaction() as session:
        result = session.query(
            "OPTIONAL MATCH (n:Missing) OPTIONAL MATCH ()-[r:Missing]->() "
            "RETURN labels(n), type(r), labels(null), type(NULL), "
            "labels($value) AS paramLabels, type($value) AS paramType",
            {"value": None},
        )
    assert result.columns == (
        "labels(n)",
        "type(r)",
        "labels(null)",
        "type(NULL)",
        "paramLabels",
        "paramType",
    )
    assert result.rows == (dict.fromkeys(result.columns),)


@pytest.mark.parametrize(
    ("function", "pattern"), [("labels", "(x:Missing)"), ("type", "()-[x:Missing]->()")]
)
def test_labels_and_type_handle_literal_null_in_optional_match(
    tmp_path: Path, function: str, pattern: str
) -> None:
    with Database(tmp_path / "db").read_transaction() as session:
        result = session.query(f"OPTIONAL MATCH {pattern} RETURN {function}(x), {function}(null)")
    assert result.rows == ({f"{function}(x)": None, f"{function}(null)": None},)


@pytest.mark.parametrize("function", ["labels", "type"])
@pytest.mark.parametrize("value", [1, "text", []])
def test_labels_and_type_report_invalid_runtime_parameter_values(
    tmp_path: Path, function: str, value: object
) -> None:
    with Database(tmp_path / "db").read_transaction() as session, pytest.raises(QueryError) as err:
        session.query(f"RETURN {function}($value)", {"value": value})
    assert (err.value.category, err.value.phase, err.value.detail) == (
        "TypeError",
        "runtime",
        "InvalidArgumentValue",
    )


@pytest.mark.parametrize("function", ["labels", "type"])
def test_labels_and_type_report_invalid_computed_values(tmp_path: Path, function: str) -> None:
    with Database(tmp_path / "db").read_transaction() as session, pytest.raises(QueryError) as err:
        session.query(f"RETURN {function}([null, 1][1])")
    assert (err.value.category, err.value.phase, err.value.detail) == (
        "TypeError",
        "runtime",
        "InvalidArgumentValue",
    )


@pytest.mark.parametrize(
    "query",
    [
        "MATCH (n) RETURN type(n)",
        "MATCH ()-[r]->() RETURN labels(r)",
        "RETURN labels(1)",
        "RETURN type([])",
    ],
)
def test_labels_and_type_reject_known_wrong_types_at_compile_time(
    tmp_path: Path, query: str
) -> None:
    with Database(tmp_path / "db").read_transaction() as session, pytest.raises(QueryError) as err:
        session.query(query)
    assert (err.value.category, err.value.phase, err.value.detail) == (
        "SyntaxError",
        "compile",
        "InvalidArgumentType",
    )


def test_labels_and_type_read_graph_bindings_and_optional_relationships(tmp_path: Path) -> None:
    db = Database(tmp_path / "db")
    with db.write_transaction() as session:
        session.query("CREATE (:A {name:'first'})-[:R]->(:B {name:'second'})")
    with db.read_transaction() as session:
        result = session.query(
            "MATCH (n) OPTIONAL MATCH (n)-[r:R]->() "
            "RETURN n.name AS name, labels(n) AS nodeLabels, type(r) AS relType ORDER BY name"
        )
    assert result.rows == (
        {"name": "first", "nodeLabels": ["A"], "relType": "R"},
        {"name": "second", "nodeLabels": ["B"], "relType": None},
    )


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
