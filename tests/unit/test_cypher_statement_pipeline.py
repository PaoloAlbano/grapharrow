"""Regression tests for structured errors and Cypher statement atomicity."""

from pathlib import Path

import pytest

from grapharrow import Database
from grapharrow.core.errors import QueryError


def test_syntax_error_reports_compile_phase_and_position(tmp_path: Path) -> None:
    with Database(tmp_path / "db").read_transaction() as session:
        with pytest.raises(QueryError) as caught:
            session.query("RETURN )")
    error = caught.value
    assert (error.category, error.phase, error.detail) == (
        "SyntaxError",
        "compile",
        "UnexpectedSyntax",
    )
    assert error.line == 1
    assert error.column == 7


@pytest.mark.parametrize(
    ("expression", "detail", "column"),
    [
        (r"'\uH'", "InvalidUnicodeLiteral", 7),
        (r'"é\U01"', "InvalidUnicodeLiteral", 7),
        (r"'\u00", "InvalidUnicodeLiteral", 7),
        (r"'\\\uH'", "InvalidUnicodeLiteral", 7),
        (r"'\u0041\q'", "UnexpectedSyntax", 7),
        (r"'\\uH", "UnexpectedSyntax", 7),
        ("42 — 41", "InvalidUnicodeCharacter", 10),
        ("42 − 41", "InvalidUnicodeCharacter", 10),
        ("42 × 41", "InvalidUnicodeCharacter", 10),
        ("42 @ 41", "UnexpectedSyntax", 10),
    ],
)
def test_unicode_syntax_errors_report_the_offending_token(
    tmp_path: Path, expression: str, detail: str, column: int
) -> None:
    with (
        Database(tmp_path / "db").read_transaction() as session,
        pytest.raises(QueryError) as caught,
    ):
        session.query(f"// First line\nRETURN {expression}")
    error = caught.value
    assert (error.category, error.phase, error.detail) == ("SyntaxError", "compile", detail)
    assert (error.line, error.column) == (2, column)


def test_unicode_escape_above_the_code_point_range(tmp_path: Path) -> None:
    with (
        Database(tmp_path / "db").read_transaction() as session,
        pytest.raises(QueryError) as caught,
    ):
        session.query(r"RETURN '\U00110000' AS value")
    assert (caught.value.category, caught.value.phase, caught.value.detail) == (
        "SyntaxError",
        "compile",
        "InvalidUnicodeLiteral",
    )


def test_unicode_strings_identifiers_and_comments(tmp_path: Path) -> None:
    with Database(tmp_path / "db").read_transaction() as session:
        result = session.query(
            r"WITH '\u01FF \U0001F600 — \\uH' AS café // — \uH"
            "\n"
            "RETURN café AS `résultat—`, 42 - 41 AS diff"
        )
    assert result.rows == ({"résultat—": "ǿ 😀 — \\uH", "diff": 1},)


def test_unicode_dashes_are_allowed_in_relationship_patterns(tmp_path: Path) -> None:
    db = Database(tmp_path / "db")
    with db.write_transaction() as session:
        session.query("CREATE (:Start)-[:R]->(:End)")
    with db.read_transaction() as session:
        assert session.query("MATCH (:Start)—[:R]—>(:End) RETURN count(*) AS n").rows == ({"n": 1},)


@pytest.mark.parametrize(
    "marked_relationship",
    [
        "-[:R^..]->",
        "-[r:`REL TYPE`|:OTHER ^..2]->",
        "-[:R*^-2]->",
        "<-[:R*1.. ^-2]-",
    ],
)
def test_malformed_relationship_ranges_report_the_offending_position(
    tmp_path: Path, marked_relationship: str
) -> None:
    pattern = f"MATCH (n){marked_relationship}(m) RETURN m"
    with (
        Database(tmp_path / "db").read_transaction() as session,
        pytest.raises(QueryError) as caught,
    ):
        session.query(f"MATCH (n:A)\n{pattern.replace('^', '')}")
    error = caught.value
    assert (error.category, error.phase, error.detail) == (
        "SyntaxError",
        "compile",
        "InvalidRelationshipPattern",
    )
    assert (error.line, error.column) == (2, pattern.index("^"))


@pytest.mark.parametrize(
    "query",
    [
        "RETURN [1, ..2] AS values",
        "RETURN [1]-[x..2]-(3)",
        "MATCH (n) WHERE (n.age)-[x..2] > 1 RETURN n",
        "MATCH ()-[:R {p:[1, ..2]}]->() RETURN 1",
    ],
)
def test_list_syntax_errors_keep_their_diagnostic(tmp_path: Path, query: str) -> None:
    with (
        Database(tmp_path / "db").read_transaction() as session,
        pytest.raises(QueryError) as error,
    ):
        session.query(query)
    assert error.value.detail == "UnexpectedSyntax"


@pytest.mark.parametrize(
    "pattern",
    ["()-[r*]-()-[]-(r)", "(r)-[]-()-[r*]-()", "()-[]-(r)-[r*]-()"],
)
def test_binding_conflicts_take_precedence_over_unbounded_expansion(
    tmp_path: Path, pattern: str
) -> None:
    with (
        Database(tmp_path / "db").read_transaction() as session,
        pytest.raises(QueryError) as caught,
    ):
        session.query(f"MATCH {pattern} RETURN r")
    assert (caught.value.category, caught.value.phase, caught.value.detail) == (
        "SyntaxError",
        "compile",
        "VariableTypeConflict",
    )


def test_traversal_bounds_preserve_supported_and_unsupported_paths(tmp_path: Path) -> None:
    db = Database(tmp_path / "db")
    with db.write_transaction() as session:
        session.query("CREATE (:A)-[:R]->(:B)")
    with db.read_transaction() as session:
        assert session.query("MATCH (:A)-[r:R*1..5]->(:B) RETURN size(r) AS hops").rows == (
            {"hops": 1},
        )
        with pytest.raises(QueryError, match="at most 5 hops"):
            session.query("MATCH (:A)-[r:R*1..6]->(:B) RETURN r")
        with pytest.raises(QueryError) as caught:
            session.query("MATCH (:A)-[r:R*]->(:B) RETURN r")
        assert (caught.value.category, caught.value.phase, caught.value.detail) == (
            "UnsupportedFeature",
            "compile",
            "UnboundedTraversal",
        )


@pytest.mark.parametrize(
    ("query", "params", "category", "phase"),
    [
        ("WITH 1 AS n RETURN n.value", {}, "TypeError", "compile"),
        ("WITH $n AS n RETURN n.value", {"n": 1}, "TypeError", "runtime"),
        ("WITH $n AS n RETURN n[0]", {"n": 1}, "TypeError", "runtime"),
        ("RETURN any(x IN [false] WHERE x % 2 = 0)", {}, "SyntaxError", "compile"),
        ("RETURN any(x IN $xs WHERE x % 2 = 0)", {"xs": [False]}, "TypeError", "runtime"),
        ("MATCH (n) WHERE (n) RETURN n", {}, "SyntaxError", "compile"),
        ("MATCH p = ()-[*]->() WHERE p.name = 'x' RETURN p", {}, "SyntaxError", "compile"),
        ("MATCH p = ()-[*]->() RETURN size(p)", {}, "SyntaxError", "compile"),
        ("RETURN 1 SKIP 1.5", {}, "SyntaxError", "compile"),
        ("RETURN 1 LIMIT $n", {"n": 1.5}, "SyntaxError", "runtime"),
        ("WITH 1 AS x RETURN x LIMIT 1.5", {}, "SyntaxError", "compile"),
        ("WITH 1 AS x RETURN x SKIP $n", {"n": 1.5}, "SyntaxError", "runtime"),
        ("MATCH (n) DELETE 1 + 1", {}, "SyntaxError", "compile"),
    ],
)
def test_invalid_argument_type_reports_category_and_phase(
    tmp_path: Path, query: str, params: dict[str, object], category: str, phase: str
) -> None:
    with (
        Database(tmp_path / "db").write_transaction() as session,
        pytest.raises(QueryError) as caught,
    ):
        session.query(query, params)
    assert (caught.value.category, caught.value.phase, caught.value.detail) == (
        category,
        phase,
        "InvalidArgumentType",
    )


@pytest.mark.parametrize(
    "query",
    [
        "MATCH (n) RETURN n SKIP n.count",
        "MATCH (n) RETURN n LIMIT n.count",
        "MATCH (n) RETURN n LIMIT $offset + n.count",
        "MATCH (n) RETURN n LIMIT {value:n.count}",
        "MATCH (n) WITH n.count AS size LIMIT size RETURN size",
        "MATCH (n) WITH n SKIP size([x IN [1] | n.count]) RETURN n",
    ],
)
def test_window_rejects_row_references_during_compilation(tmp_path: Path, query: str) -> None:
    with (
        Database(tmp_path / "db").read_transaction() as session,
        pytest.raises(QueryError) as caught,
    ):
        session.query(query, {"offset": 1})
    assert (caught.value.category, caught.value.phase, caught.value.detail) == (
        "SyntaxError",
        "compile",
        "NonConstantExpression",
    )


def test_window_accepts_parameters_and_local_comprehension_bindings(tmp_path: Path) -> None:
    with Database(tmp_path / "db").read_transaction() as session:
        result = session.query(
            "UNWIND [1,2,3,4,5] AS n RETURN n ORDER BY n "
            "SKIP $offset + 1 LIMIT size([n IN [1,2] | n])",
            {"offset": 1},
        )
    assert result.rows == ({"n": 3}, {"n": 4})


def test_nonconstant_window_is_rejected_before_writes(tmp_path: Path) -> None:
    with Database(tmp_path / "db").write_transaction() as session:
        with pytest.raises(QueryError) as caught:
            session.query("CREATE (n:Scratch {count:2}) RETURN n LIMIT n.count")
        assert (caught.value.category, caught.value.phase, caught.value.detail) == (
            "SyntaxError",
            "compile",
            "NonConstantExpression",
        )
        assert session.query("MATCH (n:Scratch) RETURN n").rows == ()


@pytest.mark.parametrize("parameters", [{}, {"props": {"name": "test"}}])
@pytest.mark.parametrize(
    "query",
    [
        "MATCH (n $props) RETURN n",
        "MATCH ()-[r:R $props]->() RETURN r",
        "OPTIONAL MATCH (n $props) WITH n RETURN n",
        "RETURN EXISTS { (:Node $props)-[:R]->() } AS found",
    ],
)
def test_matching_patterns_reject_property_map_parameters(
    tmp_path: Path, query: str, parameters: dict[str, object]
) -> None:
    with (
        Database(tmp_path / "db").read_transaction() as session,
        pytest.raises(QueryError) as caught,
    ):
        session.query(query, parameters)
    assert (caught.value.category, caught.value.phase, caught.value.detail) == (
        "SyntaxError",
        "compile",
        "InvalidParameterUse",
    )


@pytest.mark.parametrize("parameters", [{}, {"props": {"name": "test"}}])
@pytest.mark.parametrize(
    "query",
    [
        "MERGE (n:Scratch $props) RETURN n",
        "CREATE (a:Scratch), (b:Scratch) MERGE (a)-[r:R $props]->(b) RETURN r",
    ],
)
def test_merge_rejects_property_map_parameters_before_writing(
    tmp_path: Path, query: str, parameters: dict[str, object]
) -> None:
    with Database(tmp_path / "db").write_transaction() as session:
        with pytest.raises(QueryError) as caught:
            session.query(query, parameters)
        assert (caught.value.category, caught.value.phase, caught.value.detail) == (
            "SyntaxError",
            "compile",
            "InvalidParameterUse",
        )
        assert session.query("MATCH (n:Scratch) RETURN n").rows == ()


def test_create_accepts_property_map_parameters(tmp_path: Path) -> None:
    with Database(tmp_path / "db").write_transaction() as session:
        assert session.query(
            "CREATE (n:A $props) RETURN n.name AS name", {"props": {"name": "test"}}
        ).rows == ({"name": "test"},)
        assert session.query(
            "CREATE (a:A $props)-[r:R $edgeProps]->(:B) RETURN a.name AS name, r.weight AS weight",
            {"props": {"name": "other"}, "edgeProps": {"weight": 5}},
        ).rows == ({"name": "other", "weight": 5},)
        assert session.query(
            "MATCH (a:A {name:'test'}) CREATE (a)-[:R $edgeProps]->(b:B $props) "
            "RETURN b.name AS name",
            {"props": {"name": "target"}, "edgeProps": {"weight": 7}},
        ).rows == ({"name": "target"},)


def test_match_and_merge_accept_parameters_as_individual_property_values(tmp_path: Path) -> None:
    parameters = {"name": "test", "weight": 5}
    merge = "MERGE (a:A {name:$name}) MERGE (b:B {name:$name}) MERGE (a)-[:R {weight:$weight}]->(b)"
    with Database(tmp_path / "db").write_transaction() as session:
        session.query(merge, parameters)
        session.query(merge, parameters)
        rows = session.query(
            "MATCH (:A {name:$name})-[r:R {weight:$weight}]->(:B {name:$name}) "
            "RETURN r.weight AS weight",
            parameters,
        ).rows
    assert rows == ({"weight": 5},)


@pytest.mark.parametrize("tail", ["RETURN length(p)", "CREATE (:Unexpected)"])
def test_named_unbounded_traversal_has_no_executable_plan(tmp_path: Path, tail: str) -> None:
    db = Database(tmp_path / "db")
    graph = db._app.use_cases.graph
    before = graph.load_state()
    with db.write_transaction() as session, pytest.raises(QueryError) as caught:
        session.query(f"MATCH p = ()-[*]->() {tail}")
    assert caught.value.detail == "UnboundedTraversal"
    assert caught.value.phase == "compile"
    assert graph.load_state() == before


def test_failed_return_rolls_back_create(tmp_path: Path) -> None:
    db = Database(tmp_path / "db")
    graph = db._app.use_cases.graph
    before = graph.load_state()
    with db.write_transaction() as session:
        with pytest.raises(QueryError, match="[Dd]ivi"):
            session.query("CREATE (:Scratch) RETURN 1 / 0 AS value")
    assert graph.load_state() == before
    assert Database(tmp_path / "db")._app.use_cases.graph.load_state() == before


@pytest.mark.parametrize(
    "query",
    [
        "CREATE (n:First)-[:R]->(), (n:Second)-[:R]->()",
        "MATCH ()-[r]->() CREATE ()-[r]->()",
        "MATCH (a)-[r]->(b) MERGE (a)-[r]->(b)",
    ],
)
def test_write_rebinding_fails_during_compilation(tmp_path: Path, query: str) -> None:
    db = Database(tmp_path / "db")
    graph = db._app.use_cases.graph
    before = graph.load_state()
    with db.write_transaction() as session, pytest.raises(QueryError) as caught:
        session.query(query)
    assert (caught.value.category, caught.value.phase, caught.value.detail) == (
        "SyntaxError",
        "compile",
        "VariableAlreadyBound",
    )
    assert graph.load_state() == before


def test_consecutive_creates_share_bindings_and_publish_once(tmp_path: Path) -> None:
    db = Database(tmp_path / "db")
    before = db._app.use_cases.graph.load_state().snapshot.snapshot_id.value
    with db.write_transaction() as session:
        result = session.query(
            "CREATE (a:A {x:1}) CREATE (b:B {x:a.x+1}) CREATE (a)-[:R]->(b) RETURN b.x AS x"
        )
        assert result.rows == ({"x": 2},)
        assert session.query("MATCH (:A)-[r:R]->(:B) RETURN count(r) AS n").rows == ({"n": 1},)
    assert db.load_snapshot().snapshot_id.value == before + 1


def test_unwind_create_with_match_and_updates(tmp_path: Path) -> None:
    db = Database(tmp_path / "db")
    with db.write_transaction() as session:
        result = session.query(
            "UNWIND [1,2,3] AS x CREATE (n:N {old:x}) "
            "WITH n, x MATCH (n:N) SET n:Ready, n.value = x * 10 "
            "REMOVE n.old RETURN n.value AS value ORDER BY value DESC LIMIT 1"
        )
        assert result.rows == ({"value": 30},)
        assert session.query(
            "MATCH (n:Ready) RETURN n.value AS value, n.old AS old ORDER BY value"
        ).rows == (
            {"value": 10, "old": None},
            {"value": 20, "old": None},
            {"value": 30, "old": None},
        )


@pytest.mark.parametrize(
    ("query", "detail", "phase"),
    [
        ("CREATE (:Scratch) WITH 1 AS x CREATE (:N {p:1/(x-1)})", "DivisionByZero", "runtime"),
        ("CREATE (:Scratch) WITH 1 AS x CREATE (:N {p:missing})", "UndefinedVariable", "compile"),
    ],
)
def test_failure_after_first_write_keeps_snapshot_and_catalog(
    tmp_path: Path, query: str, detail: str, phase: str
) -> None:
    db = Database(tmp_path / "db")
    graph = db._app.use_cases.graph
    before = graph.load_state()
    with db.write_transaction() as session, pytest.raises(QueryError) as caught:
        session.query(query)
    assert (caught.value.detail, caught.value.phase) == (detail, phase)
    assert graph.load_state() == before


def test_failed_statement_keeps_earlier_changes_in_explicit_transaction(tmp_path: Path) -> None:
    from grapharrow.use_cases import CypherQueryUseCase, GraphTransactionUseCase

    db = Database(tmp_path / "db")
    graph = db._app.use_cases.graph
    transaction = GraphTransactionUseCase(graph)
    executor = CypherQueryUseCase(transaction)
    executor.execute("CREATE (:Kept)")
    with pytest.raises(QueryError, match="Division by zero"):
        executor.execute("CREATE (:Scratch) WITH 0 AS x CREATE (:N {p:1/x})")
    assert executor.execute("MATCH (n) RETURN labels(n) AS labels").rows == ({"labels": ["Kept"]},)
    assert graph.load_state().nodes == ()
    assert [node.labels for node in transaction.commit().nodes] == [("Kept",)]


@pytest.mark.parametrize("composed", [False, True])
def test_set_relationship_publishes_a_change_in_numeric_type(
    tmp_path: Path, composed: bool
) -> None:
    db = Database(tmp_path / "db")
    with db.write_transaction() as session:
        session.query("CREATE ()-[:R {value:1}]->()")
        before = db.load_snapshot()
        query = "MATCH ()-[r:R]->() SET r.value = 1.0"
        if composed:
            query += " WITH r SET r.echo = r.value"
        result = session.query(query + " RETURN r.value AS value")
    assert type(result.rows[0]["value"]) is float
    after = db.load_snapshot()
    assert after.edges != before.edges
    assert after.edge_properties != before.edge_properties
    assert after.adjacency_by_source == before.adjacency_by_source
    assert after.adjacency_by_dest == before.adjacency_by_dest
    graph = db._app.use_cases.graph
    assert type(graph.load_state().edges[0].properties["value"]) is float
    projection = db._app.repositories.graph.load_edge_property_table(
        after.edge_properties, rel_type="R"
    )
    assert type(projection["value"][0].as_py()) is float
    if composed:
        assert type(projection["echo"][0].as_py()) is float


def test_set_relationship_reuses_buffers_and_rolls_back_a_failed_statement(tmp_path: Path) -> None:
    from grapharrow.use_cases import CypherQueryUseCase, GraphTransactionUseCase

    db = Database(tmp_path / "db")
    with db.write_transaction() as session:
        session.query("CREATE (a)-[:R {value:1}]->(b) CREATE (a)-[:R {value:2}]->(b)")
    graph = db._app.use_cases.graph
    transaction = GraphTransactionUseCase(graph)
    initial = transaction.load_state()
    executor = CypherQueryUseCase(transaction)
    executor.execute("MATCH ()-[r:R]->() WHERE r.value=1 SET r.value=1.0")
    kept = transaction.load_state()
    assert kept.node_table is initial.node_table
    assert kept.edge_table["src_id"].chunk(0).buffers()[1].address == (
        initial.edge_table["src_id"].chunk(0).buffers()[1].address
    )
    assert kept.edge_table["properties_json"].chunk(1).buffers()[2].address == (
        initial.edge_table["properties_json"].chunk(0).buffers()[2].address
    )
    with pytest.raises(QueryError, match="Division by zero"):
        executor.execute("MATCH ()-[r:R]->() SET r.value=3 WITH r, 0 AS zero SET r.other=1/zero")
    assert transaction.load_state() is kept
    result = executor.execute("MATCH ()-[r:R]->() RETURN r.value AS value ORDER BY value")
    assert [type(row["value"]) for row in result.rows] == [float, int]
    assert graph.load_state() == initial
    assert transaction.commit().edge_table is kept.edge_table
    assert graph.load_state().edge_table.equals(kept.edge_table)


@pytest.mark.parametrize("composed", [False, True])
def test_edge_delete_reuses_transaction_tables(tmp_path: Path, composed: bool) -> None:
    from grapharrow.use_cases import CypherQueryUseCase, GraphTransactionUseCase

    db = Database(tmp_path / "parallel")
    with db.write_transaction() as session:
        session.query(
            "CREATE (a)-[:R {value:1}]->(b), (a)-[:R {value:2}]->(b), (a)-[:R {value:3}]->(a)"
        )
    graph = db._app.use_cases.graph
    transaction = GraphTransactionUseCase(graph)
    initial = transaction.load_state()
    executor = CypherQueryUseCase(transaction)
    query = "MATCH ()-[r:R {value:1}]->() DELETE r"
    if composed:
        query += " WITH 1 AS v MATCH ()-[s:R {value:2}]->() DELETE s"
    executor.execute(query)
    remaining = transaction.load_state()
    assert remaining.node_table is initial.node_table
    expected = [3] if composed else [2, 3]
    assert remaining.edge_table["edge_id"].to_pylist() == expected
    assert remaining.edge_table["properties_json"].chunk(0).buffers()[2].address == (
        initial.edge_table["properties_json"].chunk(0).buffers()[2].address
    )
    assert graph.load_state().edge_table.equals(initial.edge_table)

    with pytest.raises(QueryError, match="Division by zero"):
        executor.execute(
            "MATCH ()-[r:R]->() DELETE r WITH 0 AS zero CREATE (:Scratch {value:1/zero})"
        )
    assert transaction.load_state() is remaining
    assert transaction.commit().edge_table is remaining.edge_table
    assert graph.load_state().edge_table.equals(remaining.edge_table, check_metadata=True)


def test_pipeline_merge_reuses_earlier_rows_and_runs_actions(tmp_path: Path) -> None:
    with Database(tmp_path / "db").write_transaction() as session:
        result = session.query(
            "UNWIND [1,1,2] AS x MERGE (n:N {x:x}) "
            "ON CREATE SET n.visits=1 ON MATCH SET n.visits=n.visits+1 "
            "WITH DISTINCT n RETURN n.x AS x, n.visits AS visits ORDER BY x"
        )
    assert result.rows == ({"x": 1, "visits": 2}, {"x": 2, "visits": 1})


def test_deleted_property_in_pipeline_rolls_back_all_writes(tmp_path: Path) -> None:
    db = Database(tmp_path / "db")
    graph = db._app.use_cases.graph
    before = graph.load_state()
    with db.write_transaction() as session, pytest.raises(QueryError) as caught:
        session.query("CREATE (n:N {p:1}) WITH n, {alias:n} AS m DELETE n RETURN m.alias.p")
    assert caught.value.detail == "DeletedEntityAccess"
    assert graph.load_state() == before


@pytest.mark.parametrize(
    ("query", "category", "phase", "detail"),
    [
        ("RETURN 9223372036854775808", "SyntaxError", "compile", "IntegerOverflow"),
        ("RETURN 1 AS x, 2 AS x", "SyntaxError", "compile", "ColumnNameConflict"),
        ("RETURN 1 IN 123", "SyntaxError", "compile", "InvalidArgumentType"),
        ("CREATE (a) MERGE (a)", "SyntaxError", "compile", "VariableAlreadyBound"),
        ("CREATE (a) SET a.maplist=[{num:1}]", "TypeError", "runtime", "InvalidPropertyType"),
        (
            "CREATE (a) CREATE (:N {p:{entity:a}})",
            "TypeError",
            "runtime",
            "InvalidPropertyType",
        ),
        (
            "CREATE (a)-[:R]->() DELETE a",
            "ConstraintVerificationFailed",
            "runtime",
            "DeleteConnectedNode",
        ),
        (
            "CREATE (a),(b) MERGE (a)-[:R {x:null}]->(b)",
            "SemanticError",
            "runtime",
            "MergeReadOwnWrites",
        ),
    ],
)
def test_invalid_statements_report_detail_and_do_not_publish(
    tmp_path: Path, query: str, category: str, phase: str, detail: str
) -> None:
    db = Database(tmp_path / "db")
    graph = db._app.use_cases.graph
    before = graph.load_state()
    with db.write_transaction() as session, pytest.raises(QueryError) as caught:
        session.query(query)
    assert (caught.value.category, caught.value.phase, caught.value.detail) == (
        category,
        phase,
        detail,
    )
    assert graph.load_state() == before


def test_relationship_merge_matches_incoming_and_undirected_patterns(tmp_path: Path) -> None:
    db = Database(tmp_path / "db")
    with db.write_transaction() as session:
        result = session.query(
            "CREATE (a:A), (b:B) "
            "MERGE (a)<-[r:R]-(b) ON CREATE SET r.p=1 "
            "MERGE (a)-[s:R]-(b) ON MATCH SET s.p=s.p+1 "
            "RETURN id(r)=id(s) AS reused, s.p AS p"
        )
        assert result.rows == ({"reused": True, "p": 2},)
        assert session.query("MATCH (:B)-[r:R]->(:A) RETURN count(r) AS n").rows == ({"n": 1},)


def test_optional_missing_binding_does_not_prevent_other_writes(tmp_path: Path) -> None:
    with Database(tmp_path / "db").write_transaction() as session:
        result = session.query(
            "OPTIONAL MATCH (missing:Missing) SET missing.p=1 "
            "REMOVE missing:Missing DELETE missing CREATE (n:Kept) RETURN labels(n) AS labels"
        )
        assert result.rows == ({"labels": ["Kept"]},)


def test_set_properties_and_delete_keep_earlier_projected_value(tmp_path: Path) -> None:
    db = Database(tmp_path / "db")
    with db.write_transaction() as session:
        result = session.query(
            "CREATE (a {old:1})-[r:R]->(b) SET a={next:2} SET a += {next:null,keep:3} "
            "WITH a, r, a.keep AS keep, a.old AS old DELETE r, a RETURN keep,old"
        )
        assert result.rows == ({"keep": 3, "old": None},)
    state = db._app.use_cases.graph.load_state()
    assert len(state.nodes) == 1 and not state.edges


def test_match_results_on_newly_supported_setup_patterns(tmp_path: Path) -> None:
    with Database(tmp_path / "db").write_transaction() as session:
        session.query("CREATE (a:A)-[:T]->(b:B) CREATE (b)-[:R]->(b)")
        assert session.query("MATCH (a)-[:T|:T]->(b) RETURN count(b) AS n").rows == ({"n": 1},)
        result = session.query("MATCH (a:B) OPTIONAL MATCH (a)-[r]-(a) RETURN {node:a,rel:r} AS m")
        assert result.rows[0]["m"]["node"]["labels"] == ["B"]
        assert result.rows[0]["m"]["rel"]["type"] == "R"
        assert len(result.rows) == 1


def test_anonymous_create_does_not_overwrite_user_binding(tmp_path: Path) -> None:
    with Database(tmp_path / "db").write_transaction() as session:
        result = session.query(
            "CREATE (_anon_1:Kept {p:1}) CREATE (:Other {p:2}) RETURN _anon_1.p AS p"
        )
        assert result.rows == ({"p": 1},)


def test_constraint_failure_after_prior_create_preserves_state(tmp_path: Path) -> None:
    from grapharrow.domain import (
        CatalogVersion,
        ConstraintDefinition,
        LabelDefinition,
        LabelId,
        VersionedCatalog,
    )

    db = Database(tmp_path / "db")
    db.initialize()
    db.write_catalog(
        VersionedCatalog(
            version=CatalogVersion(2),
            labels=(LabelDefinition(id=LabelId(1), name="Required"),),
            constraints=(
                ConstraintDefinition(
                    kind="required",
                    owner_kind="label",
                    owner_name="Required",
                    property_name="name",
                ),
            ),
        )
    )
    graph = db._app.use_cases.graph
    before = graph.load_state()
    with db.write_transaction() as session:
        with pytest.raises(QueryError, match="required") as caught:
            session.query("CREATE (:Scratch) CREATE (:Required)")
        assert caught.value.phase == "runtime"
        assert caught.value.code == "Neo.ClientError.Schema.ConstraintValidationFailed"
        assert graph.load_state() == before
        session.query("CREATE (:Scratch) CREATE (:Required {name:'ok'})")
    assert db.load_snapshot().snapshot_id.value == before.snapshot.snapshot_id.value + 1


def test_create_multi_hop_rolls_back_when_second_relationship_violates_constraint(
    tmp_path: Path,
) -> None:
    from grapharrow.domain import (
        CatalogVersion,
        ConstraintDefinition,
        RelationshipTypeDefinition,
        RelationshipTypeId,
        VersionedCatalog,
    )

    db = Database(tmp_path / "db")
    db.initialize()
    db.write_catalog(
        VersionedCatalog(
            version=CatalogVersion(2),
            relationship_types=(RelationshipTypeDefinition(RelationshipTypeId(1), "Required"),),
            constraints=(
                ConstraintDefinition(
                    kind="required",
                    owner_kind="relationship_type",
                    owner_name="Required",
                    property_name="p",
                ),
            ),
        )
    )
    graph = db._app.use_cases.graph
    before = graph.load_state()
    with db.write_transaction() as session:
        with pytest.raises(QueryError, match="required") as caught:
            session.query("CREATE (:A)-[:First]->(:B)-[:Required]->(:C)")
        assert caught.value.detail == "ConstraintViolation"
        assert graph.load_state() == before
        session.query("CREATE (:A)-[:First]->(:B)-[:Required {p:1}]->(:C)")
    after = graph.load_state()
    assert len(after.nodes) == 3 and len(after.edges) == 2
    assert after.snapshot.snapshot_id.value == before.snapshot.snapshot_id.value + 1


@pytest.mark.parametrize("verb", ["CREATE", "MERGE"])
def test_exact_range_is_rejected_in_relationship_writes(tmp_path: Path, verb: str) -> None:
    with Database(tmp_path / "db").write_transaction() as session:
        with pytest.raises(QueryError) as caught:
            session.query(f"{verb} ()-[:R*1..1]->()")
    assert (caught.value.phase, caught.value.detail) == ("compile", "CreatingVarLength")


def test_create_multi_hop_after_with_uses_each_input_row(tmp_path: Path) -> None:
    with Database(tmp_path / "db").write_transaction() as session:
        rows = session.query(
            "UNWIND [1,2] AS x WITH x "
            "CREATE (a:A {x:x})-[:R]->(:B)-[:S]->(:C) RETURN a.x AS x ORDER BY x"
        ).rows
    assert rows == ({"x": 1}, {"x": 2})
