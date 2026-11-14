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


def test_failed_return_rolls_back_create(tmp_path: Path) -> None:
    db = Database(tmp_path / "db")
    graph = db._app.use_cases.graph
    before = graph.load_state()
    with db.write_transaction() as session:
        with pytest.raises(QueryError, match="[Dd]ivi"):
            session.query("CREATE (:Scratch) RETURN 1 / 0 AS value")
    assert graph.load_state() == before
    assert Database(tmp_path / "db")._app.use_cases.graph.load_state() == before


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
