from pathlib import Path

import pytest

from grapharrow import Database
from grapharrow.adapters import parse_cypher
from grapharrow.core.errors import QueryError, TransactionError
from grapharrow.interfaces.cypher_guard import query_requires_write
from grapharrow.use_cases.cypher import (
    CompiledMatchOptionalQuery,
    CompiledMatchQuery,
    CompiledMatchWithQuery,
    CompiledOptionalMatchQuery,
    _build_match_logical_plan,
    _build_match_optional_logical_plan,
    _build_match_with_logical_plan,
    _build_optional_match_logical_plan,
    _compile_query,
)
from grapharrow.use_cases.cypher_plan import (
    AggregateCountPlan,
    DistinctPlan,
    ExpandPlan,
    FilterPlan,
    NodeScanPlan,
    OptionalMatchPlan,
    ProjectPlan,
    ReturnProjectPlan,
    SlicePlan,
    SortPlan,
    WithAggregatePlan,
    WithProjectPlan,
)


def test_match_logical_plan_uses_node_scan_for_single_node_match() -> None:
    compiled = _compile_query(
        parse_cypher("MATCH (n:Person) RETURN n.name ORDER BY n.name ASC LIMIT 1"),
        {},
    )

    assert isinstance(compiled, CompiledMatchQuery)
    plan = _build_match_logical_plan(compiled)

    assert plan is not None
    assert isinstance(plan.root, SlicePlan)
    assert isinstance(plan.root.input, SortPlan)
    assert isinstance(plan.root.input.input, ProjectPlan)
    assert isinstance(plan.root.input.input.input, NodeScanPlan)


def test_match_logical_plan_uses_expand_for_single_hop_path() -> None:
    compiled = _compile_query(
        parse_cypher(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) "
            "RETURN a.name, b.name ORDER BY a.name ASC LIMIT 5"
        ),
        {},
    )

    assert isinstance(compiled, CompiledMatchQuery)
    plan = _build_match_logical_plan(compiled)

    assert plan is not None
    assert isinstance(plan.root, SlicePlan)
    assert isinstance(plan.root.input, SortPlan)
    assert isinstance(plan.root.input.input, ProjectPlan)
    assert isinstance(plan.root.input.input.input, ExpandPlan)


def test_match_logical_plan_uses_nested_expands_for_multi_hop_path() -> None:
    compiled = _compile_query(
        parse_cypher(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
            "RETURN a.name, b.name, c.name ORDER BY a.name ASC"
        ),
        {},
    )

    assert isinstance(compiled, CompiledMatchQuery)
    plan = _build_match_logical_plan(compiled)

    assert plan is not None
    assert isinstance(plan.root, SortPlan)
    assert isinstance(plan.root.input, ProjectPlan)
    assert isinstance(plan.root.input.input, ExpandPlan)
    assert isinstance(plan.root.input.input.input, ExpandPlan)
    assert isinstance(plan.root.input.input.input.input, NodeScanPlan)


def test_multi_match_logical_plan_fuses_linear_path_with_where() -> None:
    compiled = _compile_query(
        parse_cypher(
            "MATCH (a:Person) WHERE a.anchor = true "
            "MATCH (a)-[:KNOWS]->(b:Person) WHERE b.active = true "
            "MATCH (b)-[:KNOWS]->(c:Person) WHERE c.country IN ['IT', 'DE'] "
            "RETURN count(c) AS total"
        ),
        {},
    )

    assert isinstance(compiled, CompiledMatchQuery)
    assert len(compiled.patterns) == 1
    plan = _build_match_logical_plan(compiled)

    assert plan is not None
    assert isinstance(plan.root, AggregateCountPlan)
    assert isinstance(plan.root.input, FilterPlan)
    assert isinstance(plan.root.input.input, ExpandPlan)
    assert isinstance(plan.root.input.input.input, ExpandPlan)
    assert isinstance(plan.root.input.input.input.input, NodeScanPlan)


def test_match_logical_plan_falls_back_for_reused_path_variable() -> None:
    compiled = _compile_query(
        parse_cypher(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(a:Person) RETURN a.name, b.name"
        ),
        {},
    )

    assert isinstance(compiled, CompiledMatchQuery)
    assert _build_match_logical_plan(compiled) is None


def test_match_logical_plan_uses_aggregate_count_for_multi_hop_count_query() -> None:
    compiled = _compile_query(
        parse_cypher(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN COUNT(*) AS total"
        ),
        {},
    )

    assert isinstance(compiled, CompiledMatchQuery)
    plan = _build_match_logical_plan(compiled)

    assert plan is not None
    assert isinstance(plan.root, AggregateCountPlan)
    assert isinstance(plan.root.input, ExpandPlan)
    assert isinstance(plan.root.input.input, ExpandPlan)
    assert isinstance(plan.root.input.input.input, NodeScanPlan)


def test_match_logical_plan_supports_return_distinct() -> None:
    compiled = _compile_query(
        parse_cypher("MATCH (n:Person) RETURN DISTINCT n.country ORDER BY n.country ASC LIMIT 5"),
        {},
    )

    assert isinstance(compiled, CompiledMatchQuery)
    plan = _build_match_logical_plan(compiled)

    assert plan is not None
    assert isinstance(plan.root, SlicePlan)
    assert isinstance(plan.root.input, SortPlan)
    assert isinstance(plan.root.input.input, DistinctPlan)
    assert isinstance(plan.root.input.input.input, ReturnProjectPlan)
    assert isinstance(plan.root.input.input.input.input, NodeScanPlan)


def test_optional_match_logical_plan_uses_optional_match_plan() -> None:
    compiled = _compile_query(
        parse_cypher(
            "OPTIONAL MATCH (n:Person)-[r:KNOWS]->(m:Person) "
            "RETURN n.name, type(r), m.name ORDER BY n.name ASC"
        ),
        {},
    )

    assert isinstance(compiled, CompiledOptionalMatchQuery)
    plan = _build_optional_match_logical_plan(compiled)

    assert plan is not None
    assert isinstance(plan.root, SortPlan)
    assert isinstance(plan.root.input, ProjectPlan)
    assert isinstance(plan.root.input.input, OptionalMatchPlan)
    assert plan.root.input.input.input is None


def test_optional_match_logical_plan_supports_return_distinct() -> None:
    compiled = _compile_query(
        parse_cypher("OPTIONAL MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN DISTINCT n"),
        {},
    )

    assert isinstance(compiled, CompiledOptionalMatchQuery)
    plan = _build_optional_match_logical_plan(compiled)

    assert plan is not None
    assert isinstance(plan.root, DistinctPlan)
    assert isinstance(plan.root.input, ReturnProjectPlan)
    assert isinstance(plan.root.input.input, OptionalMatchPlan)


def test_match_logical_plan_falls_back_for_incompatible_order_by_with_return_distinct() -> None:
    compiled = _compile_query(
        parse_cypher("MATCH (n:Person) RETURN DISTINCT n.country ORDER BY n.name ASC LIMIT 5"),
        {},
    )

    assert isinstance(compiled, CompiledMatchQuery)
    assert _build_match_logical_plan(compiled) is None


def test_optional_match_logical_plan_falls_back_for_projected_return_distinct() -> None:
    compiled = _compile_query(
        parse_cypher(
            "OPTIONAL MATCH (n:Person)-[r:KNOWS]->(m:Person) "
            "RETURN DISTINCT n.name, type(r) ORDER BY n.name ASC"
        ),
        {},
    )

    assert isinstance(compiled, CompiledOptionalMatchQuery)
    assert _build_optional_match_logical_plan(compiled) is None


def test_match_optional_logical_plan_wraps_match_root_with_optional_plan() -> None:
    compiled = _compile_query(
        parse_cypher(
            "MATCH (n:Person) OPTIONAL MATCH (n)-[r:KNOWS]->(m:Person) "
            "RETURN n.name, type(r), m.name ORDER BY n.name ASC"
        ),
        {},
    )

    assert isinstance(compiled, CompiledMatchOptionalQuery)
    plan = _build_match_optional_logical_plan(compiled)

    assert plan is not None
    assert isinstance(plan.root, SortPlan)
    assert isinstance(plan.root.input, ProjectPlan)
    assert isinstance(plan.root.input.input, OptionalMatchPlan)
    assert isinstance(plan.root.input.input.input, NodeScanPlan)


def test_match_optional_logical_plan_pushes_window_into_match_root_when_safe() -> None:
    compiled = _compile_query(
        parse_cypher(
            "MATCH (n:Person) OPTIONAL MATCH (n)-[r:KNOWS]->(m:Person) "
            "RETURN n.name, type(r), m.name ORDER BY n.name ASC LIMIT 10"
        ),
        {},
    )

    assert isinstance(compiled, CompiledMatchOptionalQuery)
    plan = _build_match_optional_logical_plan(compiled)

    assert plan is not None
    assert isinstance(plan.root, SlicePlan)
    assert isinstance(plan.root.input, SortPlan)
    assert isinstance(plan.root.input.input, ProjectPlan)
    assert isinstance(plan.root.input.input.input, OptionalMatchPlan)
    assert isinstance(plan.root.input.input.input.input, SlicePlan)
    assert isinstance(plan.root.input.input.input.input.input, SortPlan)
    assert isinstance(plan.root.input.input.input.input.input.input, NodeScanPlan)


def test_match_with_logical_plan_uses_with_aggregate_plan() -> None:
    compiled = _compile_query(
        parse_cypher(
            "MATCH (n:Person) "
            "WITH n.country AS country, COUNT(*) AS total "
            "RETURN country, total "
            "ORDER BY total DESC, country ASC "
            "LIMIT 10"
        ),
        {},
    )

    assert isinstance(compiled, CompiledMatchWithQuery)
    plan = _build_match_with_logical_plan(compiled)

    assert plan is not None
    assert isinstance(plan.root, SlicePlan)
    assert isinstance(plan.root.input, SortPlan)
    assert isinstance(plan.root.input.input, WithAggregatePlan)
    assert isinstance(plan.root.input.input.input, ProjectPlan)
    assert isinstance(plan.root.input.input.input.input, NodeScanPlan)


def test_match_with_logical_plan_uses_with_project_plan_for_non_aggregate_with() -> None:
    compiled = _compile_query(
        parse_cypher("MATCH (n:Person) WITH n.name AS name RETURN name ORDER BY name ASC LIMIT 5"),
        {},
    )

    assert isinstance(compiled, CompiledMatchWithQuery)
    plan = _build_match_with_logical_plan(compiled)

    assert plan is not None
    assert isinstance(plan.root, SlicePlan)
    assert isinstance(plan.root.input, SortPlan)
    assert isinstance(plan.root.input.input, WithProjectPlan)
    assert isinstance(plan.root.input.input.input, ProjectPlan)
    assert isinstance(plan.root.input.input.input.input, NodeScanPlan)


def test_match_with_logical_plan_keeps_post_with_filter_above_aggregate() -> None:
    compiled = _compile_query(
        parse_cypher(
            "MATCH (n:Person) "
            "WITH n.country AS country, COUNT(*) AS total "
            "WHERE total > 1 "
            "RETURN country, total "
            "ORDER BY total DESC"
        ),
        {},
    )

    assert isinstance(compiled, CompiledMatchWithQuery)
    plan = _build_match_with_logical_plan(compiled)

    assert plan is not None
    assert isinstance(plan.root, SortPlan)
    assert isinstance(plan.root.input, FilterPlan)
    assert isinstance(plan.root.input.input, WithAggregatePlan)
    assert isinstance(plan.root.input.input.input, ProjectPlan)
    assert isinstance(plan.root.input.input.input.input, NodeScanPlan)


def test_match_with_logical_plan_supports_with_order_and_limit_before_return() -> None:
    compiled = _compile_query(
        parse_cypher(
            "MATCH (n:Person) "
            "WITH n.country AS country, COUNT(*) AS total "
            "ORDER BY total DESC, country ASC "
            "LIMIT 3 "
            "RETURN country, total"
        ),
        {},
    )

    assert isinstance(compiled, CompiledMatchWithQuery)
    plan = _build_match_with_logical_plan(compiled)

    assert plan is not None
    assert isinstance(plan.root, SlicePlan)
    assert isinstance(plan.root.input, SortPlan)
    assert isinstance(plan.root.input.input, WithAggregatePlan)
    assert isinstance(plan.root.input.input.input, ProjectPlan)
    assert isinstance(plan.root.input.input.input.input, NodeScanPlan)


def test_match_with_logical_plan_supports_with_distinct_for_non_aggregate_with() -> None:
    compiled = _compile_query(
        parse_cypher(
            "MATCH (n:Person) WITH DISTINCT n.name AS name ORDER BY name ASC LIMIT 1 RETURN name"
        ),
        {},
    )

    assert isinstance(compiled, CompiledMatchWithQuery)
    plan = _build_match_with_logical_plan(compiled)

    assert plan is not None
    assert isinstance(plan.root, SlicePlan)
    assert isinstance(plan.root.input, SortPlan)
    assert isinstance(plan.root.input.input, DistinctPlan)
    assert isinstance(plan.root.input.input.input, WithProjectPlan)
    assert isinstance(plan.root.input.input.input.input, ProjectPlan)
    assert isinstance(plan.root.input.input.input.input.input, NodeScanPlan)


def test_create_and_match_single_node_with_params_and_where(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        create_result = session.query(
            "CREATE (n:Person {name: $name, age: 30}) RETURN n",
            {"name": "Alice"},
        )

    assert create_result.columns == ("n",)
    assert create_result.rows[0]["n"] == {
        "id": 1,
        "labels": ["Person"],
        "properties": {"age": 30, "name": "Alice"},
    }

    with database.read_transaction() as session:
        match_result = session.query(
            "MATCH (n:Person) WHERE n.name = $name RETURN n ORDER BY n.age DESC LIMIT 1",
            {"name": "Alice"},
        )

    assert match_result.columns == ("n",)
    assert match_result.rows == (
        {
            "n": {
                "id": 1,
                "labels": ["Person"],
                "properties": {"age": 30, "name": "Alice"},
            }
        },
    )


def test_create_relationship_and_match_projected_properties(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        create_result = session.query(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, b"
        )

    assert create_result.columns == ("a", "b")
    assert create_result.rows[0]["a"]["properties"]["name"] == "Alice"
    assert create_result.rows[0]["b"]["properties"]["name"] == "Bob"

    with database.read_transaction() as session:
        match_result = session.query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) "
            "RETURN a.name, b.name ORDER BY a.name ASC LIMIT 1"
        )

    assert match_result.columns == ("a.name", "b.name")
    assert match_result.rows == ({"a.name": "Alice", "b.name": "Bob"},)


def test_create_supports_multiple_node_patterns_in_one_statement(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        result = session.query(
            "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) RETURN a, b"
        )

    assert result.columns == ("a", "b")
    assert result.rows == (
        {
            "a": {"id": 1, "labels": ["Person"], "properties": {"name": "Alice"}},
            "b": {"id": 2, "labels": ["Person"], "properties": {"name": "Bob"}},
        },
    )


def test_create_supports_multiple_patterns_with_relationships_in_one_statement(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        result = session.query(
            "CREATE (a:Person {name: 'Alice'}), "
            "(b:Person {name: 'Bob'}), "
            "(a)-[:KNOWS]->(b) "
            "RETURN a, b"
        )

    assert result.columns == ("a", "b")
    assert result.rows == (
        {
            "a": {"id": 1, "labels": ["Person"], "properties": {"name": "Alice"}},
            "b": {"id": 2, "labels": ["Person"], "properties": {"name": "Bob"}},
        },
    )

    with database.read_transaction() as session:
        match_result = session.query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")

    assert match_result.rows == ({"a.name": "Alice", "b.name": "Bob"},)


def test_create_supports_reusing_created_variables_across_pattern_parts(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        result = session.query(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}), "
            "(b)-[:KNOWS]->(c:Person {name: 'Carol'}) "
            "RETURN a, b, c"
        )

    assert result.columns == ("a", "b", "c")
    assert result.rows == (
        {
            "a": {"id": 1, "labels": ["Person"], "properties": {"name": "Alice"}},
            "b": {"id": 2, "labels": ["Person"], "properties": {"name": "Bob"}},
            "c": {"id": 3, "labels": ["Person"], "properties": {"name": "Carol"}},
        },
    )

    with database.read_transaction() as session:
        match_result = session.query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
            "RETURN a.name, b.name, c.name"
        )

    assert match_result.rows == ({"a.name": "Alice", "b.name": "Bob", "c.name": "Carol"},)


def test_merge_creates_a_node_when_no_match_exists(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        result = session.query("MERGE (n:Person {name: 'Alice'}) RETURN n")

    assert result.columns == ("n",)
    assert result.rows == (
        {
            "n": {
                "id": 1,
                "labels": ["Person"],
                "properties": {"name": "Alice"},
            }
        },
    )


def test_merge_reuses_existing_matching_nodes(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (n:Person {name: 'Alice'}) RETURN n")
        result = session.query("MERGE (n:Person {name: 'Alice'}) RETURN n")

    assert result.rows == (
        {
            "n": {
                "id": 1,
                "labels": ["Person"],
                "properties": {"name": "Alice"},
            }
        },
    )

    with database.read_transaction() as session:
        persisted = session.query("MATCH (n:Person) RETURN COUNT(*) AS total")

    assert persisted.rows == ({"total": 1},)


def test_merge_returns_multiple_existing_matches_for_same_pattern(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (n:Person {name: 'Alice'}) RETURN n")
        session.query("CREATE (n:Person {name: 'Alice'}) RETURN n")

    with database.write_transaction() as session:
        result = session.query("MERGE (n:Person {name: 'Alice'}) RETURN n.name ORDER BY id(n) ASC")

    assert result.rows == (
        {"n.name": "Alice"},
        {"n.name": "Alice"},
    )


def test_match_merge_creates_relationship_only_once(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:FRIEND]->(b:Person {name: 'Bob'}) RETURN a, b"
        )

        first = session.query(
            "MATCH (a:Person)-[:FRIEND]->(b:Person) MERGE (a)-[:KNOWS]->(b) RETURN a.name, b.name"
        )
        second = session.query(
            "MATCH (a:Person)-[:FRIEND]->(b:Person) MERGE (a)-[:KNOWS]->(b) RETURN a.name, b.name"
        )

    assert first.rows == ({"a.name": "Alice", "b.name": "Bob"},)
    assert second.rows == ({"a.name": "Alice", "b.name": "Bob"},)

    with database.read_transaction() as session:
        count_result = session.query("MATCH ()-[r:KNOWS]->() RETURN COUNT(r) AS total")

    assert count_result.rows == ({"total": 1},)


def test_match_merge_can_create_relationship_with_properties(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:FRIEND]->(b:Person {name: 'Bob'}) RETURN a, b"
        )
        session.query(
            "MATCH (a:Person)-[:FRIEND]->(b:Person) "
            "MERGE (a)-[:KNOWS {since: 2024}]->(b) "
            "RETURN a.name, b.name"
        )
        session.query(
            "MATCH (a:Person)-[:FRIEND]->(b:Person) "
            "MERGE (a)-[:KNOWS {since: 2025}]->(b) "
            "RETURN a.name, b.name"
        )

    with database.read_transaction() as session:
        count_result = session.query("MATCH ()-[r:KNOWS]->() RETURN COUNT(r) AS total")

    assert count_result.rows == ({"total": 2},)


def test_merge_supports_on_create_and_on_match_set_actions(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        created = session.query(
            "MERGE (n:Person {name: 'Alice'}) "
            "ON CREATE SET n.age = 30 "
            "ON MATCH SET n.last_seen = 1 "
            "RETURN n"
        )
        matched = session.query(
            "MERGE (n:Person {name: 'Alice'}) "
            "ON CREATE SET n.age = 30 "
            "ON MATCH SET n.last_seen = 1 "
            "RETURN n"
        )

    assert created.rows == (
        {
            "n": {
                "id": 1,
                "labels": ["Person"],
                "properties": {"name": "Alice", "age": 30},
            }
        },
    )
    assert matched.rows == (
        {
            "n": {
                "id": 1,
                "labels": ["Person"],
                "properties": {"name": "Alice", "age": 30, "last_seen": 1},
            }
        },
    )


def test_match_merge_supports_on_create_and_on_match_set_actions(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:FRIEND]->(b:Person {name: 'Bob'}) RETURN a, b"
        )
        created = session.query(
            "MATCH (a:Person)-[:FRIEND]->(b:Person) "
            "MERGE (a)-[:KNOWS]->(b) "
            "ON CREATE SET a.flag = true "
            "ON MATCH SET b.seen = true "
            "RETURN a.flag, b.seen"
        )
        matched = session.query(
            "MATCH (a:Person)-[:FRIEND]->(b:Person) "
            "MERGE (a)-[:KNOWS]->(b) "
            "ON CREATE SET a.flag = true "
            "ON MATCH SET b.seen = true "
            "RETURN a.flag, b.seen"
        )

    assert created.rows == ({"a.flag": True, "b.seen": None},)
    assert matched.rows == ({"a.flag": True, "b.seen": True},)

    with database.read_transaction() as session:
        persisted = session.query(
            "MATCH (a:Person)-[:FRIEND]->(b:Person) RETURN a.flag, b.seen ORDER BY a.name ASC"
        )

    assert persisted.rows == ({"a.flag": True, "b.seen": True},)


def test_match_merge_supports_relationship_variable_and_relationship_set_actions(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:FRIEND]->(b:Person {name: 'Bob'}) RETURN a, b"
        )
        created = session.query(
            "MATCH (a:Person)-[:FRIEND]->(b:Person) "
            "MERGE (a)-[r:KNOWS]->(b) "
            "ON CREATE SET r.since = 2024 "
            "ON MATCH SET r.last_seen = 1 "
            "RETURN type(r), r.since, r.last_seen"
        )
        matched = session.query(
            "MATCH (a:Person)-[:FRIEND]->(b:Person) "
            "MERGE (a)-[r:KNOWS]->(b) "
            "ON CREATE SET r.since = 2024 "
            "ON MATCH SET r.last_seen = 1 "
            "RETURN r"
        )

    assert created.rows == ({"type(r)": "KNOWS", "r.since": 2024, "r.last_seen": None},)
    assert matched.rows == (
        {
            "r": {
                "id": 2,
                "type": "KNOWS",
                "source": 1,
                "target": 2,
                "properties": {"since": 2024, "last_seen": 1},
            }
        },
    )


def test_match_merge_supports_inline_properties_on_bound_nodes(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice', status: 'active'})-[:FRIEND]->"
            "(b:Person {name: 'Bob'}) RETURN a, b"
        )

    with database.write_transaction() as session:
        result = session.query(
            "MATCH (a:Person)-[:FRIEND]->(b:Person) "
            "MERGE (a:Person {status: 'active'})-[:KNOWS]->(b:Person) "
            "RETURN a.name, b.name"
        )

    assert result.rows == ({"a.name": "Alice", "b.name": "Bob"},)

    with database.read_transaction() as session:
        count_result = session.query("MATCH ()-[r:KNOWS]->() RETURN COUNT(r) AS total")

    assert count_result.rows == ({"total": 1},)


def test_match_merge_filters_out_rows_when_bound_node_properties_do_not_match(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice', status: 'inactive'})-[:FRIEND]->"
            "(b:Person {name: 'Bob'}) RETURN a, b"
        )

    with database.write_transaction() as session:
        result = session.query(
            "MATCH (a:Person)-[:FRIEND]->(b:Person) "
            "MERGE (a:Person {status: 'active'})-[:KNOWS]->(b:Person) "
            "RETURN a.name, b.name"
        )

    assert result.rows == ()

    with database.read_transaction() as session:
        count_result = session.query("MATCH ()-[r:KNOWS]->() RETURN COUNT(r) AS total")

    assert count_result.rows == ({"total": 0},)


def test_match_merge_supports_unbound_inline_node_pattern(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (a:Person {name: 'Alice'}) RETURN a")
        created = session.query(
            "MATCH (a:Person {name: 'Alice'}) "
            "MERGE (a)-[:TAGGED]->(t:Tag {name: 'vip'}) "
            "ON CREATE SET t.created = true "
            "ON MATCH SET t.last_seen = 1 "
            "RETURN t"
        )
        matched = session.query(
            "MATCH (a:Person {name: 'Alice'}) "
            "MERGE (a)-[:TAGGED]->(t:Tag {name: 'vip'}) "
            "ON CREATE SET t.created = true "
            "ON MATCH SET t.last_seen = 1 "
            "RETURN t"
        )

    assert created.rows == (
        {
            "t": {
                "id": 2,
                "labels": ["Tag"],
                "properties": {"name": "vip", "created": True},
            }
        },
    )
    assert matched.rows == (
        {
            "t": {
                "id": 2,
                "labels": ["Tag"],
                "properties": {"name": "vip", "created": True, "last_seen": 1},
            }
        },
    )

    with database.read_transaction() as session:
        tag_count = session.query("MATCH (t:Tag) RETURN COUNT(*) AS total")
        edge_count = session.query("MATCH (:Person)-[r:TAGGED]->(:Tag) RETURN COUNT(r) AS total")

    assert tag_count.rows == ({"total": 1},)
    assert edge_count.rows == ({"total": 1},)


def test_match_merge_supports_unbound_inline_left_node_pattern(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (a:Person {name: 'Alice'}) RETURN a")
        created = session.query(
            "MATCH (a:Person {name: 'Alice'}) "
            "MERGE (t:Tag {name: 'vip'})-[:TAGGED]->(a) "
            "ON CREATE SET t.created = true "
            "ON MATCH SET t.last_seen = 1 "
            "RETURN t"
        )
        matched = session.query(
            "MATCH (a:Person {name: 'Alice'}) "
            "MERGE (t:Tag {name: 'vip'})-[:TAGGED]->(a) "
            "ON CREATE SET t.created = true "
            "ON MATCH SET t.last_seen = 1 "
            "RETURN t"
        )

    assert created.rows == (
        {
            "t": {
                "id": 2,
                "labels": ["Tag"],
                "properties": {"name": "vip", "created": True},
            }
        },
    )
    assert matched.rows == (
        {
            "t": {
                "id": 2,
                "labels": ["Tag"],
                "properties": {"name": "vip", "created": True, "last_seen": 1},
            }
        },
    )

    with database.read_transaction() as session:
        tag_count = session.query("MATCH (t:Tag) RETURN COUNT(*) AS total")
        edge_count = session.query("MATCH (:Tag)-[r:TAGGED]->(:Person) RETURN COUNT(r) AS total")

    assert tag_count.rows == ({"total": 1},)
    assert edge_count.rows == ({"total": 1},)


def test_match_supports_linear_multi_hop_paths(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        alice = session.add_node(labels=["Person"], properties={"name": "Alice"})
        bob = session.add_node(labels=["Person"], properties={"name": "Bob"})
        carol = session.add_node(labels=["Person"], properties={"name": "Carol"})
        session.add_edge(src=alice["id"], rel_type="KNOWS", dst=bob["id"], properties={})
        session.add_edge(src=bob["id"], rel_type="KNOWS", dst=carol["id"], properties={})

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
            "RETURN a.name, b.name, c.name ORDER BY a.name ASC"
        )

    assert result.columns == ("a.name", "b.name", "c.name")
    assert result.rows == ({"a.name": "Alice", "b.name": "Bob", "c.name": "Carol"},)


def test_match_supports_three_hop_linear_paths(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        alice = session.add_node(labels=["Person"], properties={"name": "Alice"})
        bob = session.add_node(labels=["Person"], properties={"name": "Bob"})
        carol = session.add_node(labels=["Person"], properties={"name": "Carol"})
        dave = session.add_node(labels=["Person"], properties={"name": "Dave"})
        session.add_edge(src=alice["id"], rel_type="KNOWS", dst=bob["id"], properties={})
        session.add_edge(src=bob["id"], rel_type="KNOWS", dst=carol["id"], properties={})
        session.add_edge(src=carol["id"], rel_type="KNOWS", dst=dave["id"], properties={})

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person) "
            "RETURN a.name, b.name, c.name, d.name ORDER BY a.name ASC"
        )

    assert result.columns == ("a.name", "b.name", "c.name", "d.name")
    assert result.rows == (
        {"a.name": "Alice", "b.name": "Bob", "c.name": "Carol", "d.name": "Dave"},
    )


def test_match_supports_three_hop_projection_order_by_and_limit(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        alice = session.add_node(
            labels=["Person"],
            properties={"name": "Alice", "external_id": "alice"},
        )
        bob = session.add_node(
            labels=["Person"],
            properties={"name": "Bob", "external_id": "bob"},
        )
        carol = session.add_node(
            labels=["Person"],
            properties={"name": "Carol", "external_id": "carol"},
        )
        dave = session.add_node(
            labels=["Person"],
            properties={"name": "Dave", "external_id": "dave"},
        )
        session.add_edge(src=alice["id"], rel_type="KNOWS", dst=bob["id"], properties={})
        session.add_edge(src=bob["id"], rel_type="KNOWS", dst=carol["id"], properties={})
        session.add_edge(src=carol["id"], rel_type="KNOWS", dst=dave["id"], properties={})

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person) "
            "RETURN a.external_id, d.external_id "
            "ORDER BY a.external_id ASC, d.external_id ASC "
            "LIMIT 10"
        )

    assert result.columns == ("a.external_id", "d.external_id")
    assert result.rows == ({"a.external_id": "alice", "d.external_id": "dave"},)


def test_match_where_pushdown_preserves_three_hop_results(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    query = (
        "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person) "
        "WHERE a.anchor = true "
        "AND b.active = true "
        "AND c.country IN ['IT', 'DE'] "
        "AND d.score >= 0.5 "
        "AND d.external_id STARTS WITH 'person-000' "
        "RETURN a.external_id, d.external_id "
        "ORDER BY a.external_id ASC, d.external_id ASC"
    )

    planner_database = _three_hop_pushdown_database(tmp_path / "planner-db")
    with planner_database.read_transaction() as session:
        planner_result = session.query(query)

    monkeypatch.setenv("GRAPHARROW_DISABLE_CYPHER_PLANNER", "1")
    fallback_database = _three_hop_pushdown_database(tmp_path / "fallback-db")
    with fallback_database.read_transaction() as session:
        fallback_result = session.query(query)

    expected_rows = ({"a.external_id": "person-000001", "d.external_id": "person-000004"},)
    assert planner_result.columns == ("a.external_id", "d.external_id")
    assert planner_result.rows == expected_rows
    assert fallback_result.rows == expected_rows


def _three_hop_pushdown_database(path: Path) -> Database:
    database = Database(path)
    with database.write_transaction() as session:
        alice = session.add_node(
            labels=["Person"],
            properties={
                "external_id": "person-000001",
                "anchor": True,
                "active": True,
                "country": "IT",
                "score": 0.1,
            },
        )
        bob = session.add_node(
            labels=["Person"],
            properties={
                "external_id": "person-000002",
                "anchor": False,
                "active": True,
                "country": "US",
                "score": 0.2,
            },
        )
        carol = session.add_node(
            labels=["Person"],
            properties={
                "external_id": "person-000003",
                "anchor": False,
                "active": True,
                "country": "IT",
                "score": 0.3,
            },
        )
        dave = session.add_node(
            labels=["Person"],
            properties={
                "external_id": "person-000004",
                "anchor": False,
                "active": True,
                "country": "DE",
                "score": 0.9,
            },
        )
        eve = session.add_node(
            labels=["Person"],
            properties={
                "external_id": "person-999999",
                "anchor": False,
                "active": True,
                "country": "IT",
                "score": 0.9,
            },
        )
        inactive = session.add_node(
            labels=["Person"],
            properties={
                "external_id": "person-000005",
                "anchor": False,
                "active": False,
                "country": "IT",
                "score": 0.9,
            },
        )
        session.add_edge(src=alice["id"], rel_type="KNOWS", dst=bob["id"], properties={})
        session.add_edge(src=bob["id"], rel_type="KNOWS", dst=carol["id"], properties={})
        session.add_edge(src=carol["id"], rel_type="KNOWS", dst=dave["id"], properties={})
        session.add_edge(src=carol["id"], rel_type="KNOWS", dst=eve["id"], properties={})
        session.add_edge(src=alice["id"], rel_type="KNOWS", dst=inactive["id"], properties={})
        session.add_edge(src=inactive["id"], rel_type="KNOWS", dst=carol["id"], properties={})
    return database


def test_match_supports_variable_length_relationship_ranges(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        alice = session.add_node(labels=["Person"], properties={"name": "Alice"})
        bob = session.add_node(labels=["Person"], properties={"name": "Bob"})
        carol = session.add_node(labels=["Person"], properties={"name": "Carol"})
        session.add_edge(src=alice["id"], rel_type="KNOWS", dst=bob["id"], properties={})
        session.add_edge(src=bob["id"], rel_type="KNOWS", dst=carol["id"], properties={})

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) "
            "RETURN DISTINCT a.name, b.name ORDER BY b.name ASC"
        )

    assert result.columns == ("a.name", "b.name")
    assert {(row["a.name"], row["b.name"]) for row in result.rows} == {
        ("Alice", "Bob"),
        ("Alice", "Carol"),
        ("Bob", "Carol"),
    }


def test_match_distinct_range_hop_applies_distinct_before_limit(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        alice = session.add_node(labels=["Person"], properties={"name": "Alice"})
        bob = session.add_node(labels=["Person"], properties={"name": "Bob"})
        bridge = session.add_node(labels=["Person"], properties={"name": "Bridge"})
        carol = session.add_node(labels=["Person"], properties={"name": "Carol"})
        session.add_edge(src=alice["id"], rel_type="KNOWS", dst=bob["id"], properties={})
        session.add_edge(src=alice["id"], rel_type="KNOWS", dst=bridge["id"], properties={})
        session.add_edge(src=bridge["id"], rel_type="KNOWS", dst=bob["id"], properties={})
        session.add_edge(src=alice["id"], rel_type="KNOWS", dst=carol["id"], properties={})

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) "
            "WHERE a.name = 'Alice' "
            "RETURN DISTINCT a.name, b.name "
            "ORDER BY b.name ASC "
            "LIMIT 2"
        )

    assert result.rows == (
        {"a.name": "Alice", "b.name": "Bob"},
        {"a.name": "Alice", "b.name": "Bridge"},
    )


def test_match_supports_exact_length_relationship_ranges(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        alice = session.add_node(labels=["Person"], properties={"name": "Alice"})
        bob = session.add_node(labels=["Person"], properties={"name": "Bob"})
        carol = session.add_node(labels=["Person"], properties={"name": "Carol"})
        session.add_edge(src=alice["id"], rel_type="KNOWS", dst=bob["id"], properties={})
        session.add_edge(src=bob["id"], rel_type="KNOWS", dst=carol["id"], properties={})

    with database.read_transaction() as session:
        result = session.query("MATCH (a:Person)-[:KNOWS*2]->(b:Person) RETURN a.name, b.name")

    assert result.rows == ({"a.name": "Alice", "b.name": "Carol"},)


def test_match_supports_count_of_relationship_binding(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        alice = session.add_node(labels=["Person"], properties={"name": "Alice"})
        bob = session.add_node(labels=["Person"], properties={"name": "Bob"})
        carol = session.add_node(labels=["Person"], properties={"name": "Carol"})
        session.add_edge(src=alice["id"], rel_type="KNOWS", dst=bob["id"], properties={})
        session.add_edge(src=bob["id"], rel_type="KNOWS", dst=carol["id"], properties={})

    with database.read_transaction() as session:
        result = session.query("MATCH ()-[r]->() RETURN count(r) as count")

    assert result.columns == ("count",)
    assert result.rows == ({"count": 2},)


def test_match_with_can_pipeline_entity_bindings(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (n:Person {name: 'Alice'}) RETURN n")
        session.query("CREATE (n:Person {name: 'Bob'}) RETURN n")

    with database.read_transaction() as session:
        result = session.query("MATCH (n:Person) WITH n RETURN n.name ORDER BY n.name ASC")

    assert result.columns == ("n.name",)
    assert result.rows == (
        {"n.name": "Alice"},
        {"n.name": "Bob"},
    )


def test_match_with_can_rename_entity_bindings(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (n:Person {name: 'Alice'}) RETURN n")

    with database.read_transaction() as session:
        result = session.query("MATCH (n:Person) WITH n AS person RETURN person.name")

    assert result.columns == ("person.name",)
    assert result.rows == ({"person.name": "Alice"},)


def test_match_with_can_project_scalar_aliases_and_filter_on_them(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (n:Person {name: 'Alice'}) RETURN n")
        session.query("CREATE (n:Person {name: 'Bob'}) RETURN n")
        session.query("CREATE (n:Person {name: 'Carol'}) RETURN n")

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person) "
            "WITH n.name AS name "
            "WHERE name CONTAINS 'o' "
            "RETURN name ORDER BY name DESC"
        )

    assert result.columns == ("name",)
    assert result.rows == (
        {"name": "Carol"},
        {"name": "Bob"},
    )


def test_match_with_supports_distinct_order_and_limit(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (n:Person {name: 'Alice'}) RETURN n")
        session.query("CREATE (n:Person {name: 'Alice'}) RETURN n")
        session.query("CREATE (n:Person {name: 'Bob'}) RETURN n")

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person) WITH DISTINCT n.name AS name ORDER BY name ASC LIMIT 1 RETURN name"
        )

    assert result.columns == ("name",)
    assert result.rows == ({"name": "Alice"},)


def test_match_with_supports_count_over_projected_scalar_aliases(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (n:Person {name: 'Alice'}) RETURN n")
        session.query("CREATE (n:Person {name: 'Bob'}) RETURN n")

    with database.read_transaction() as session:
        result = session.query("MATCH (n:Person) WITH n.name AS name RETURN COUNT(name) AS total")

    assert result.columns == ("total",)
    assert result.rows == ({"total": 2},)


def test_match_with_supports_aggregate_only_pipeline(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (n:Person {name: 'Alice'}) RETURN n")
        session.query("CREATE (n:Person {name: 'Bob'}) RETURN n")

    with database.read_transaction() as session:
        result = session.query("MATCH (n:Person) WITH COUNT(*) AS total RETURN total")

    assert result.columns == ("total",)
    assert result.rows == ({"total": 2},)


def test_match_with_supports_aggregate_only_where_and_distinct_count(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (n:Person {name: 'Alice'}) RETURN n")
        session.query("CREATE (n:Person {name: 'Alice'}) RETURN n")
        session.query("CREATE (n:Person {name: 'Bob'}) RETURN n")

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person) "
            "WITH COUNT(DISTINCT n.name) AS unique_names "
            "WHERE unique_names > 1 "
            "RETURN unique_names"
        )

    assert result.columns == ("unique_names",)
    assert result.rows == ({"unique_names": 2},)


def test_match_with_supports_grouping_by_scalar_projection(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (n:Person {name: 'Alice'}) RETURN n")
        session.query("CREATE (n:Person {name: 'Alice'}) RETURN n")
        session.query("CREATE (n:Person {name: 'Bob'}) RETURN n")

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person) "
            "WITH n.name AS name, COUNT(*) AS total "
            "RETURN name, total ORDER BY name ASC"
        )

    assert result.columns == ("name", "total")
    assert result.rows == (
        {"name": "Alice", "total": 2},
        {"name": "Bob", "total": 1},
    )


def test_match_with_supports_grouping_by_entity_binding(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        alice = session.add_node(labels=["Person"], properties={"name": "Alice"})
        bob = session.add_node(labels=["Person"], properties={"name": "Bob"})
        carol = session.add_node(labels=["Person"], properties={"name": "Carol"})
        session.add_edge(src=alice["id"], rel_type="KNOWS", dst=bob["id"], properties={})
        session.add_edge(src=alice["id"], rel_type="KNOWS", dst=carol["id"], properties={})

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person)-[r:KNOWS]->(m:Person) "
            "WITH n, COUNT(r) AS outgoing "
            "RETURN n.name, outgoing"
        )

    assert result.columns == ("n.name", "outgoing")
    assert result.rows == ({"n.name": "Alice", "outgoing": 2},)


def test_match_with_requires_alias_for_scalar_expressions(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (n:Person {name: 'Alice'}) RETURN n")

    with database.read_transaction() as session:
        with pytest.raises(QueryError, match="require an explicit alias"):
            session.query("MATCH (n:Person) WITH n.name RETURN n.name")


def test_match_after_with_is_not_supported_yet(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, b"
        )
        session.query(
            "CREATE (c:Person {name: 'Carol'})-[:KNOWS]->(d:Person {name: 'Dave'}) RETURN c, d"
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person) "
            "WITH n "
            "MATCH (n)-[:KNOWS]->(m:Person) "
            "RETURN n.name, m.name ORDER BY n.name ASC"
        )

    assert result.columns == ("n.name", "m.name")
    assert result.rows == (
        {"n.name": "Alice", "m.name": "Bob"},
        {"n.name": "Carol", "m.name": "Dave"},
    )


def test_match_with_match_can_filter_using_new_scope(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, b"
        )
        session.query(
            "CREATE (c:Person {name: 'Carol'})-[:KNOWS]->(d:Person {name: 'Dave'}) RETURN c, d"
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person) "
            "WITH n "
            "MATCH (n)-[:KNOWS]->(m:Person) "
            "WHERE m.name STARTS WITH 'D' "
            "RETURN m.name"
        )

    assert result.columns == ("m.name",)
    assert result.rows == ({"m.name": "Dave"},)


def test_match_after_with_rejects_reusing_scalar_alias_as_entity(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, b"
        )

    with database.read_transaction() as session:
        with pytest.raises(QueryError, match="cannot reuse scalar binding"):
            session.query("MATCH (n:Person) WITH n.name AS n MATCH (n)-[:KNOWS]->(m) RETURN m.name")


def test_match_set_updates_node_property_and_returns_updated_node(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (n:Person {name: 'Alice', age: 30}) RETURN n")
        result = session.query("MATCH (n:Person) WHERE n.name = 'Alice' SET n.age = 31 RETURN n")

    assert result.columns == ("n",)
    assert result.rows == (
        {
            "n": {
                "id": 1,
                "labels": ["Person"],
                "properties": {"name": "Alice", "age": 31},
            }
        },
    )

    with database.read_transaction() as session:
        persisted = session.query("MATCH (n:Person) RETURN n.age")

    assert persisted.rows == ({"n.age": 31},)


def test_match_set_updates_relationship_property_and_returns_updated_projection(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a"
        )
        result = session.query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) SET r.since = 2025 RETURN r.since"
        )

    assert result.columns == ("r.since",)
    assert result.rows == ({"r.since": 2025},)

    with database.read_transaction() as session:
        persisted = session.query("MATCH (:Person)-[r:KNOWS]->(:Person) RETURN r.since")

    assert persisted.rows == ({"r.since": 2025},)


def test_match_set_plus_equals_merges_node_properties(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (n:Person {name: 'Alice', age: 30}) RETURN n")
        result = session.query(
            "MATCH (n:Person) WHERE n.name = 'Alice' SET n += {age: 31, city: 'Rome'} RETURN n"
        )

    assert result.columns == ("n",)
    assert result.rows == (
        {
            "n": {
                "id": 1,
                "labels": ["Person"],
                "properties": {"name": "Alice", "age": 31, "city": "Rome"},
            }
        },
    )

    with database.read_transaction() as session:
        persisted = session.query("MATCH (n:Person) RETURN n.age, n.city")

    assert persisted.rows == ({"n.age": 31, "n.city": "Rome"},)


def test_match_set_plus_equals_merges_relationship_properties_from_param(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a"
        )
        result = session.query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) SET r += $props RETURN r.since, r.weight",
            params={"props": {"since": 2025, "weight": 0.9}},
        )

    assert result.columns == ("r.since", "r.weight")
    assert result.rows == ({"r.since": 2025, "r.weight": 0.9},)

    with database.read_transaction() as session:
        persisted = session.query("MATCH (:Person)-[r:KNOWS]->(:Person) RETURN r.since, r.weight")

    assert persisted.rows == ({"r.since": 2025, "r.weight": 0.9},)


def test_match_set_plus_equals_requires_map_value(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (n:Person {name: 'Alice'}) RETURN n")
        with pytest.raises(QueryError, match="SET \\+= requires a map value"):
            session.query("MATCH (n:Person) SET n += 1 RETURN n")


def test_read_transaction_rejects_set_queries(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (n:Person {name: 'Alice'}) RETURN n")

    with database.read_transaction() as session:
        with pytest.raises(TransactionError, match="write transaction"):
            session.query("MATCH (n:Person) SET n.age = 31 RETURN n")


def test_match_remove_deletes_node_property_and_returns_null_projection(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (n:Person {name: 'Alice', temp: true}) RETURN n")
        result = session.query("MATCH (n:Person) REMOVE n.temp RETURN n.temp, n.name")

    assert result.columns == ("n.temp", "n.name")
    assert result.rows == ({"n.temp": None, "n.name": "Alice"},)

    with database.read_transaction() as session:
        persisted = session.query("MATCH (n:Person) RETURN keys(n) AS keys")

    assert persisted.rows == ({"keys": ["name"]},)


def test_match_remove_deletes_relationship_property(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS {temp: true, since: 2024}]->"
            "(b:Person {name: 'Bob'}) RETURN a"
        )
        result = session.query(
            "MATCH (:Person)-[r:KNOWS]->(:Person) REMOVE r.temp RETURN r.temp, r.since"
        )

    assert result.rows == ({"r.temp": None, "r.since": 2024},)

    with database.read_transaction() as session:
        persisted = session.query("MATCH (:Person)-[r:KNOWS]->(:Person) RETURN keys(r) AS keys")

    assert persisted.rows == ({"keys": ["since"]},)


def test_match_remove_label_deletes_node_label(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (n:Person:Temp {name: 'Alice'}) RETURN n")
        result = session.query("MATCH (n:Temp) REMOVE n:Temp RETURN labels(n) AS labels")

    assert result.rows == ({"labels": ["Person"]},)

    with database.read_transaction() as session:
        persisted = session.query("MATCH (n) RETURN labels(n) AS labels, n.name")

    assert persisted.rows == ({"labels": ["Person"], "n.name": "Alice"},)


def test_read_transaction_rejects_remove_queries(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (n:Person {name: 'Alice', temp: true}) RETURN n")

    with database.read_transaction() as session:
        with pytest.raises(TransactionError, match="write transaction"):
            session.query("MATCH (n:Person) REMOVE n.temp RETURN n")


def test_match_delete_removes_relationship_and_returns_deleted_count(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a"
        )
        result = session.query("MATCH ()-[r:KNOWS]->() DELETE r RETURN count(r) as count")

    assert result.columns == ("count",)
    assert result.rows == ({"count": 1},)

    with database.read_transaction() as session:
        persisted = session.query("MATCH ()-[r]->() RETURN count(r) as count")

    assert persisted.rows == ({"count": 0},)


def test_match_delete_removes_isolated_node(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (n:Person {name: 'Alice'}) RETURN n")
        result = session.query("MATCH (n:Person) DELETE n RETURN n.name")

    assert result.columns == ("n.name",)
    assert result.rows == ({"n.name": "Alice"},)

    with database.read_transaction() as session:
        persisted = session.query("MATCH (n:Person) RETURN count(n) as count")

    assert persisted.rows == ({"count": 0},)


def test_match_delete_rejects_node_with_existing_relationships(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a"
        )
        with pytest.raises(QueryError, match="Delete relationships first"):
            session.query("MATCH (n:Person {name: 'Alice'}) DELETE n RETURN n.name")


def test_match_detach_delete_removes_node_and_incident_relationships(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a"
        )
        result = session.query("MATCH (n:Person {name: 'Alice'}) DETACH DELETE n RETURN n.name")

    assert result.columns == ("n.name",)
    assert result.rows == ({"n.name": "Alice"},)

    with database.read_transaction() as session:
        node_count = session.query("MATCH (n:Person) RETURN count(n) as count")
        edge_count = session.query("MATCH ()-[r]->() RETURN count(r) as count")

    assert node_count.rows == ({"count": 1},)
    assert edge_count.rows == ({"count": 0},)


def test_match_detach_delete_supports_multiple_node_targets(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a"
        )
        session.query(
            "CREATE (c:Person {name: 'Carol'})-[:KNOWS]->(d:Person {name: 'Dave'}) RETURN c"
        )
        result = session.query(
            "MATCH (n:Person) WHERE n.name IN ['Alice', 'Bob'] "
            "DETACH DELETE n RETURN n.name ORDER BY n.name ASC"
        )

    assert result.rows == ({"n.name": "Alice"}, {"n.name": "Bob"})

    with database.read_transaction() as session:
        names = session.query("MATCH (n:Person) RETURN n.name ORDER BY n.name ASC")
        edge_count = session.query("MATCH ()-[r]->() RETURN count(r) as count")

    assert names.rows == ({"n.name": "Carol"}, {"n.name": "Dave"})
    assert edge_count.rows == ({"count": 1},)


def test_match_detach_delete_can_mix_node_and_relationship_targets(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        alice = session.add_node(labels=["Person"], properties={"name": "Alice"})
        bob = session.add_node(labels=["Person"], properties={"name": "Bob"})
        carol = session.add_node(labels=["Person"], properties={"name": "Carol"})
        session.add_edge(src=alice["id"], rel_type="KNOWS", dst=bob["id"], properties={})
        session.add_edge(src=bob["id"], rel_type="LIKES", dst=carol["id"], properties={})
        result = session.query(
            "MATCH (b:Person {name: 'Bob'})-[r:LIKES]->(c:Person) "
            "DETACH DELETE b, r RETURN b.name, type(r)"
        )

    assert result.rows == ({"b.name": "Bob", "type(r)": "LIKES"},)

    with database.read_transaction() as session:
        node_names = session.query("MATCH (n:Person) RETURN n.name ORDER BY n.name ASC")
        edge_types = session.query("MATCH ()-[r]->() RETURN type(r) ORDER BY type(r) ASC")

    assert node_names.rows == ({"n.name": "Alice"}, {"n.name": "Carol"})
    assert edge_types.rows == ()


def test_read_transaction_rejects_delete_queries(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (n:Person {name: 'Alice'}) RETURN n")

    with database.read_transaction() as session:
        with pytest.raises(TransactionError, match="write transaction"):
            session.query("MATCH (n:Person) DELETE n RETURN n")


def test_optional_match_returns_null_row_when_pattern_has_no_match(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.read_transaction() as session:
        result = session.query("OPTIONAL MATCH (n:Person) RETURN n")

    assert result.rows == ({"n": None},)


def test_optional_match_returns_rows_when_pattern_matches(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (n:Person {name: 'Alice'}) RETURN n")

    with database.read_transaction() as session:
        result = session.query("OPTIONAL MATCH (n:Person) RETURN n")

    assert result.rows == ({"n": {"id": 1, "labels": ["Person"], "properties": {"name": "Alice"}}},)


def test_match_optional_match_preserves_row_when_optional_pattern_is_missing(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (n:Person {name: 'Alice'}) RETURN n")

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person) OPTIONAL MATCH (n)-[r:KNOWS]->(m:Person) "
            "RETURN n.name, type(r), m ORDER BY n.name ASC"
        )

    assert result.rows == ({"n.name": "Alice", "type(r)": None, "m": None},)


def test_match_optional_match_preserves_row_when_optional_where_filters_match(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a"
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person {name: 'Alice'}) "
            "OPTIONAL MATCH (n)-[r:KNOWS]->(m:Person) WHERE m.name = 'Carol' "
            "RETURN n.name, type(r), m"
        )

    assert result.rows == ({"n.name": "Alice", "type(r)": None, "m": None},)


def test_match_optional_match_returns_optional_bindings_when_present(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a"
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person {name: 'Alice'}) OPTIONAL MATCH (n)-[r:KNOWS]->(m:Person) "
            "RETURN n.name, type(r), m.name"
        )

    assert result.rows == ({"n.name": "Alice", "type(r)": "KNOWS", "m.name": "Bob"},)


def test_unwind_supports_literal_lists(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.read_transaction() as session:
        result = session.query("UNWIND [1, 2, 3] AS value RETURN value ORDER BY value DESC")

    assert result.rows == (
        {"value": 3},
        {"value": 2},
        {"value": 1},
    )


def test_unwind_does_not_load_graph_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = Database(tmp_path / "example-db")

    def fail_load_state() -> object:
        raise AssertionError("UNWIND should not load graph state")

    monkeypatch.setattr(database._app.use_cases.graph, "load_state", fail_load_state)

    with database.read_transaction() as session:
        result = session.query("UNWIND [1, 2] AS value RETURN value ORDER BY value ASC")

    assert result.rows == ({"value": 1}, {"value": 2})


def test_unwind_supports_list_parameters(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.read_transaction() as session:
        result = session.query(
            "UNWIND $values AS value RETURN value ORDER BY value ASC",
            {"values": ["Alice", "Bob"]},
        )

    assert result.rows == (
        {"value": "Alice"},
        {"value": "Bob"},
    )


def test_match_with_unwind_supports_literal_lists(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (n:Person {name: 'Alice'}) RETURN n")

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person) WITH n UNWIND ['x', 'y'] AS tag RETURN n.name, tag ORDER BY tag ASC"
        )

    assert result.rows == (
        {"n.name": "Alice", "tag": "x"},
        {"n.name": "Alice", "tag": "y"},
    )


def test_match_with_unwind_supports_list_properties(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (n:Person {name: 'Alice', tags: ['ml', 'graph']}) RETURN n")

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person) WITH n UNWIND n.tags AS tag RETURN n.name, tag ORDER BY tag ASC"
        )

    assert result.rows == (
        {"n.name": "Alice", "tag": "graph"},
        {"n.name": "Alice", "tag": "ml"},
    )


def test_optional_match_projects_null_labels_for_missing_node_binding(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.read_transaction() as session:
        result = session.query("OPTIONAL MATCH (n:Person) RETURN labels(n)")

    assert result.rows == ({"labels(n)": None},)


def test_optional_match_with_pipeline_returns_null_entity_row(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.read_transaction() as session:
        result = session.query("OPTIONAL MATCH (n:Person) WITH n RETURN n")

    assert result.rows == ({"n": None},)


def test_match_with_optional_match_pipeline_preserves_with_row(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (n:Person {name: 'Alice'}) RETURN n")

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person) WITH n OPTIONAL MATCH (n)-[r:KNOWS]->(m:Person) "
            "RETURN n.name, type(r), m.name"
        )

    assert result.rows == ({"n.name": "Alice", "type(r)": None, "m.name": None},)


def test_match_with_optional_match_pipeline_returns_bindings_when_present(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a"
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person {name: 'Alice'}) WITH n "
            "OPTIONAL MATCH (n)-[r:KNOWS]->(m:Person) "
            "RETURN n.name, type(r), m.name"
        )

    assert result.rows == ({"n.name": "Alice", "type(r)": "KNOWS", "m.name": "Bob"},)


def test_match_rejects_open_ended_variable_length_relationships(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.read_transaction() as session:
        with pytest.raises(QueryError, match="Only exact or fully bounded variable-length"):
            session.query("MATCH (a)-[:KNOWS*..3]->(b) RETURN a, b")
        with pytest.raises(QueryError, match="Only exact or fully bounded variable-length"):
            session.query("MATCH (a)-[:KNOWS*1..]->(b) RETURN a, b")


def test_match_rejects_variable_length_relationships_above_five_hops(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.read_transaction() as session:
        with pytest.raises(QueryError, match="limited to at most 5 hops"):
            session.query("MATCH (a)-[:KNOWS*1..6]->(b) RETURN a, b")


def test_match_supports_in_predicates_with_literal_lists(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "Alice"})
        session.add_node(labels=["Person"], properties={"name": "Bob"})
        session.add_node(labels=["Person"], properties={"name": "Carol"})

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person) WHERE n.name IN ['Alice', 'Carol'] RETURN n.name ORDER BY n.name ASC"
        )

    assert result.rows == (
        {"n.name": "Alice"},
        {"n.name": "Carol"},
    )


def test_match_supports_in_predicates_with_parameter_lists(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "Alice"})
        session.add_node(labels=["Person"], properties={"name": "Bob"})
        session.add_node(labels=["Person"], properties={"name": "Carol"})

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person) WHERE n.name IN $names RETURN n.name ORDER BY n.name ASC",
            {"names": ["Bob", "Carol"]},
        )

    assert result.rows == (
        {"n.name": "Bob"},
        {"n.name": "Carol"},
    )


def test_match_supports_is_null_and_is_not_null(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "Alice"})
        session.add_node(labels=["Person"], properties={"age": 30})

    with database.read_transaction() as session:
        unnamed = session.query("MATCH (n:Person) WHERE n.name IS NULL RETURN n.age")
        named = session.query("MATCH (n:Person) WHERE n.name IS NOT NULL RETURN n.name")

    assert unnamed.rows == ({"n.age": 30},)
    assert named.rows == ({"n.name": "Alice"},)


def test_match_supports_or_conditions(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "Alice", "age": 30})
        session.add_node(labels=["Person"], properties={"name": "Bob", "age": 40})
        session.add_node(labels=["Person"], properties={"name": "Carol", "age": 50})

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person) WHERE n.age = 30 OR n.age = 50 RETURN n.name ORDER BY n.name ASC"
        )

    assert result.rows == (
        {"n.name": "Alice"},
        {"n.name": "Carol"},
    )


def test_match_supports_contains_string_predicates(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "Alice"})
        session.add_node(labels=["Person"], properties={"name": "Alicia"})
        session.add_node(labels=["Person"], properties={"name": "Bob"})

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person) WHERE n.name CONTAINS 'lic' RETURN n.name ORDER BY n.name ASC"
        )

    assert result.rows == (
        {"n.name": "Alice"},
        {"n.name": "Alicia"},
    )


def test_match_supports_starts_with_string_predicates(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "Alice"})
        session.add_node(labels=["Person"], properties={"name": "Alicia"})
        session.add_node(labels=["Person"], properties={"name": "Bob"})

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person) WHERE n.name STARTS WITH 'Ali' RETURN n.name ORDER BY n.name ASC"
        )

    assert result.rows == (
        {"n.name": "Alice"},
        {"n.name": "Alicia"},
    )


def test_match_supports_ends_with_string_predicates_and_parameters(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "Alice"})
        session.add_node(labels=["Person"], properties={"name": "Charlie"})
        session.add_node(labels=["Person"], properties={"name": "Bob"})

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person) WHERE n.name ENDS WITH $suffix RETURN n.name ORDER BY n.name ASC",
            {"suffix": "ie"},
        )

    assert result.rows == ({"n.name": "Charlie"},)


def test_match_supports_tolower_in_return_where_and_order_by(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "Alice"})
        session.add_node(labels=["Person"], properties={"name": "BOB"})

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person) WHERE toLower(n.name) CONTAINS 'o' "
            "RETURN toLower(n.name) as lowered ORDER BY toLower(n.name) ASC"
        )

    assert result.rows == ({"lowered": "bob"},)


def test_match_supports_toupper_in_return_and_order_by(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "alice"})
        session.add_node(labels=["Person"], properties={"name": "Bob"})

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person) RETURN toUpper(n.name) as upper_name ORDER BY toUpper(n.name) DESC"
        )

    assert result.rows == (
        {"upper_name": "BOB"},
        {"upper_name": "ALICE"},
    )


def test_match_with_supports_tolower_and_toupper_on_scalar_aliases(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "Alice"})
        session.add_node(labels=["Person"], properties={"name": "Bob"})

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person) WITH n.name AS name "
            "RETURN toLower(name) as lower_name, toUpper(name) as upper_name "
            "ORDER BY toLower(name) ASC"
        )

    assert result.rows == (
        {"lower_name": "alice", "upper_name": "ALICE"},
        {"lower_name": "bob", "upper_name": "BOB"},
    )


def test_match_supports_size_on_string_property(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "Al"})
        session.add_node(labels=["Person"], properties={"name": "Alice"})
        session.add_node(labels=["Person"], properties={"name": "Roberto"})

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person) WHERE size(n.name) >= 5 "
            "RETURN n.name, size(n.name) as name_size "
            "ORDER BY size(n.name) DESC, n.name ASC"
        )

    assert result.rows == (
        {"n.name": "Roberto", "name_size": 7},
        {"n.name": "Alice", "name_size": 5},
    )


def test_match_supports_size_on_list_property(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (n:Person {name: 'Alice', tags: ['ml', 'graph']}) RETURN n")
        session.query("CREATE (n:Person {name: 'Bob', tags: ['db']}) RETURN n")

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person) WHERE size(n.tags) > 1 "
            "RETURN n.name, size(n.tags) as tag_count "
            "ORDER BY size(n.tags) DESC"
        )

    assert result.rows == ({"n.name": "Alice", "tag_count": 2},)


def test_match_with_supports_size_on_scalar_alias(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "Alice"})
        session.add_node(labels=["Person"], properties={"name": "Bob"})

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person) WITH n.name AS name "
            "RETURN name, size(name) as name_size "
            "ORDER BY size(name) DESC, name ASC"
        )

    assert result.rows == (
        {"name": "Alice", "name_size": 5},
        {"name": "Bob", "name_size": 3},
    )


def test_match_supports_id_function_in_return_and_order_by(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "Bob"})
        session.add_node(labels=["Person"], properties={"name": "Alice"})

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person) RETURN id(n) as node_id, n.name ORDER BY id(n) DESC"
        )

    assert result.columns == ("node_id", "n.name")
    assert result.rows == (
        {"node_id": 2, "n.name": "Alice"},
        {"node_id": 1, "n.name": "Bob"},
    )


def test_match_supports_labels_function_in_return(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person", "Employee"], properties={"name": "Alice"})

    with database.read_transaction() as session:
        result = session.query("MATCH (n) RETURN labels(n) as labels")

    assert result.columns == ("labels",)
    assert result.rows == ({"labels": ["Person", "Employee"]},)


def test_match_supports_keys_function_for_lab_internal_node_variable(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(
            labels=["Person"],
            properties={"age": 42, "name": "Alice"},
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (APP_INTERNAL_EXEC_VAR:Person) "
            "RETURN keys(APP_INTERNAL_EXEC_VAR) AS property_keys"
        )

    assert result.columns == ("property_keys",)
    assert result.rows == ({"property_keys": ["age", "name"]},)


def test_match_with_entity_alias_supports_keys_function_for_lab_schema_query(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Product"], properties={"id": "prod_1", "name": "Widget"})

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Product) "
            "WITH n AS APP_INTERNAL_EXEC_VAR "
            "RETURN keys(APP_INTERNAL_EXEC_VAR) AS property_keys"
        )

    assert result.rows == ({"property_keys": ["id", "name"]},)


def test_match_supports_keys_function_for_relationships(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        alice = session.add_node(labels=["Person"], properties={"name": "Alice"})
        bob = session.add_node(labels=["Person"], properties={"name": "Bob"})
        session.add_edge(
            src=alice["id"],
            rel_type="KNOWS",
            dst=bob["id"],
            properties={"since": 2024, "weight": 0.9},
        )

    with database.read_transaction() as session:
        result = session.query("MATCH ()-[r:KNOWS]->() RETURN keys(r) AS property_keys")

    assert result.rows == ({"property_keys": ["since", "weight"]},)


def test_match_supports_type_function_in_return_and_order_by(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        alice = session.add_node(labels=["Person"], properties={"name": "Alice"})
        bob = session.add_node(labels=["Person"], properties={"name": "Bob"})
        carol = session.add_node(labels=["Person"], properties={"name": "Carol"})
        session.add_edge(src=alice["id"], rel_type="LIKES", dst=bob["id"], properties={})
        session.add_edge(src=alice["id"], rel_type="KNOWS", dst=carol["id"], properties={})

    with database.read_transaction() as session:
        result = session.query("MATCH ()-[r]->() RETURN type(r) as rel_type ORDER BY type(r) ASC")

    assert result.columns == ("rel_type",)
    assert result.rows == (
        {"rel_type": "KNOWS"},
        {"rel_type": "LIKES"},
    )


def test_match_supports_id_function_in_where(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "Alice"})
        session.add_node(labels=["Person"], properties={"name": "Bob"})

    with database.read_transaction() as session:
        result = session.query("MATCH (n:Person) WHERE id(n) = 2 RETURN n.name")

    assert result.rows == ({"n.name": "Bob"},)


def test_match_supports_type_function_in_where_and_string_predicates(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        alice = session.add_node(labels=["Person"], properties={"name": "Alice"})
        bob = session.add_node(labels=["Person"], properties={"name": "Bob"})
        carol = session.add_node(labels=["Person"], properties={"name": "Carol"})
        session.add_edge(src=alice["id"], rel_type="LIKES", dst=bob["id"], properties={})
        session.add_edge(src=alice["id"], rel_type="KNOWS", dst=carol["id"], properties={})

    with database.read_transaction() as session:
        equals_result = session.query(
            "MATCH ()-[r]->() WHERE type(r) = 'KNOWS' RETURN count(r) as count"
        )
        starts_with_result = session.query(
            "MATCH ()-[r]->() WHERE type(r) STARTS WITH 'KN' RETURN count(r) as count"
        )

    assert equals_result.rows == ({"count": 1},)
    assert starts_with_result.rows == ({"count": 1},)


def test_match_supports_labels_function_in_where_with_parameter(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person", "Employee"], properties={"name": "Alice"})
        session.add_node(labels=["Person"], properties={"name": "Bob"})

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n) WHERE labels(n) = $labels RETURN n.name",
            {"labels": ["Person", "Employee"]},
        )

    assert result.rows == ({"n.name": "Alice"},)


def test_match_supports_label_predicates_in_where(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (:Character {name: 'Jon Snow'})-[:LOYAL_TO]->"
            "(:Allegiance {name: 'Night Watch'}) RETURN *"
        )
        session.query(
            "CREATE (:Character {name: 'Jon Snow'})-[:KILLED]->"
            "(:Character {name: 'Orell'}) RETURN *"
        )
        session.query(
            "CREATE (:Location {name: 'Castle Black'})-[:HAS]->"
            "(:Allegiance {name: 'Night Watch'}) RETURN *"
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n)-[e]->(m) "
            "WHERE n:Character AND (m:Character OR m:Allegiance) "
            "RETURN n, e, m"
        )

    assert len(result.rows) == 2
    assert {row["e"]["type"] for row in result.rows} == {"KILLED", "LOYAL_TO"}
    assert {row["n"]["properties"]["name"] for row in result.rows} == {"Jon Snow"}


def test_match_supports_exists_for_node_properties(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "Alice", "age": 30})
        session.add_node(labels=["Person"], properties={"name": "Bob"})

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person) WHERE EXISTS(n.age) RETURN n.name ORDER BY n.name ASC"
        )

    assert result.rows == ({"n.name": "Alice"},)


def test_match_supports_not_exists_for_node_properties(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "Alice", "age": 30})
        session.add_node(labels=["Person"], properties={"name": "Bob"})

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person) WHERE NOT EXISTS(n.age) RETURN n.name ORDER BY n.name ASC"
        )

    assert result.rows == ({"n.name": "Bob"},)


def test_match_supports_exists_for_relationship_properties(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        alice = session.add_node(labels=["Person"], properties={"name": "Alice"})
        bob = session.add_node(labels=["Person"], properties={"name": "Bob"})
        carol = session.add_node(labels=["Person"], properties={"name": "Carol"})
        session.add_edge(
            src=alice["id"],
            rel_type="KNOWS",
            dst=bob["id"],
            properties={"since": 2025},
        )
        session.add_edge(src=alice["id"], rel_type="KNOWS", dst=carol["id"], properties={})

    with database.read_transaction() as session:
        result = session.query(
            "MATCH ()-[r:KNOWS]->() WHERE EXISTS(r.since) RETURN count(r) as count"
        )

    assert result.rows == ({"count": 1},)


def test_match_supports_multiple_order_by_items(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "Alice", "age": 30})
        session.add_node(labels=["Person"], properties={"name": "Alice", "age": 25})
        session.add_node(labels=["Person"], properties={"name": "Bob", "age": 20})

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n:Person) RETURN n.name, n.age ORDER BY n.name ASC, n.age DESC"
        )

    assert result.rows == (
        {"n.name": "Alice", "n.age": 30},
        {"n.name": "Alice", "n.age": 25},
        {"n.name": "Bob", "n.age": 20},
    )


def test_match_supports_not_conditions(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "Alice"})
        session.add_node(labels=["Person"], properties={"name": "Bob"})

    with database.read_transaction() as session:
        result = session.query("MATCH (n:Person) WHERE NOT n.name = 'Alice' RETURN n.name")

    assert result.rows == ({"n.name": "Bob"},)


def test_match_supports_rebinding_the_same_node_variable(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        alice = session.add_node(labels=["Person"], properties={"name": "Alice"})
        bob = session.add_node(labels=["Person"], properties={"name": "Bob"})
        session.add_edge(src=alice["id"], rel_type="KNOWS", dst=bob["id"], properties={})
        session.add_edge(src=bob["id"], rel_type="KNOWS", dst=alice["id"], properties={})

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(a:Person) "
            "RETURN a.name, b.name ORDER BY a.name ASC"
        )

    assert result.columns == ("a.name", "b.name")
    assert result.rows == (
        {"a.name": "Alice", "b.name": "Bob"},
        {"a.name": "Bob", "b.name": "Alice"},
    )


def test_match_can_return_whole_nodes_from_arrow_backed_path_results(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, b"
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b ORDER BY a.name ASC"
        )

    assert result.columns == ("a", "b")
    assert result.rows == (
        {
            "a": {"id": 1, "labels": ["Person"], "properties": {"name": "Alice"}},
            "b": {"id": 2, "labels": ["Person"], "properties": {"name": "Bob"}},
        },
    )


def test_match_supports_return_distinct_for_projected_values(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "Alice"})
        session.add_node(labels=["Person"], properties={"name": "Alice"})
        session.add_node(labels=["Person"], properties={"name": "Bob"})

    with database.read_transaction() as session:
        result = session.query("MATCH (n:Person) RETURN DISTINCT n.name ORDER BY n.name ASC")

    assert result.columns == ("n.name",)
    assert result.rows == (
        {"n.name": "Alice"},
        {"n.name": "Bob"},
    )


def test_match_supports_count_star(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "Alice"})
        session.add_node(labels=["Person"], properties={"name": "Bob"})
        session.add_node(labels=["Company"], properties={"name": "Acme"})

    with database.read_transaction() as session:
        result = session.query("MATCH (n:Person) RETURN COUNT(*) AS total")

    assert result.columns == ("total",)
    assert result.rows == ({"total": 2},)


def test_match_supports_count_of_property_projection(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "Alice"})
        session.add_node(labels=["Person"], properties={"name": "Bob"})
        session.add_node(labels=["Person"], properties={"age": 30})

    with database.read_transaction() as session:
        result = session.query("MATCH (n:Person) RETURN COUNT(n.name) AS named_people")

    assert result.columns == ("named_people",)
    assert result.rows == ({"named_people": 2},)


def test_match_supports_count_distinct_of_property_projection(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "Alice"})
        session.add_node(labels=["Person"], properties={"name": "Alice"})
        session.add_node(labels=["Person"], properties={"name": "Bob"})
        session.add_node(labels=["Person"], properties={"age": 30})

    with database.read_transaction() as session:
        result = session.query("MATCH (n:Person) RETURN COUNT(DISTINCT n.name) AS unique_names")

    assert result.columns == ("unique_names",)
    assert result.rows == ({"unique_names": 2},)


def test_match_supports_count_distinct_of_nodes(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, b"
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person), (a:Person) "
            "RETURN COUNT(DISTINCT a) AS unique_people"
        )

    assert result.columns == ("unique_people",)
    assert result.rows == ({"unique_people": 1},)


def test_match_rejects_count_distinct_star_as_invalid_opencypher_syntax(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.read_transaction() as session:
        with pytest.raises(QueryError, match="openCypher syntax error"):
            session.query("MATCH (n) RETURN COUNT(DISTINCT *) AS total")


def test_match_supports_grouped_aggregate_return_by_property(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "Alice"})
        session.add_node(labels=["Person"], properties={"name": "Alice"})
        session.add_node(labels=["Person"], properties={"name": "Bob"})

    with database.read_transaction() as session:
        result = session.query("MATCH (n:Person) RETURN n.name, COUNT(n) ORDER BY COUNT(n) DESC")

    assert result.columns == ("n.name", "COUNT(n)")
    assert result.rows == (
        {"n.name": "Alice", "COUNT(n)": 2},
        {"n.name": "Bob", "COUNT(n)": 1},
    )


def test_match_supports_grouped_aggregate_return_by_related_property_alias(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (d:Death {name: 'A'})-[:HAPPENED_IN]->(e:Episode {name: 'Episode 1'}) RETURN d"
        )
        session.query(
            "CREATE (d:Death {name: 'B'})-[:HAPPENED_IN]->(e:Episode {name: 'Episode 1'}) RETURN d"
        )
        session.query(
            "CREATE (d:Death {name: 'C'})-[:HAPPENED_IN]->(e:Episode {name: 'Episode 2'}) RETURN d"
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (d:Death)-[:HAPPENED_IN]->(e:Episode) "
            "RETURN e.name AS episode_name, COUNT(d) AS kill_count "
            "ORDER BY kill_count DESC;"
        )

    assert result.columns == ("episode_name", "kill_count")
    assert result.rows == (
        {"episode_name": "Episode 1", "kill_count": 2},
        {"episode_name": "Episode 2", "kill_count": 1},
    )


def test_memgraph_got_location_deaths_query_supports_incoming_relationship(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (d:Death {order: 1})-[:HAPPENED_IN]->"
            "(l:Location {name: 'Castle Black'}) RETURN d"
        )
        session.query(
            "CREATE (d:Death {order: 2})-[:HAPPENED_IN]->"
            "(l:Location {name: 'Castle Black'}) RETURN d"
        )
        session.query(
            "CREATE (d:Death {order: 3})-[:HAPPENED_IN]->(l:Location {name: 'Winterfell'}) RETURN d"
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (l:Location)<-[:HAPPENED_IN]-(d:Death) "
            "RETURN l.name AS location_name, count(d) AS death_count "
            "ORDER BY death_count DESC"
        )

    assert result.rows == (
        {"location_name": "Castle Black", "death_count": 2},
        {"location_name": "Winterfell", "death_count": 1},
    )

    with database.read_transaction() as session:
        graph_result = session.query(
            "MATCH (l:Location)<-[h:HAPPENED_IN]-(d:Death) RETURN l, h, d LIMIT 1"
        )

    assert graph_result.columns == ("l", "h", "d")
    assert graph_result.rows[0]["l"]["labels"] == ["Location"]
    assert graph_result.rows[0]["h"]["type"] == "HAPPENED_IN"
    assert graph_result.rows[0]["d"]["labels"] == ["Death"]


def test_memgraph_got_season_deaths_query_orders_by_group_alias(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (d:Death {order: 1})-[:HAPPENED_IN]->(s:Season {number: 2}) RETURN d")
        session.query("CREATE (d:Death {order: 2})-[:HAPPENED_IN]->(s:Season {number: 1}) RETURN d")
        session.query("CREATE (d:Death {order: 3})-[:HAPPENED_IN]->(s:Season {number: 1}) RETURN d")

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (d:Death)-[:HAPPENED_IN]->(s:Season) "
            "RETURN s.number AS season_number, count(d) AS death_count "
            "ORDER BY season_number ASC"
        )

    assert result.rows == (
        {"season_number": 1, "death_count": 2},
        {"season_number": 2, "death_count": 1},
    )


def test_memgraph_got_victim_death_location_visual_query_supports_multi_match(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (jon:Character {name: 'Jon Snow'})-[:KILLED]->"
            "(victim:Character {name: 'Orell'}) RETURN jon"
        )
        session.query(
            "MATCH (jon:Character {name: 'Jon Snow'}), (victim:Character {name: 'Orell'}) "
            "CREATE (jon)-[:KILLER_IN]->(death:Death {order: 1}) RETURN death"
        )
        session.query(
            "MATCH (death:Death {order: 1}), (victim:Character {name: 'Orell'}) "
            "CREATE (victim)-[:VICTIM_IN]->(death) RETURN death"
        )
        session.query(
            "MATCH (death:Death {order: 1}) "
            "CREATE (death)-[:HAPPENED_IN]->(location:Location {name: 'Beyond the Wall'}) "
            "RETURN location"
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (jon:Character {name: 'Jon Snow'})-[:KILLED]->(victim:Character) "
            "MATCH (jon)-[:KILLER_IN]->(death:Death)<-[victim_to_death:VICTIM_IN]-(victim) "
            "MATCH (death)-[death_to_location:HAPPENED_IN]->(location:Location) "
            "RETURN victim,victim_to_death,death,death_to_location,location"
        )

    assert result.columns == (
        "victim",
        "victim_to_death",
        "death",
        "death_to_location",
        "location",
    )
    assert result.rows[0]["victim"]["properties"]["name"] == "Orell"
    assert result.rows[0]["victim_to_death"]["type"] == "VICTIM_IN"
    assert result.rows[0]["death"]["properties"]["order"] == 1
    assert result.rows[0]["death_to_location"]["type"] == "HAPPENED_IN"
    assert result.rows[0]["location"]["properties"]["name"] == "Beyond the Wall"


def test_memgraph_got_loyalties_visual_query_supports_undirected_relationship(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (character:Character {name: 'Arya Stark'})-"
            "[:LOYAL_TO]->(allegiance:Allegiance {name: 'House Stark'}) "
            "RETURN character"
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (character:Character)-[loyal_to:LOYAL_TO]-(allegiance) "
            "RETURN character, loyal_to, allegiance"
        )

    assert result.columns == ("character", "loyal_to", "allegiance")
    assert result.rows[0]["character"]["properties"]["name"] == "Arya Stark"
    assert result.rows[0]["loyal_to"]["type"] == "LOYAL_TO"
    assert result.rows[0]["allegiance"]["properties"]["name"] == "House Stark"


def test_memgraph_got_undirected_happened_in_matches_reverse_textual_direction(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (death:Death {order: 1})-[:HAPPENED_IN]->"
            "(episode:Episode {name: 'Battle of the Bastards'}) RETURN death"
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (death:Death)-[:HAPPENED_IN]-(episode:Episode) "
            "RETURN episode.name AS episode_name, count(death) AS death_count"
        )

    assert result.rows == ({"episode_name": "Battle of the Bastards", "death_count": 1},)


def test_memgraph_got_episode_ratings_supports_avg_and_round_expression(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (episode:Episode {name: 'Winter Is Coming', imdb_rating: 8.912})-"
            "[:PART_OF]->(season:Season {number: 1}) RETURN episode"
        )
        session.query(
            "CREATE (episode:Episode {name: 'The Kingsroad', imdb_rating: 8.321})-"
            "[:PART_OF]->(season:Season {number: 1}) RETURN episode"
        )
        session.query(
            "CREATE (episode:Episode {name: 'Blackwater', imdb_rating: 9.654})-"
            "[:PART_OF]->(season:Season {number: 2}) RETURN episode"
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (e:Episode)-[:PART_OF]->(s:Season) "
            "RETURN s.number AS season_name, "
            "round(100 * avg(e.imdb_rating))/100 AS rating "
            "ORDER BY rating DESC"
        )

    assert result.rows == (
        {"season_name": 2, "rating": 9.65},
        {"season_name": 1, "rating": 8.62},
    )


def test_memgraph_got_killed_relationship_supports_numeric_aggregates(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (killer:Character {name: 'Jon Snow'})-[:KILLED {count: 2}]->"
            "(:Character {name: 'Orell'}) RETURN killer"
        )
        session.query(
            "CREATE (killer:Character {name: 'Arya Stark'})-[:KILLED {count: 3}]->"
            "(:Character {name: 'Meryn Trant'}) RETURN killer"
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (:Character)-[k:KILLED]->(:Character) "
            "RETURN sum(k.count) AS total_kills, avg(k.count) AS avg_kills, "
            "min(k.count) AS min_kills, max(k.count) AS max_kills"
        )

    assert result.rows == (
        {
            "total_kills": 5,
            "avg_kills": 2.5,
            "min_kills": 2,
            "max_kills": 3,
        },
    )


def test_neo4j_recommendations_return_star_projects_visible_graph_bindings(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (u:User {name: 'Misty Williams'})-"
            "[:RATED {rating: 5}]->"
            "(m:Movie {title: 'The Matrix', year: 1999}) RETURN u"
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (u:User {name: 'Misty Williams'}) "
            "MATCH (u)-[r:RATED]->(m:Movie) "
            "RETURN * "
            "LIMIT 100"
        )

    assert result.columns == ("u", "r", "m")
    assert result.rows[0]["u"]["properties"]["name"] == "Misty Williams"
    assert result.rows[0]["r"]["type"] == "RATED"
    assert result.rows[0]["r"]["properties"]["rating"] == 5
    assert result.rows[0]["m"]["properties"]["title"] == "The Matrix"


def test_neo4j_recommendations_return_star_keeps_with_scalar_bindings(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (u:User {name: 'Misty Williams'})-"
            "[:RATED {rating: 5}]->"
            "(m:Movie {title: 'The Matrix'}) RETURN u"
        )
        session.query(
            "MATCH (u:User {name: 'Misty Williams'}) "
            "CREATE (u)-[:RATED {rating: 3}]->"
            "(speed:Movie {title: 'Speed'}) RETURN u"
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (u:User {name: 'Misty Williams'})-[r:RATED]->(m:Movie) "
            "WITH u, avg(r.rating) AS average "
            "MATCH (u)-[r:RATED]->(m:Movie) "
            "WHERE r.rating > average "
            "RETURN * "
            "LIMIT 100"
        )

    assert result.columns == ("u", "average", "r", "m")
    assert result.rows == (
        {
            "u": result.rows[0]["u"],
            "average": 4.0,
            "r": result.rows[0]["r"],
            "m": result.rows[0]["m"],
        },
    )
    assert result.rows[0]["u"]["properties"]["name"] == "Misty Williams"
    assert result.rows[0]["r"]["properties"]["rating"] == 5
    assert result.rows[0]["m"]["properties"]["title"] == "The Matrix"


def test_neo4j_recommendations_exists_pattern_filters_candidates(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (:User {name: 'Misty Williams'})-[:RATED {rating: 5}]->"
            "(:Movie {title: 'The Matrix'}) RETURN *"
        )
        session.query("CREATE (:Movie {title: 'Speed'}) RETURN *")

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (rec:Movie) "
            "WHERE EXISTS { (:User {name: 'Misty Williams'})-[:RATED]->(rec) } "
            "RETURN rec.title AS title"
        )

    assert result.rows == ({"title": "The Matrix"},)


def test_neo4j_recommendations_not_exists_pattern_excludes_seen_movies(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (misty:User {name: 'Misty Williams'})-[:RATED {rating: 5}]->"
            "(matrix:Movie {title: 'The Matrix'}) RETURN misty"
        )
        session.query(
            "MATCH (matrix:Movie {title: 'The Matrix'}) "
            "CREATE (other:User {name: 'Claudia Jones'})-[:RATED {rating: 5}]->"
            "(matrix) RETURN other"
        )
        session.query(
            "MATCH (other:User {name: 'Claudia Jones'}) "
            "CREATE (other)-[:RATED {rating: 4}]->"
            "(speed:Movie {title: 'Speed'}) RETURN speed"
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (u:User {name: 'Misty Williams'})-[:RATED]->(:Movie)<-[:RATED]-"
            "(other:User)-[:RATED]->(rec:Movie) "
            "WHERE NOT EXISTS { (u)-[:RATED]->(rec) } "
            "RETURN DISTINCT rec.title AS title "
            "ORDER BY title ASC"
        )

    assert result.rows == ({"title": "Speed"},)


def test_neo4j_recommendations_relationship_type_alternatives_match_content_edges(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (:Person {name: 'Keanu Reeves'})-[:ACTED_IN]->"
            "(:Movie {title: 'The Matrix'}) RETURN *"
        )
        session.query(
            "CREATE (:Person {name: 'Lana Wachowski'})-[:DIRECTED]->"
            "(:Movie {title: 'The Matrix'}) RETURN *"
        )
        session.query(
            "CREATE (:Person {name: 'Someone Else'})-[:RATED]->"
            "(:Movie {title: 'The Matrix'}) RETURN *"
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (p:Person)-[:ACTED_IN|DIRECTED]->(m:Movie) "
            "RETURN p.name AS person "
            "ORDER BY person ASC"
        )

    assert result.rows == (
        {"person": "Keanu Reeves"},
        {"person": "Lana Wachowski"},
    )


def test_neo4j_recommendations_relationship_type_alternatives_work_inside_not_exists(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (:User {name: 'Misty Williams'})-[:RATED]->"
            "(:Movie {title: 'The Matrix'}) RETURN *"
        )
        session.query(
            "CREATE (:User {name: 'Misty Williams'})-[:WATCHED]->(:Movie {title: 'Speed'}) RETURN *"
        )
        session.query("CREATE (:Movie {title: 'John Wick'}) RETURN *")
        session.query(
            "MATCH (matrix:Movie {title: 'The Matrix'}), (john:Movie {title: 'John Wick'}) "
            "CREATE (john)-[:SIMILAR_TO]->(matrix) RETURN john"
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (u:User {name: 'Misty Williams'}) "
            "MATCH (u)-[:RATED]->(:Movie)<-[:SIMILAR_TO]-(rec:Movie) "
            "WHERE NOT EXISTS { (u)-[:RATED|WATCHED]->(rec) } "
            "RETURN rec.title AS title "
            "ORDER BY title ASC"
        )

    assert result.rows == ({"title": "John Wick"},)


def test_neo4j_recommendations_collect_groups_movie_genres(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (:Movie {title: 'The Matrix'})-[:IN_GENRE]->(:Genre {name: 'Action'}) RETURN *"
        )
        session.query(
            "MATCH (m:Movie {title: 'The Matrix'}) "
            "CREATE (m)-[:IN_GENRE]->(genre:Genre {name: 'Sci-Fi'}) RETURN m"
        )
        session.query(
            "CREATE (:Movie {title: 'Speed'})-[:IN_GENRE]->(:Genre {name: 'Action'}) RETURN *"
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (m:Movie)-[:IN_GENRE]->(g:Genre) "
            "RETURN m.title AS movie, collect(g.name) AS genres "
            "ORDER BY movie ASC"
        )

    assert result.rows[0] == {"movie": "Speed", "genres": ["Action"]}
    assert result.rows[1]["movie"] == "The Matrix"
    assert set(result.rows[1]["genres"]) == {"Action", "Sci-Fi"}


def test_neo4j_recommendations_collect_distinct_deduplicates_values(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (:Movie {title: 'The Matrix'})-[:IN_GENRE]->(:Genre {name: 'Action'}) RETURN *"
        )
        session.query(
            "MATCH (m:Movie {title: 'The Matrix'}) "
            "CREATE (m)-[:TAGGED_AS]->(genre:Genre {name: 'Action'}) RETURN m"
        )
        session.query(
            "MATCH (m:Movie {title: 'The Matrix'}) "
            "CREATE (m)-[:IN_GENRE]->(genre:Genre {name: 'Sci-Fi'}) RETURN m"
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (m:Movie)-[:IN_GENRE|TAGGED_AS]->(g:Genre) "
            "RETURN m.title AS movie, collect(DISTINCT g.name) AS genres"
        )

    assert result.rows[0]["movie"] == "The Matrix"
    assert set(result.rows[0]["genres"]) == {"Action", "Sci-Fi"}


def test_neo4j_recommendations_collect_result_can_be_unwound_after_with(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (:Movie {title: 'The Matrix'})-[:IN_GENRE]->(:Genre {name: 'Action'}) RETURN *"
        )
        session.query(
            "MATCH (m:Movie {title: 'The Matrix'}) "
            "CREATE (m)-[:IN_GENRE]->(genre:Genre {name: 'Sci-Fi'}) RETURN m"
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (m:Movie)-[:IN_GENRE]->(g:Genre) "
            "WITH collect(g.name) AS genres "
            "UNWIND genres AS genre "
            "RETURN genre "
            "ORDER BY genre ASC"
        )

    assert result.rows == ({"genre": "Action"}, {"genre": "Sci-Fi"})


def test_neo4j_recommendations_size_of_collected_list_in_return(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (:Movie {title: 'The Matrix'})-[:IN_GENRE]->(:Genre {name: 'Action'}) RETURN *"
        )
        session.query(
            "MATCH (m:Movie {title: 'The Matrix'}) "
            "CREATE (m)-[:IN_GENRE]->(genre:Genre {name: 'Sci-Fi'}) RETURN m"
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (m:Movie)-[:IN_GENRE]->(g:Genre) "
            "WITH m.title AS movie, collect(g.name) AS genres "
            "RETURN movie, size(genres) AS genre_count"
        )

    assert result.rows == ({"movie": "The Matrix", "genre_count": 2},)


def test_neo4j_recommendations_abs_arithmetic_filter_for_rating_similarity(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (:User {name: 'Misty Williams'})-[:RATED {rating: 5}]->"
            "(:Movie {title: 'The Matrix'}) RETURN *"
        )
        session.query(
            "MATCH (m:Movie {title: 'The Matrix'}) "
            "CREATE (other:User {name: 'Claudia Jones'})-[:RATED {rating: 4}]->(m) RETURN m"
        )
        session.query(
            "MATCH (m:Movie {title: 'The Matrix'}) "
            "CREATE (other:User {name: 'Distant User'})-[:RATED {rating: 1}]->(m) RETURN m"
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (misty:User {name: 'Misty Williams'})-[r1:RATED]->"
            "(m:Movie)<-[r2:RATED]-(other:User) "
            "WHERE abs(r1.rating-r2.rating) < 2 AND other.name <> 'Misty Williams' "
            "RETURN other.name AS user "
            "ORDER BY user ASC"
        )

    assert result.rows == ({"user": "Claudia Jones"},)


def test_neo4j_recommendations_arithmetic_projection_returns_numeric_score(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (:User {name: 'Misty Williams'})-[:RATED {rating: 5}]->"
            "(:Movie {title: 'The Matrix'}) RETURN *"
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (:User {name: 'Misty Williams'})-[r:RATED]->(m:Movie) "
            "RETURN m.title AS movie, r.rating * 2 + 1 AS score"
        )

    assert result.rows == ({"movie": "The Matrix", "score": 11},)


def test_multi_match_supports_where_on_later_match_binding(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (jon:Character {name: 'Jon Snow'})-[:KILLED]->"
            "(orell:Character {name: 'Orell'}) RETURN jon"
        )
        session.query(
            "MATCH (jon:Character {name: 'Jon Snow'}), (orell:Character {name: 'Orell'}) "
            "CREATE (jon)-[:KILLER_IN]->(death:Death {order: 1}) RETURN death"
        )
        session.query(
            "MATCH (death:Death {order: 1}), (orell:Character {name: 'Orell'}) "
            "CREATE (orell)-[:VICTIM_IN]->(death) RETURN death"
        )
        session.query(
            "MATCH (death:Death {order: 1}) "
            "CREATE (death)-[:HAPPENED_IN]->(location:Location {name: 'Beyond the Wall'}) "
            "RETURN location"
        )
        session.query(
            "MATCH (jon:Character {name: 'Jon Snow'}) "
            "CREATE (jon)-[:KILLED]->(victim:Character {name: 'Qhorin Halfhand'}) RETURN victim"
        )
        session.query(
            "MATCH (jon:Character {name: 'Jon Snow'}), "
            "(victim:Character {name: 'Qhorin Halfhand'}) "
            "CREATE (jon)-[:KILLER_IN]->(death:Death {order: 2}) RETURN death"
        )
        session.query(
            "MATCH (death:Death {order: 2}), (victim:Character {name: 'Qhorin Halfhand'}) "
            "CREATE (victim)-[:VICTIM_IN]->(death) RETURN death"
        )
        session.query(
            "MATCH (death:Death {order: 2}) "
            "CREATE (death)-[:HAPPENED_IN]->(location:Location {name: 'Frostfangs'}) "
            "RETURN location"
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (jon:Character {name: 'Jon Snow'})-[:KILLED]->(victim:Character) "
            "MATCH (jon)-[:KILLER_IN]->(death:Death)<-[victim_to_death:VICTIM_IN]-(victim) "
            "MATCH (death)-[death_to_location:HAPPENED_IN]->(location:Location) "
            "WHERE location.name = 'Beyond the Wall' "
            "RETURN victim,victim_to_death,death,death_to_location,location"
        )

    assert len(result.rows) == 1
    assert result.rows[0]["victim"]["properties"]["name"] == "Orell"
    assert result.rows[0]["location"]["properties"]["name"] == "Beyond the Wall"


def test_match_normalizes_legacy_backticked_schema_names(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        death = session.add_node(labels=["`Death`"], properties={"`name`": "A"})
        episode = session.add_node(labels=["`Episode`"], properties={"`name`": "Episode 1"})
        session.add_edge(
            src=death["id"],
            rel_type="`HAPPENED_IN`",
            dst=episode["id"],
            properties={},
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (d:Death)-[:HAPPENED_IN]->(e:Episode) "
            "RETURN e.name AS episode_name, COUNT(d) AS kill_count"
        )

    assert result.rows == ({"episode_name": "Episode 1", "kill_count": 1},)


def test_match_with_supports_grouped_aggregate_count_entity(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (d:Death {name: 'A'})-[:HAPPENED_IN]->(e:Episode {name: 'Episode 1'}) RETURN d"
        )
        session.query(
            "CREATE (d:Death {name: 'B'})-[:HAPPENED_IN]->(e:Episode {name: 'Episode 1'}) RETURN d"
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (d:Death)-[:HAPPENED_IN]->(e:Episode) "
            "WITH e.name AS episode_name, COUNT(d) AS kill_count "
            "RETURN episode_name, kill_count"
        )

    assert result.rows == ({"episode_name": "Episode 1", "kill_count": 2},)


def test_match_grouped_aggregate_treats_missing_group_property_as_null(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (n:Person {name: 'Alice'}) RETURN n")
        session.query("CREATE (n:Person {name: 'Bob'}) RETURN n")

    with database.read_transaction() as session:
        result = session.query("MATCH (n:Person) RETURN n.country AS country, COUNT(n) AS total")

    assert result.rows == ({"country": None, "total": 2},)


def test_match_grouped_aggregate_treats_missing_related_property_as_null(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (d:Death {name: 'A'})-[:HAPPENED_IN]->(e:Episode {title: 'Episode 1'}) RETURN d"
        )
        session.query(
            "CREATE (d:Death {name: 'B'})-[:HAPPENED_IN]->(e:Episode {title: 'Episode 2'}) RETURN d"
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (d:Death)-[:HAPPENED_IN]->(e:Episode) "
            "RETURN e.name AS episode_name, COUNT(d) AS kill_count"
        )

    assert result.rows == ({"episode_name": None, "kill_count": 2},)


def test_match_grouped_aggregate_over_empty_path_returns_no_rows(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (n:Person {name: 'Arya'}) RETURN n")

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (d:Death)-[:HAPPENED_IN]->(e:Episode) "
            "RETURN e.name AS episode_name, COUNT(d) AS kill_count "
            "ORDER BY kill_count DESC"
        )

    assert result.columns == ("episode_name", "kill_count")
    assert result.rows == ()


def test_match_supports_grouped_aggregate_return_by_labels(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "Alice"})
        session.add_node(labels=["Person"], properties={"name": "Bob"})
        session.add_node(labels=["Company"], properties={"name": "Acme"})

    with database.read_transaction() as session:
        result = session.query("MATCH (n) RETURN labels(n), count(n) ORDER BY count(n) DESC")

    assert result.columns == ("labels(n)", "count(n)")
    assert result.rows == (
        {"labels(n)": ["Person"], "count(n)": 2},
        {"labels(n)": ["Company"], "count(n)": 1},
    )


def test_match_supports_order_by_return_alias_for_labels(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "Alice"})
        session.add_node(labels=["Company"], properties={"name": "Acme"})

    with database.read_transaction() as session:
        result = session.query("MATCH (n) RETURN DISTINCT labels(n) AS labels ORDER BY labels")

    assert result.rows == (
        {"labels": ["Company"]},
        {"labels": ["Person"]},
    )


def test_match_supports_order_by_return_alias_for_property(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "Bob"})
        session.add_node(labels=["Person"], properties={"name": "Alice"})

    with database.read_transaction() as session:
        result = session.query("MATCH (n:Person) RETURN n.name AS name ORDER BY name")

    assert result.rows == ({"name": "Alice"}, {"name": "Bob"})


def test_match_supports_distinct_grouped_aggregate_return_by_labels(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "Alice"})
        session.add_node(labels=["Person"], properties={"name": "Bob"})
        session.add_node(labels=["Company"], properties={"name": "Acme"})

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (n) RETURN DISTINCT labels(n), count(n) ORDER BY count(n) DESC"
        )

    assert result.rows == (
        {"labels(n)": ["Person"], "count(n)": 2},
        {"labels(n)": ["Company"], "count(n)": 1},
    )


def test_match_supports_lab_schema_distinct_aggregate_with_keys(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Product"], properties={"id": "prod_1", "name": "Widget"})
        session.add_node(labels=["Product"], properties={"id": "prod_2", "name": "Gadget"})

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (APP_INTERNAL_EXEC_VAR:Product) "
            "RETURN DISTINCT "
            "labels(APP_INTERNAL_EXEC_VAR) AS labels, "
            "keys(APP_INTERNAL_EXEC_VAR) AS property_keys, "
            "count(APP_INTERNAL_EXEC_VAR) AS node_count "
            "ORDER BY node_count DESC"
        )

    assert result.columns == ("labels", "property_keys", "node_count")
    assert result.rows == (
        {"labels": ["Product"], "property_keys": ["id", "name"], "node_count": 2},
    )


def test_match_path_can_return_whole_nodes_with_list_properties(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        product = session.add_node(
            labels=["Product"],
            properties={"name": "Widget", "product_ids": ["prod_1", "prod_2"]},
        )
        category = session.add_node(labels=["Category"], properties={"name": "Tools"})
        session.add_edge(
            src=product["id"],
            rel_type="BELONGS_TO",
            dst=category["id"],
            properties={},
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (p:Product)-[r:BELONGS_TO]->(c:Category) RETURN p, r, c LIMIT 1"
        )

    assert result.rows == (
        {
            "p": {
                "id": 1,
                "labels": ["Product"],
                "properties": {"name": "Widget", "product_ids": ["prod_1", "prod_2"]},
            },
            "r": {
                "id": 1,
                "type": "BELONGS_TO",
                "source": 1,
                "target": 2,
                "properties": {},
            },
            "c": {
                "id": 2,
                "labels": ["Category"],
                "properties": {"name": "Tools"},
            },
        },
    )


def test_match_supports_connected_multiple_pattern_parts(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        alice = session.add_node(labels=["Person"], properties={"name": "Alice"})
        bob = session.add_node(labels=["Person"], properties={"name": "Bob"})
        acme = session.add_node(labels=["Company"], properties={"name": "Acme"})
        session.add_edge(src=alice["id"], rel_type="KNOWS", dst=bob["id"], properties={})
        session.add_edge(src=bob["id"], rel_type="WORKS_AT", dst=acme["id"], properties={})

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person), (b)-[:WORKS_AT]->(c:Company) "
            "RETURN a.name, b.name, c.name ORDER BY a.name ASC"
        )

    assert result.columns == ("a.name", "b.name", "c.name")
    assert result.rows == ({"a.name": "Alice", "b.name": "Bob", "c.name": "Acme"},)


def test_match_supports_connected_multiple_node_pattern_parts(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person", "Employee"], properties={"name": "Alice"})
        session.add_node(labels=["Person"], properties={"name": "Bob"})

    with database.read_transaction() as session:
        result = session.query("MATCH (a:Person), (a:Employee) RETURN a.name ORDER BY a.name ASC")

    assert result.columns == ("a.name",)
    assert result.rows == ({"a.name": "Alice"},)


def test_match_rejects_disconnected_multiple_pattern_parts(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "Alice"})
        session.add_node(labels=["Company"], properties={"name": "Acme"})

    with database.read_transaction() as session:
        with pytest.raises(QueryError, match="Disconnected pattern parts"):
            session.query("MATCH (a:Person), (c:Company) RETURN a.name, c.name")


def test_match_supports_disconnected_bounded_node_lookup_parts(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"id": "p1", "name": "Alice"})
        session.add_node(labels=["Company"], properties={"id": "c1", "name": "Acme"})

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (a:Person {id: 'p1'}), (c:Company {id: 'c1'}) RETURN a.name, c.name"
        )

    assert result.rows == ({"a.name": "Alice", "c.name": "Acme"},)


def test_match_create_can_create_an_edge_between_bound_nodes(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, b"
        )

    with database.write_transaction() as session:
        result = session.query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) CREATE (a)-[:LIKES]->(b) RETURN a.name, b.name"
        )

    assert result.columns == ("a.name", "b.name")
    assert result.rows == ({"a.name": "Alice", "b.name": "Bob"},)

    with database.read_transaction() as session:
        check = session.query("MATCH (a:Person)-[:LIKES]->(b:Person) RETURN a.name, b.name")

    assert check.rows == ({"a.name": "Alice", "b.name": "Bob"},)


def test_match_create_supports_disconnected_bounded_loader_lookup_parts(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"id": "p1", "name": "Alice"})
        session.add_node(labels=["Company"], properties={"id": "c1", "name": "Acme"})
        result = session.query(
            "MATCH (a:Person {id: 'p1'}), (c:Company {id: 'c1'}) CREATE (a)-[:WORKS_FOR]->(c)"
        )

    assert result.rows == ()

    with database.read_transaction() as session:
        check = session.query("MATCH (a:Person)-[:WORKS_FOR]->(c:Company) RETURN a.name, c.name")

    assert check.rows == ({"a.name": "Alice", "c.name": "Acme"},)


def test_match_can_filter_property_named_id_separately_from_internal_id(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (u:User {id: 'user_0001', name: 'Alice'}) RETURN u")

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (u:User {id: $user_id}) RETURN id(u) AS iid, u.id, u.name",
            {"user_id": "user_0001"},
        )

    assert result.rows == ({"iid": 1, "u.id": "user_0001", "u.name": "Alice"},)


def test_neo4j_components_procedure_is_available_for_bolt_clients(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.read_transaction() as session:
        result = session.query("CALL dbms.components()")

    assert result.columns == ("name", "versions", "edition")
    assert result.rows == (
        {"name": "Neo4j Kernel", "versions": ["5.26.0"], "edition": "community"},
    )


def test_neo4j_components_procedure_supports_discovery_projection(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.read_transaction() as session:
        result = session.query(
            "CALL dbms.components() "
            "YIELD name, versions, edition "
            "UNWIND versions AS version "
            "RETURN name, version, edition"
        )

    assert result.columns == ("name", "version", "edition")
    assert result.rows == ({"name": "Neo4j Kernel", "version": "5.26.0", "edition": "community"},)


def test_neo4j_ping_procedure_is_available_for_cypher_shell(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.read_transaction() as session:
        result = session.query("CALL db.ping()")

    assert result.columns == ()
    assert result.rows == ()


def test_schema_visualization_procedure_returns_label_relationship_summary(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (p:Product {id: 'p1'}) RETURN p")
        session.query(
            "MATCH (p:Product {id: 'p1'}) "
            "CREATE (p)-[:BELONGS_TO]->(c:Category {name: 'Books'}) "
            "RETURN c"
        )

    with database.read_transaction() as session:
        result = session.query("CALL db.schema.visualization()")

    assert result.columns == ("nodes", "relationships")
    assert result.rows == (
        {
            "nodes": [
                {"id": 0, "labels": ["Category"], "properties": {"name": "Category"}},
                {"id": 1, "labels": ["Product"], "properties": {"name": "Product"}},
            ],
            "relationships": [
                {
                    "id": 0,
                    "start": 1,
                    "end": 0,
                    "type": "BELONGS_TO",
                    "properties": {},
                },
            ],
        },
    )


def test_schema_visualization_procedure_supports_yield_return_projection(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.read_transaction() as session:
        result = session.query(
            "CALL db.schema.visualization() YIELD nodes, relationships RETURN nodes, relationships"
        )

    assert result.columns == ("nodes", "relationships")
    assert result.rows == ({"nodes": [], "relationships": []},)


def test_schema_metadata_procedures_support_graph_exploration_clients(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (p:Product {id: 'p1', price: 10.5})-"
            "[:BELONGS_TO {rank: 1}]->(c:Category {name: 'Books'}) "
            "RETURN p, c"
        )

    with database.read_transaction() as session:
        labels = session.query("CALL db.labels()")
        relationship_types = session.query("CALL db.relationshipTypes()")
        property_keys = session.query("CALL db.propertyKeys()")
        node_properties = session.query("CALL db.schema.nodeTypeProperties()")
        rel_properties = session.query("CALL db.schema.relTypeProperties()")

    assert labels.rows == ({"label": "Category"}, {"label": "Product"})
    assert relationship_types.rows == ({"relationshipType": "BELONGS_TO"},)
    assert property_keys.rows == (
        {"propertyKey": "id"},
        {"propertyKey": "name"},
        {"propertyKey": "price"},
        {"propertyKey": "rank"},
    )
    assert {(row["nodeLabels"][0], row["propertyName"]) for row in node_properties.rows} == {
        ("Category", "name"),
        ("Product", "id"),
        ("Product", "price"),
    }
    assert rel_properties.rows == (
        {
            "relType": ":`BELONGS_TO`",
            "propertyName": "rank",
            "propertyTypes": ["Integer"],
            "mandatory": True,
        },
    )


def test_memgraph_lab_show_metadata_statements_are_available(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (n:Person {name: 'Alice'}) RETURN n")

    with database.read_transaction() as session:
        storage = session.query("SHOW STORAGE INFO;")
        storage_current = session.query("SHOW STORAGE INFO ON CURRENT DATABASE")
        replication = session.query("SHOW REPLICATION ROLE")
        license_info = session.query("SHOW LICENSE INFO")
        indexes = session.query("SHOW INDEX INFO;")
        constraints = session.query("SHOW CONSTRAINT INFO")
        triggers = session.query("SHOW TRIGGERS")
        schema = session.query("SHOW SCHEMA INFO;")
        unknown = session.query("SHOW METRICS INFO")

    assert {"name": "vertex_count", "value": 1} in storage.rows
    assert {"name": "edge_count", "value": 0} in storage_current.rows
    assert replication.rows == ({"role": "main"},)
    assert license_info.rows == (
        {
            "license_type": "community",
            "organization_name": "GraphArrow",
            "valid_until": None,
        },
    )
    assert indexes.columns == ("index_type", "label", "property", "count", "identifier")
    assert indexes.rows == ()
    assert constraints.columns == ("constraint_type", "label", "properties")
    assert constraints.rows == ()
    assert triggers.columns == ("name", "event_type", "phase", "statement", "owner")
    assert triggers.rows == ()
    assert schema.columns == ("nodes", "relationships")
    assert schema.rows == ()
    assert unknown.columns == ("name", "value")
    assert unknown.rows == ()


def test_memgraph_bfs_syntax_reports_clear_unsupported_error(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (:Character {name: 'Jon Snow'})-[:KILLED]->"
            "(:Character {name: 'Orell'}) RETURN *"
        )

    with database.read_transaction() as session:
        with pytest.raises(QueryError, match=r"Memgraph BFS relationship expansion"):
            session.query(
                'MATCH path=(:Character { name: "Jon Snow" })-[:KILLED *bfs]->'
                "(:Character) RETURN path"
            )


def test_memgraph_lab_create_index_statement_is_noop_for_dataset_loader(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        result = session.query("CREATE INDEX ON :Death(character)")

    assert result.columns == ()
    assert result.rows == ()
    assert query_requires_write("CREATE INDEX ON :Death(character)") is True
    assert query_requires_write("MATCH (n:Death) REMOVE n.temp") is True


def test_memgraph_lab_drop_index_and_remove_bootstrap_markers(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (u:__mg_vertex__ {__mg_id__: 1, name: 'Arya'}) RETURN u")
        drop_result = session.query("DROP INDEX ON :__mg_vertex__(__mg_id__);")
        remove_result = session.query("MATCH (u) REMOVE u:__mg_vertex__, u.__mg_id__;")

    assert drop_result.rows == ()
    assert remove_result.rows == ()

    with database.read_transaction() as session:
        persisted = session.query("MATCH (u) RETURN labels(u) AS labels, keys(u) AS keys, u.name")

    assert persisted.rows == ({"labels": [], "keys": ["name"], "u.name": "Arya"},)


def test_unwind_with_create_supports_dataset_loader_shape(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        result = session.query(
            "UNWIND $rows AS row "
            "WITH row "
            "CREATE (c:Character {name: row.name, deaths: row.deaths}) "
            "RETURN count(c) AS created",
            {
                "rows": [
                    {"name": "Ned Stark", "deaths": 1},
                    {"name": "Robb Stark", "deaths": 1},
                ],
            },
        )

    assert result.rows == ({"created": 2},)

    with database.read_transaction() as session:
        check = session.query("MATCH (c:Character) RETURN c.name, c.deaths ORDER BY c.name")

    assert check.rows == (
        {"c.name": "Ned Stark", "c.deaths": 1},
        {"c.name": "Robb Stark", "c.deaths": 1},
    )


def test_unwind_with_create_allows_loader_shape_without_return(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        result = session.query(
            "UNWIND $rows AS row "
            "WITH row "
            "CREATE (d:Death {character: row.character, episode: row.episode})",
            {
                "rows": [
                    {"character": "Robert Baratheon", "episode": "S01E07"},
                    {"character": "Ned Stark", "episode": "S01E09"},
                ],
            },
        )

    assert result.columns == ()
    assert result.rows == ()

    with database.read_transaction() as session:
        check = session.query("MATCH (d:Death) RETURN d.character, d.episode ORDER BY d.character")

    assert check.rows == (
        {"d.character": "Ned Stark", "d.episode": "S01E09"},
        {"d.character": "Robert Baratheon", "d.episode": "S01E07"},
    )


def test_unwind_with_match_create_supports_pandora_like_relationship_loader(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "UNWIND $rows AS row "
            "WITH row "
            "CREATE (o:Officer {node_id: row.node_id, name: row.name})",
            {"rows": [{"node_id": "officer_1", "name": "Alice"}]},
        )
        session.query(
            "UNWIND $rows AS row WITH row CREATE (e:Entity {node_id: row.node_id, name: row.name})",
            {"rows": [{"node_id": "entity_1", "name": "Acme Offshore"}]},
        )
        result = session.query(
            "UNWIND $rows AS row "
            "WITH row "
            "MATCH (o:Officer {node_id: row.officer_id}), "
            "(e:Entity {node_id: row.entity_id}) "
            "CREATE (o)-[:OFFICER_OF {role: row.role}]->(e)",
            {
                "rows": [
                    {
                        "officer_id": "officer_1",
                        "entity_id": "entity_1",
                        "role": "beneficial owner",
                    },
                ],
            },
        )

    assert result.columns == ()
    assert result.rows == ()

    with database.read_transaction() as session:
        check = session.query(
            "MATCH (o:Officer)-[r:OFFICER_OF]->(e:Entity) RETURN o.name, r.role, e.name"
        )

    assert check.rows == (
        {
            "o.name": "Alice",
            "r.role": "beneficial owner",
            "e.name": "Acme Offshore",
        },
    )


def test_unwind_with_merge_supports_memgraph_lab_dataset_loader_shape(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")
    rows = [
        {"character": "Robert Baratheon", "episode": "S01E07"},
        {"character": "Ned Stark", "episode": "S01E09"},
        {"character": "Ned Stark", "episode": "S01E09"},
    ]

    with database.write_transaction() as session:
        result = session.query(
            "UNWIND $rows AS row "
            "WITH row "
            "MERGE (d:Death {character: row.character, episode: row.episode})",
            {"rows": rows},
        )

    assert result.columns == ()
    assert result.rows == ()

    with database.read_transaction() as session:
        check = session.query("MATCH (d:Death) RETURN d.character, d.episode ORDER BY d.character")

    assert check.rows == (
        {"d.character": "Ned Stark", "d.episode": "S01E09"},
        {"d.character": "Robert Baratheon", "d.episode": "S01E07"},
    )


def test_unwind_with_merge_supports_row_references_in_merge_actions(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "UNWIND $rows AS row "
            "WITH row "
            "MERGE (d:Death {character: row.character}) "
            "ON CREATE SET d.episode = row.episode "
            "ON MATCH SET d.seen = row.seen",
            {
                "rows": [
                    {"character": "Ned Stark", "episode": "S01E09", "seen": 1},
                    {"character": "Ned Stark", "episode": "S01E09", "seen": 2},
                ],
            },
        )

    with database.read_transaction() as session:
        check = session.query("MATCH (d:Death) RETURN d.character, d.episode, d.seen")

    assert check.rows == ({"d.character": "Ned Stark", "d.episode": "S01E09", "d.seen": 2},)


def test_unwind_with_match_set_supports_dataset_loader_update_shape(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (d:Death {character: 'Ned Stark'}) RETURN d")
        session.query(
            "UNWIND $rows AS row "
            "WITH row "
            "MATCH (d:Death {character: row.character}) "
            "SET d.episode = row.episode",
            {"rows": [{"character": "Ned Stark", "episode": "S01E09"}]},
        )

    with database.read_transaction() as session:
        check = session.query("MATCH (d:Death) RETURN d.character, d.episode")

    assert check.rows == ({"d.character": "Ned Stark", "d.episode": "S01E09"},)


def test_unwind_with_merge_set_row_supports_dataset_loader_update_shape(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "UNWIND $rows AS row WITH row MERGE (d:Death {character: row.character}) SET d += row",
            {
                "rows": [
                    {
                        "character": "Ned Stark",
                        "episode": "S01E09",
                        "method": "Execution",
                    },
                    {
                        "character": "Ned Stark",
                        "episode": "S01E09",
                        "method": "Execution",
                    },
                ],
            },
        )

    with database.read_transaction() as session:
        check = session.query("MATCH (d:Death) RETURN d.character, d.episode, d.method")

    assert check.rows == (
        {
            "d.character": "Ned Stark",
            "d.episode": "S01E09",
            "d.method": "Execution",
        },
    )


def test_match_with_detach_delete_supports_dataset_cleanup_shape(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (d:Death {character: 'Ned Stark'})-[:IN_EPISODE]->"
            "(e:Episode {id: 'S01E09'}) RETURN d, e"
        )
        result = session.query("MATCH (n) WITH n DETACH DELETE n")

    assert result.columns == ()
    assert result.rows == ()

    with database.read_transaction() as session:
        check = session.query("MATCH (n) RETURN count(n) AS count")

    assert check.rows == ({"count": 0},)


def test_match_update_queries_allow_write_only_loader_shapes(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (a:Person {name: 'Alice'}) RETURN a")
        create_result = session.query(
            "MATCH (a:Person {name: 'Alice'}) CREATE (a)-[:KNOWS]->(b:Person {name: 'Bob'})"
        )
        set_result = session.query("MATCH (b:Person {name: 'Bob'}) SET b.loaded = true")
        merge_result = session.query(
            "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) "
            "MERGE (a)-[:LIKES]->(b)"
        )
        delete_result = session.query("MATCH (b:Person {name: 'Bob'}) DETACH DELETE b")

    assert create_result.rows == ()
    assert set_result.rows == ()
    assert merge_result.rows == ()
    assert delete_result.rows == ()

    with database.read_transaction() as session:
        check = session.query("MATCH (n:Person) RETURN n.name")

    assert check.rows == ({"n.name": "Alice"},)


def test_unwind_with_match_merge_supports_pandora_like_relationship_loader(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "UNWIND $rows AS row "
            "WITH row "
            "CREATE (o:Officer {node_id: row.node_id, name: row.name})",
            {"rows": [{"node_id": "officer_1", "name": "Alice"}]},
        )
        session.query(
            "UNWIND $rows AS row WITH row CREATE (e:Entity {node_id: row.node_id, name: row.name})",
            {"rows": [{"node_id": "entity_1", "name": "Acme Offshore"}]},
        )
        session.query(
            "UNWIND $rows AS row "
            "WITH row "
            "MATCH (o:Officer {node_id: row.officer_id}), "
            "(e:Entity {node_id: row.entity_id}) "
            "MERGE (o)-[r:OFFICER_OF {role: row.role}]->(e)",
            {
                "rows": [
                    {
                        "officer_id": "officer_1",
                        "entity_id": "entity_1",
                        "role": "beneficial owner",
                    },
                    {
                        "officer_id": "officer_1",
                        "entity_id": "entity_1",
                        "role": "beneficial owner",
                    },
                ],
            },
        )

    with database.read_transaction() as session:
        check = session.query(
            "MATCH (o:Officer)-[r:OFFICER_OF]->(e:Entity) RETURN o.name, r.role, e.name"
        )

    assert check.rows == (
        {
            "o.name": "Alice",
            "r.role": "beneficial owner",
            "e.name": "Acme Offshore",
        },
    )


def test_match_with_create_supports_updating_clause_after_with(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (s:Source {name: 'Arya Stark'}) RETURN s")
        result = session.query(
            "MATCH (s:Source) WITH s.name AS name CREATE (c:Character {name: name}) RETURN c.name"
        )

    assert result.rows == ({"c.name": "Arya Stark"},)


def test_standalone_return_supports_connectivity_probes(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.read_transaction() as session:
        result = session.query("RETURN 1 AS ok")

    assert result.columns == ("ok",)
    assert result.rows == ({"ok": 1},)


def test_match_create_supports_multiple_create_patterns_per_row(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, b"
        )

    with database.write_transaction() as session:
        session.query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) "
            "CREATE (a)-[:LIKES]->(b), (b)-[:LIKES]->(a) "
            "RETURN a, b"
        )

    with database.read_transaction() as session:
        check = session.query(
            "MATCH (a:Person)-[:LIKES]->(b:Person) RETURN a.name, b.name ORDER BY a.name ASC"
        )

    assert check.rows == (
        {"a.name": "Alice", "b.name": "Bob"},
        {"a.name": "Bob", "b.name": "Alice"},
    )


def test_match_create_can_write_for_multiple_matched_rows(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, b"
        )
        session.query(
            "CREATE (a:Person {name: 'Bob'})-[:KNOWS]->(b:Person {name: 'Carol'}) RETURN a, b"
        )

    with database.write_transaction() as session:
        result = session.query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) "
            "CREATE (b)-[:FOLLOWS]->(a) "
            "RETURN a.name, b.name ORDER BY a.name ASC"
        )

    assert result.rows == (
        {"a.name": "Alice", "b.name": "Bob"},
        {"a.name": "Bob", "b.name": "Carol"},
    )

    with database.read_transaction() as session:
        check = session.query(
            "MATCH (a:Person)-[:FOLLOWS]->(b:Person) RETURN a.name, b.name ORDER BY a.name ASC"
        )

    assert check.rows == (
        {"a.name": "Bob", "b.name": "Alice"},
        {"a.name": "Carol", "b.name": "Bob"},
    )


def test_match_create_can_return_whole_nodes(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, b"
        )

    with database.write_transaction() as session:
        result = session.query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) CREATE (a)-[:LIKES]->(b) RETURN a, b"
        )

    assert result.columns == ("a", "b")
    assert result.rows == (
        {
            "a": {"id": 1, "labels": ["Person"], "properties": {"name": "Alice"}},
            "b": {"id": 2, "labels": ["Person"], "properties": {"name": "Bob"}},
        },
    )


def test_match_create_can_create_right_inline_node(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (a:Person {name: 'Alice'}) RETURN a")
        result = session.query(
            "MATCH (a:Person {name: 'Alice'}) "
            "CREATE (a)-[:KNOWS]->(b:Person {name: 'Bob'}) "
            "RETURN id(b) AS iid, b.name"
        )

    assert result.columns == ("iid", "b.name")
    assert result.rows == ({"iid": 2, "b.name": "Bob"},)

    with database.read_transaction() as session:
        check = session.query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")

    assert check.rows == ({"a.name": "Alice", "b.name": "Bob"},)


def test_match_create_can_create_left_inline_node(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (a:Person {name: 'Alice'}) RETURN a")
        result = session.query(
            "MATCH (a:Person {name: 'Alice'}) "
            "CREATE (b:Tag {name: 'vip'})-[:TAGGED]->(a) "
            "RETURN id(b) AS iid, b.name"
        )

    assert result.columns == ("iid", "b.name")
    assert result.rows == ({"iid": 2, "b.name": "vip"},)

    with database.read_transaction() as session:
        check = session.query("MATCH (b:Tag)-[:TAGGED]->(a:Person) RETURN b.name, a.name")

    assert check.rows == ({"b.name": "vip", "a.name": "Alice"},)


def test_match_create_rejects_unbound_variables(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (a:Person {name: 'Alice'}) RETURN a")
        with pytest.raises(QueryError, match="requires unbound nodes"):
            session.query("MATCH (a:Person) CREATE (a)-[:KNOWS]->(b) RETURN a")


def test_match_create_rejects_relationship_variables_and_ranges(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, b"
        )
        with pytest.raises(QueryError, match="Relationship variables are not supported yet."):
            session.query(
                "MATCH (a:Person)-[:KNOWS]->(b:Person) CREATE (a)-[r:LIKES]->(b) RETURN a"
            )
        with pytest.raises(
            QueryError,
            match="Variable-length relationships are not supported yet.",
        ):
            session.query(
                "MATCH (a:Person)-[:KNOWS]->(b:Person) CREATE (a)-[:LIKES*1..2]->(b) RETURN a"
            )


def test_read_transaction_rejects_match_create_queries(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, b"
        )

    with database.read_transaction() as session:
        with pytest.raises(TransactionError):
            session.query("MATCH (a:Person)-[:KNOWS]->(b:Person) CREATE (a)-[:LIKES]->(b) RETURN a")


def test_imperative_add_node_uses_the_same_storage_pipeline(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        created_node = session.add_node(
            labels=["Person", "Employee"],
            properties={"name": "Carol"},
        )

    assert created_node == {
        "id": 1,
        "labels": ["Person", "Employee"],
        "properties": {"name": "Carol"},
    }

    with database.read_transaction() as session:
        result = session.query("MATCH (n:Employee) RETURN n")

    assert result.rows[0]["n"]["properties"]["name"] == "Carol"


def test_read_transaction_rejects_create_queries(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.read_transaction() as session:
        with pytest.raises(TransactionError):
            session.query("CREATE (n:Person {name: 'Alice'}) RETURN n")


def test_invalid_cypher_syntax_raises_query_error(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.read_transaction() as session:
        with pytest.raises(QueryError, match="openCypher syntax error"):
            session.query("MATCH (n:Person RETURN n")


def test_valid_but_unsupported_cypher_feature_raises_query_error(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        with pytest.raises(
            QueryError,
            match="Only a single WITH clause is supported per multi-part query.",
        ):
            session.query("MATCH (n:Person) WITH n WITH n RETURN n")


def test_create_rejects_multi_hop_paths_for_now(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        with pytest.raises(QueryError, match="single relationship hop"):
            session.query(
                "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})"
                "-[:KNOWS]->(c:Person {name: 'Carol'}) RETURN a, b, c"
            )


def test_create_rejects_redefining_an_existing_create_variable(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        with pytest.raises(QueryError, match="cannot redefine variable 'a'"):
            session.query("CREATE (a:Person {name: 'Alice'}), (a:Person {name: 'Bob'}) RETURN a")
