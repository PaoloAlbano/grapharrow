from pathlib import Path

import pytest

from grapharrow import Database
from grapharrow.adapters import parse_cypher
from grapharrow.core.errors import QueryError, TransactionError
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


def test_match_rejects_mixing_aggregate_and_non_aggregate_return_items(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "Alice"})

    with database.read_transaction() as session:
        with pytest.raises(QueryError, match="Mixing aggregate and non-aggregate"):
            session.query("MATCH (n:Person) RETURN COUNT(*), n.name")


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


def test_match_create_rejects_inline_new_nodes(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (a:Person {name: 'Alice'}) RETURN a")
        with pytest.raises(QueryError, match="inline labels or properties"):
            session.query("MATCH (a:Person) CREATE (a)-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a")


def test_match_create_rejects_unbound_variables(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query("CREATE (a:Person {name: 'Alice'}) RETURN a")
        with pytest.raises(QueryError, match="unbound node variable"):
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
