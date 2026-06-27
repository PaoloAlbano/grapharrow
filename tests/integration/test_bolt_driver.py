from pathlib import Path

import pytest
from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import AuthError, Neo4jError
from neo4j.graph import Node, Relationship

from grapharrow.config import GraphArrowSettings
from grapharrow.interfaces.bolt import build_bolt_application, run_bolt_server


def test_bolt_application_uses_configured_server_agent(tmp_path: Path) -> None:
    database_path = tmp_path / "example-db"
    settings = GraphArrowSettings(
        database_path=database_path,
        bolt_server_agent="Neo4j/5.26.0",
    )

    app = build_bolt_application(database_path, settings=settings)

    assert app.server_agent == "Neo4j/5.26.0"


@pytest.mark.integration
def test_bolt_server_supports_direct_neo4j_driver_queries(tmp_path: Path) -> None:
    database_path = tmp_path / "example-db"
    settings = GraphArrowSettings(
        database_path=database_path,
        bolt_host="127.0.0.1",
        bolt_port=0,
        bolt_auth_token="secret",
    )

    with run_bolt_server(database_path, settings=settings) as handle:
        driver = GraphDatabase.driver(
            f"bolt://127.0.0.1:{handle.port}",
            auth=basic_auth("neo4j", "secret"),
        )
        try:
            with driver.session() as session:
                create_record = session.run(
                    "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) "
                    "RETURN a, b"
                ).single()
                assert create_record is not None
                assert create_record["a"]["properties"]["name"] == "Alice"
                assert create_record["b"]["properties"]["name"] == "Bob"

                result = session.run(
                    "MATCH (a:Person)-[:KNOWS]->(b:Person) "
                    "RETURN a.name, b.name ORDER BY a.name ASC"
                ).data()
        finally:
            driver.close()

    assert result == [{"a.name": "Alice", "b.name": "Bob"}]


@pytest.mark.integration
def test_bolt_server_exposes_common_summary_metadata_for_driver_consumers(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "example-db"
    settings = GraphArrowSettings(
        database_path=database_path,
        bolt_host="127.0.0.1",
        bolt_port=0,
        bolt_auth_token="secret",
    )

    with run_bolt_server(database_path, settings=settings) as handle:
        driver = GraphDatabase.driver(
            f"bolt://127.0.0.1:{handle.port}",
            auth=basic_auth("neo4j", "secret"),
        )
        try:
            with driver.session() as session:
                create_summary = session.run(
                    "CREATE (n:Person {name: 'Alice'}) RETURN n.name"
                ).consume()
                read_summary = session.run(
                    "MATCH (n:Person) RETURN DISTINCT n.name ORDER BY n.name"
                ).consume()
                bookmarks = session.last_bookmarks()
        finally:
            driver.close()

    assert create_summary.database == "example-db"
    assert create_summary.query_type == "w"
    assert create_summary.counters.contains_updates is True
    assert create_summary.counters.nodes_created == 1
    assert create_summary.counters.labels_added == 1
    assert create_summary.counters.properties_set == 1
    assert create_summary.result_available_after == 0
    assert create_summary.result_consumed_after == 0
    assert "grapharrow:snapshot:2" in bookmarks.raw_values

    assert read_summary.database == "example-db"
    assert read_summary.query_type == "r"
    assert read_summary.counters.contains_updates is False
    assert read_summary.result_available_after == 0
    assert read_summary.result_consumed_after == 0


@pytest.mark.integration
def test_bolt_server_supports_match_create_with_inline_endpoint(tmp_path: Path) -> None:
    database_path = tmp_path / "example-db"
    settings = GraphArrowSettings(
        database_path=database_path,
        bolt_host="127.0.0.1",
        bolt_port=0,
        bolt_auth_token="secret",
    )

    with run_bolt_server(database_path, settings=settings) as handle:
        driver = GraphDatabase.driver(
            f"bolt://127.0.0.1:{handle.port}",
            auth=basic_auth("neo4j", "secret"),
        )
        try:
            with driver.session() as session:
                category = session.run(
                    "CREATE (c:Category {name: $name}) RETURN id(c) AS iid",
                    name="Electronics",
                ).single()
                product = session.run(
                    "MATCH (c:Category {name: $category}) "
                    "CREATE (p:Product {id: $id, name: $name})-[:BELONGS_TO]->(c) "
                    "RETURN id(p) AS iid, p.name",
                    category="Electronics",
                    id="prod_0001",
                    name="Acme Widget",
                ).single()
                linked = session.run(
                    "MATCH (p:Product)-[:BELONGS_TO]->(c:Category) "
                    "RETURN p.name, c.name"
                ).single()
        finally:
            driver.close()

    assert category is not None
    assert category["iid"] == 1
    assert product is not None
    assert product["iid"] == 2
    assert product["p.name"] == "Acme Widget"
    assert linked is not None
    assert linked["p.name"] == "Acme Widget"
    assert linked["c.name"] == "Electronics"


@pytest.mark.integration
def test_bolt_server_rejects_invalid_credentials(tmp_path: Path) -> None:
    database_path = tmp_path / "example-db"
    settings = GraphArrowSettings(
        database_path=database_path,
        bolt_host="127.0.0.1",
        bolt_port=0,
        bolt_auth_token="secret",
    )

    with run_bolt_server(database_path, settings=settings) as handle:
        driver = GraphDatabase.driver(
            f"bolt://127.0.0.1:{handle.port}",
            auth=basic_auth("neo4j", "wrong-password"),
        )
        try:
            with pytest.raises(AuthError):
                driver.verify_connectivity()
        finally:
            driver.close()


@pytest.mark.integration
def test_bolt_server_supports_explicit_transaction_commit(tmp_path: Path) -> None:
    database_path = tmp_path / "example-db"
    settings = GraphArrowSettings(
        database_path=database_path,
        bolt_host="127.0.0.1",
        bolt_port=0,
        bolt_auth_token="secret",
    )

    with run_bolt_server(database_path, settings=settings) as handle:
        driver = GraphDatabase.driver(
            f"bolt://127.0.0.1:{handle.port}",
            auth=basic_auth("neo4j", "secret"),
        )
        try:
            with driver.session() as session:
                transaction = session.begin_transaction()
                try:
                    first = transaction.run("CREATE (n:Person {name: 'Alice'}) RETURN n").single()
                    second = transaction.run("CREATE (n:Person {name: 'Bob'}) RETURN n").single()
                    assert first is not None
                    assert second is not None
                    transaction.commit()
                finally:
                    transaction.close()

                persisted = session.run("MATCH (n:Person) RETURN n.name ORDER BY n.name ASC").data()
        finally:
            driver.close()

    assert persisted == [{"n.name": "Alice"}, {"n.name": "Bob"}]


@pytest.mark.integration
def test_bolt_server_supports_explicit_transaction_rollback(tmp_path: Path) -> None:
    database_path = tmp_path / "example-db"
    settings = GraphArrowSettings(
        database_path=database_path,
        bolt_host="127.0.0.1",
        bolt_port=0,
        bolt_auth_token="secret",
    )

    with run_bolt_server(database_path, settings=settings) as handle:
        driver = GraphDatabase.driver(
            f"bolt://127.0.0.1:{handle.port}",
            auth=basic_auth("neo4j", "secret"),
        )
        try:
            with driver.session() as session:
                transaction = session.begin_transaction()
                try:
                    record = transaction.run("CREATE (n:Person {name: 'Alice'}) RETURN n").single()
                    assert record is not None
                    transaction.rollback()
                finally:
                    transaction.close()

                persisted = session.run("MATCH (n:Person) RETURN COUNT(*) AS total").single()
        finally:
            driver.close()

    assert persisted is not None
    assert persisted["total"] == 0


@pytest.mark.integration
def test_bolt_server_allows_rollback_after_failed_transaction_query(tmp_path: Path) -> None:
    database_path = tmp_path / "example-db"
    settings = GraphArrowSettings(
        database_path=database_path,
        bolt_host="127.0.0.1",
        bolt_port=0,
        bolt_auth_token="secret",
    )

    with run_bolt_server(database_path, settings=settings) as handle:
        driver = GraphDatabase.driver(
            f"bolt://127.0.0.1:{handle.port}",
            auth=basic_auth("neo4j", "secret"),
        )
        try:
            with driver.session() as session:
                transaction = session.begin_transaction()
                try:
                    transaction.run(
                        "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) "
                        "RETURN a, b"
                    ).consume()
                    with pytest.raises(Neo4jError):
                        transaction.run(
                            "MATCH (n:Person {name: 'Alice'}) DELETE n RETURN n.name"
                        ).consume()
                    transaction.rollback()
                finally:
                    transaction.close()

                after_rollback = session.run(
                    "CREATE (n:Person {name: 'Bob'}) RETURN n.name"
                ).single()
                persisted = session.run("MATCH (n:Person) RETURN COUNT(*) AS total").single()
        finally:
            driver.close()

    assert after_rollback is not None
    assert after_rollback["n.name"] == "Bob"
    assert persisted is not None
    assert persisted["total"] == 1


@pytest.mark.integration
def test_bolt_server_returns_native_graph_values_for_schema_visualization(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "example-db"
    settings = GraphArrowSettings(
        database_path=database_path,
        bolt_host="127.0.0.1",
        bolt_port=0,
        bolt_auth_token="secret",
    )

    with run_bolt_server(database_path, settings=settings) as handle:
        driver = GraphDatabase.driver(
            f"bolt://127.0.0.1:{handle.port}",
            auth=basic_auth("neo4j", "secret"),
        )
        try:
            with driver.session() as session:
                session.run(
                    "CREATE (p:Product {id: 'p1'})-[:BELONGS_TO]->"
                    "(c:Category {name: 'Books'}) RETURN p, c"
                ).consume()
                record = session.run("CALL db.schema.visualization()").single()
        finally:
            driver.close()

    assert record is not None
    assert all(isinstance(node, Node) for node in record["nodes"])
    assert all(
        isinstance(relationship, Relationship)
        for relationship in record["relationships"]
    )
    assert {tuple(node.labels)[0] for node in record["nodes"]} == {
        "Category",
        "Product",
    }
    assert {relationship.type for relationship in record["relationships"]} == {
        "BELONGS_TO",
    }


@pytest.mark.integration
def test_bolt_server_returns_native_graph_values_for_regular_return(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "example-db"
    settings = GraphArrowSettings(
        database_path=database_path,
        bolt_host="127.0.0.1",
        bolt_port=0,
        bolt_auth_token="secret",
    )

    with run_bolt_server(database_path, settings=settings) as handle:
        driver = GraphDatabase.driver(
            f"bolt://127.0.0.1:{handle.port}",
            auth=basic_auth("neo4j", "secret"),
        )
        try:
            with driver.session() as session:
                session.run(
                    "CREATE (d:Death {order: 1})-[:HAPPENED_IN]->"
                    "(a:Location {name: 'Castle Black'}) RETURN d"
                ).consume()
                record = session.run(
                    "MATCH (a:Location)<-[r:HAPPENED_IN]-(d:Death) "
                    "RETURN a, r, d"
                ).single()
        finally:
            driver.close()

    assert record is not None
    assert isinstance(record["a"], Node)
    assert isinstance(record["r"], Relationship)
    assert isinstance(record["d"], Node)
    assert "Location" in record["a"].labels
    assert record["r"].type == "HAPPENED_IN"
    assert "Death" in record["d"].labels


@pytest.mark.integration
def test_bolt_server_supports_unwind_with_create_loader_shape_without_return(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "example-db"
    settings = GraphArrowSettings(
        database_path=database_path,
        bolt_host="127.0.0.1",
        bolt_port=0,
        bolt_auth_token="secret",
    )

    with run_bolt_server(database_path, settings=settings) as handle:
        driver = GraphDatabase.driver(
            f"bolt://127.0.0.1:{handle.port}",
            auth=basic_auth("neo4j", "secret"),
        )
        try:
            with driver.session() as session:
                summary = session.run(
                    "UNWIND $rows AS row "
                    "WITH row "
                    "CREATE (d:Death {character: row.character, episode: row.episode})",
                    rows=[
                        {"character": "Robert Baratheon", "episode": "S01E07"},
                        {"character": "Ned Stark", "episode": "S01E09"},
                    ],
                ).consume()
                persisted = session.run(
                    "MATCH (d:Death) RETURN d.character, d.episode ORDER BY d.character"
                ).data()
        finally:
            driver.close()

    assert summary.query_type == "w"
    assert summary.counters.nodes_created == 2
    assert persisted == [
        {"d.character": "Ned Stark", "d.episode": "S01E09"},
        {"d.character": "Robert Baratheon", "d.episode": "S01E07"},
    ]
