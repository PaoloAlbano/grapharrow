from pathlib import Path

import pytest
from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import AuthError, Neo4jError

from grapharrow.config import GraphArrowSettings
from grapharrow.interfaces.bolt import run_bolt_server


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
