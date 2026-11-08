"""Cypher semantics compared with an explicitly configured disposable Neo4j DB."""

import json
import os
from collections import Counter
from typing import Any

import pytest
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError

from grapharrow import Database
from grapharrow.core.errors import QueryError

pytestmark = [pytest.mark.integration, pytest.mark.neo4j_reference]

READS = [
    "MATCH (p:Person) RETURN coalesce(p.name,'?') AS name ORDER BY p.age DESC,name ASC LIMIT 2",
    "MATCH (p:Person) RETURN coalesce(p.country,'?') AS country "
    "ORDER BY p.country DESC,p.name ASC SKIP 1 LIMIT 2",
    "MATCH (p:Person) RETURN coalesce(p.name,'?') AS name SKIP 1 LIMIT 1",
    "MATCH (p:Person) RETURN coalesce(p.name,'?') AS name LIMIT 0",
    "MATCH (p:Person) WHERE p.name CONTAINS 'a' RETURN coalesce(p.name,'?') AS name SKIP 1 LIMIT 1",
    "MATCH (p:Person) WITH coalesce(p.name,'?') AS name ORDER BY name LIMIT 2 "
    "WHERE p.age<30 RETURN name",
    "MATCH (p:Person) WITH coalesce(p.country,'?') AS country "
    "WITH country,count(*) AS n RETURN country,n ORDER BY n DESC,country LIMIT 1",
    "RETURN coalesce(null, 2) AS n",
    "RETURN CASE WHEN true THEN 2 ELSE 1/0 END AS n",
    "RETURN CASE WHEN false THEN 1/0 WHEN null THEN 1/0 ELSE 3 END AS n",
    "RETURN CASE null WHEN null THEN 1 ELSE 2 END AS n",
    "RETURN CASE 2 WHEN 1 THEN 9 WHEN 2 THEN coalesce(null,5) END AS n",
    "RETURN coalesce(null,abs(-3)+2) AS n",
    "RETURN coalesce(null,-5/2) AS n,coalesce(null,-5%2) AS m",
    "RETURN coalesce(null,5%-2) AS n,coalesce(null,5/-2) AS m",
    "RETURN coalesce('x'+1,'') AS n, coalesce(1+'x','') AS m",
    "RETURN coalesce(null,true=1) AS n",
    "RETURN coalesce(null,false AND null) AS n,coalesce(null,true OR null) AS m",
    "RETURN coalesce(null,1 IN [2,null]) AS n,coalesce(null,null IN []) AS m",
    "RETURN coalesce(null,[1,null]=[1,null]) AS n",
    "RETURN coalesce(null,[x IN [1,2,3] WHERE x>1 | x*2]) AS n",
    "RETURN coalesce(null,all(x IN [] WHERE x>0)) AS n",
    "RETURN coalesce(null,any(x IN [null,false] WHERE x)) AS n",
    "RETURN coalesce(null,[1,2,3][-1]) AS n,coalesce(null,[1,2,3][1..]) AS m",
    "RETURN coalesce(null,{nested:{x:4}}.nested.x) AS n",
    "RETURN coalesce(null,'EXISTS(n.prop)') AS n",
    "MATCH (p:Person) WITH p ORDER BY p.name RETURN [x IN collect(p) | x.name] AS names",
    "MATCH (p:Person) RETURN p.name AS name,coalesce(p.country,'unknown') AS country ORDER BY name",
    "MATCH (p:Person) WITH p WITH p.name AS name RETURN name ORDER BY name",
    "MATCH (p:Person) WITH * WITH p.name AS name RETURN name ORDER BY name",
    "MATCH (p:Person) WITH p.name AS name WHERE p.age>20 RETURN coalesce(name,'?') AS name",
    "UNWIND [1,2] AS x WITH x ORDER BY x LIMIT 1 WHERE x=2 RETURN coalesce(x,0) AS x",
    "MATCH (p:Person) WITH coalesce(p.country,'unknown') AS country,p.age AS age "
    "WITH country,sum(CASE WHEN age>=30 THEN age ELSE 0 END) AS total "
    "WHERE total>0 WITH country,total ORDER BY total DESC LIMIT 1 RETURN country,total",
    "MATCH (p:Person) RETURN p.country AS country,coalesce(sum(p.age),0) AS total ORDER BY country",
    "MATCH (p:Missing) WITH count(*) AS n,collect(p) AS people WITH coalesce(n,0) AS "
    "n,people RETURN n,people",
    "MATCH (p:Person) RETURN coalesce(sum(DISTINCT p.age),0) AS n",
    "MATCH (p:Person) WITH p OPTIONAL MATCH (p)-[:KNOWS]->(q) "
    "OPTIONAL MATCH (q)-[:KNOWS]->(r) WITH *,coalesce(q.name,'none') AS friend "
    "RETURN p.name AS name,friend,r.name AS next ORDER BY name",
    "RETURN 1 AS n UNION RETURN 1 AS n",
    "RETURN 'a' AS n UNION RETURN 'b' AS n",
    "RETURN 'a'+'b' AS n",
    "RETURN 1 AS n UNION ALL RETURN 1 AS n",
    "RETURN null AS n UNION RETURN null AS n UNION RETURN true AS n UNION RETURN 1 AS n",
    "RETURN 1 AS a,2 AS b UNION RETURN 3 AS b,4 AS a",
    "CALL () { MATCH (p:Person) RETURN p.name AS name UNION MATCH (p:Person) RETURN p.name "
    "AS name } RETURN name ORDER BY name",
    "WITH 10 AS base CALL () { RETURN 1 AS n UNION ALL RETURN 2 AS n } WITH base+n AS total "
    "RETURN total ORDER BY total",
    "MATCH (p:Person) CALL (p) { MATCH (p)-[:KNOWS]->(q) RETURN q.name AS friend } RETURN "
    "p.name AS person,friend",
    "MATCH (p:Person) CALL (*) { MATCH (p)-[:KNOWS]->(q) RETURN q.name AS friend } RETURN "
    "p.name AS person,friend",
    "MATCH (p:Person) CALL { WITH p MATCH (p)-[:KNOWS]->(q) RETURN q.name AS friend } "
    "RETURN p.name AS person,friend",
    "MATCH (p:Person) CALL (p) { MATCH (q:Missing) WITH count(*) AS n RETURN p.name AS "
    "name,n } RETURN name,n ORDER BY name",
    "MATCH (p:Person) CALL (p) { MATCH (q:Person {age:p.age}) WHERE q.name<>p.name "
    "RETURN count(q) AS peers } RETURN p.name AS name,peers ORDER BY name",
    "MATCH (p:Person) OPTIONAL CALL (p) { MATCH (p)-[:KNOWS]->(q) RETURN q.name AS friend } "
    "RETURN p.name AS name,friend ORDER BY name",
    "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:KNOWS]->() } RETURN p.name AS name",
    "MATCH (p:Person) WHERE NOT EXISTS { MATCH (p)-[:KNOWS]->() } RETURN p.name AS name "
    "ORDER BY name",
    "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:KNOWS]->(q) WITH q WHERE q.age>10 } RETURN "
    "p.name AS name",
    "MATCH (p:Person) WHERE EXISTS { MATCH (q:Missing) RETURN count(*) AS n } RETURN p.name "
    "AS name ORDER BY name",
    "MATCH (p:Person) WHERE EXISTS { CALL (p) { MATCH (p)-[:KNOWS]->(q) RETURN q } RETURN "
    "q.name AS name } RETURN p.name AS name",
    "UNWIND coalesce(2,0) AS x RETURN x",
    "WITH 1 AS x WITH x AS y RETURN y",
]

INVALID = [
    "MATCH (p:Missing) RETURN CASE WHEN true THEN unknown ELSE 0 END AS n",
    "MATCH (p:Person) WITH p.name AS name WITH p RETURN name",
    "MATCH (p:Person) WITH DISTINCT p.name AS name WHERE p.age>20 RETURN coalesce(name,'?') "
    "AS name",
    "RETURN 1 AS a UNION RETURN 2 AS b",
    "RETURN 1 AS a UNION ALL RETURN 2 AS a UNION RETURN 3 AS a",
    "MATCH (p:Person) CALL { RETURN p.name AS name } RETURN name",
    "MATCH (p:Person) CALL (unknown) { RETURN 1 AS n } RETURN n",
    "MATCH (p:Person) CALL (p) { RETURN p AS p } RETURN p",
    "MATCH (p:Person) CALL (p) { WITH 1 AS p RETURN p AS n } RETURN n",
    "MATCH (p:Person) CALL { WITH p AS q RETURN q } RETURN q",
    "RETURN coalesce() AS n",
    "RETURN CASE WHEN 1 THEN 2 ELSE 0 END AS n",
    "MATCH (p:Missing) RETURN CASE WHEN 1 THEN 2 ELSE 0 END AS n",
    "MATCH (p:Missing) WITH p WITH p WHERE 1 RETURN p",
    "RETURN coalesce(null,{x:1}[[1]]) AS n",
    "RETURN *",
    "CALL () { RETURN * } RETURN 1 AS n",
]


@pytest.fixture(scope="module")
def reference(tmp_path_factory: pytest.TempPathFactory) -> Any:
    uri = os.environ.get("GRAPHARROW_TEST_NEO4J_URI")
    if not uri:
        pytest.skip("Set GRAPHARROW_TEST_NEO4J_URI for Cypher reference comparisons")
    password = os.environ["GRAPHARROW_TEST_NEO4J_PASSWORD"]
    user = os.environ.get("GRAPHARROW_TEST_NEO4J_USER", "neo4j")
    db = Database(tmp_path_factory.mktemp("cypher-reference") / "db")
    fixture = [
        "CREATE (a:Person {name:'Alice',age:30,country:'IT'})"
        "-[:KNOWS {since:2020}]->(b:Person {name:'Bob',age:20})",
        "CREATE (c:Person {name:'Carol',age:30,country:'IT'})",
    ]
    with GraphDatabase.driver(uri, auth=(user, password)) as driver, driver.session() as session:
        if session.run("MATCH (n) RETURN count(n) AS count").single()["count"]:
            pytest.fail("Cypher reference checks require an empty disposable Neo4j database")
        identifiers = []
        try:
            with db.write_transaction() as local:
                for index, query in enumerate(fixture):
                    local.query(query)
                    projection = (
                        "RETURN elementId(a) AS a,elementId(b) AS b"
                        if index == 0
                        else "RETURN elementId(c) AS c"
                    )
                    record = session.run(query + " " + projection).single()
                    identifiers.extend(record.values())
            yield db, session
        finally:
            session.run(
                "MATCH (n) WHERE elementId(n) IN $ids DETACH DELETE n", ids=identifiers
            ).consume()


@pytest.mark.parametrize("query", READS)
def test_read_results_match_neo4j(reference: Any, query: str) -> None:
    database, remote = reference
    expected = remote.run(query)
    expected_columns = list(expected.keys())
    expected_rows = expected.data()
    with database.read_transaction() as session:
        actual = session.query(query)
    assert list(actual.columns) == expected_columns
    assert Counter(json.dumps(row, sort_keys=True) for row in actual.rows) == Counter(
        json.dumps(row, sort_keys=True) for row in expected_rows
    )
    if "ORDER BY" in query:
        assert list(actual.rows) == expected_rows


@pytest.mark.parametrize("query", INVALID)
def test_binding_and_type_errors_match_neo4j(reference: Any, query: str) -> None:
    database, remote = reference
    with pytest.raises(Neo4jError):
        remote.run(query).consume()
    with database.read_transaction() as session, pytest.raises(QueryError):
        session.query(query)
