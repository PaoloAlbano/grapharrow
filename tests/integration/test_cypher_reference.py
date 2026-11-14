"""Cypher semantics compared with an explicitly configured disposable Neo4j DB."""

import json
import os
from collections import Counter
from typing import Any

import pytest
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError
from neo4j.graph import Node, Path, Relationship

from grapharrow import Database
from grapharrow.core.errors import QueryError

pytestmark = [pytest.mark.integration, pytest.mark.neo4j_reference]

READS = [
    "MATCH (p:Person {name:'Alice'}) "
    "RETURN properties(head([p])).name AS name, last([1,2]) AS last, tail([]) AS tail",
    "RETURN toString(date({year:2024, ordinalDay:60})) AS day, "
    "toString(datetime.fromEpoch(-1,999999999)) AS instant, "
    "localtime('12:34:56.123456789').nanosecond AS nanos",
    "RETURN (datetime('2024-03-30T12:00[Europe/Rome]') + duration('P1D')).hour AS calendar, "
    "(datetime('2024-03-30T12:00[Europe/Rome]') + duration('PT24H')).hour AS clock",
    "RETURN toString(duration.between(date('2024-01-01'), date('2025-04-05'))) AS between, "
    "time.truncate('minute', time('12:34:56.123456789+02:00')).second AS truncated",
    "RETURN toString(date('2024-03-01') - duration('PT23H')) AS date, "
    "duration('PT-0.1S').nanosecondsOfSecond AS nanos, "
    "datetime('2024-01-02T00:30+01:00') < datetime('2024-01-01T23:45Z') AS earlier",
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
    "RETURN range(-2,2) AS xs,range(5,0,-2) AS ys,range(3,1) AS empty",
    "UNWIND range(2,6,2) AS x RETURN x,range(1,x)[-1] AS last ORDER BY x",
    "RETURN toInteger('-42.9') AS i,toFloat('1.2e2') AS f,"
    "toBoolean(' TRUE ') AS b,toString(false) AS s,sign(-3) AS sign",
    "RETURN toInteger(null) AS i,toFloat('bad') AS f,toBoolean('1') AS b,toString(null) AS s",
    "RETURN toInteger('0x10') AS i,toFloat('0x1.fp2') AS f,toString(10000000.0) AS s",
    "MATCH (p:Person) WHERE toInteger(p.age) IN range(20,30) "
    "RETURN toString(p.age) AS age ORDER BY age",
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
    "MATCH (p:Person) WITH p.name AS name WHERE p.name='Alice' RETURN name",
    "MATCH (p:Person) WITH DISTINCT p.country AS country WHERE p.country='IT' RETURN country",
    "MATCH (p:Person) OPTIONAL MATCH (p)-[:NOT_EXIST]->(x) "
    "RETURN p.name AS name,collect(x) AS nodes ORDER BY name",
    "UNWIND [1,2.0,5,null,3.2] AS x RETURN max(x) AS value",
    "RETURN [1,2,3][null..null] AS value",
    "RETURN [1,2,3][..null] AS value",
    r"RETURN '\u01FF' AS value",
    "RETURN 4^3*2^3 AS value,-3^2 AS unary",
    "WITH [1] AS xs,[2] AS ys UNWIND xs AS x UNWIND ys AS y RETURN *",
    "MATCH (p:Person)-[r:KNOWS*1..1]->(q) RETURN size(r) AS hops",
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
    "RETURN range(1,3,0) AS xs",
    "RETURN range(null,1) AS xs",
    "RETURN range(1,null) AS xs",
    "RETURN range(1,3,null) AS xs",
    "RETURN toInteger('9223372036854775808') AS n",
    "RETURN [x IN [1,true] | toFloat(x)] AS values",
    "RETURN CASE WHEN 1 THEN 2 ELSE 0 END AS n",
    "MATCH (p:Missing) RETURN CASE WHEN 1 THEN 2 ELSE 0 END AS n",
    "MATCH (p:Missing) WITH p WITH p WHERE 1 RETURN p",
    "RETURN coalesce(null,{x:1}[[1]]) AS n",
    "RETURN *",
    "CALL () { RETURN * } RETURN 1 AS n",
    "MATCH (r)-[r]->() RETURN r",
    "MATCH ()-[r]->() MATCH (r) RETURN r",
    "MATCH (p:Person) RETURN type(p)",
    "MATCH (p:Person) RETURN missing.p",
    "RETURN false AND 123 AS value",
    "RETURN true OR [] AS value",
    "RETURN [x IN [1,2] | count(*)] AS value",
    "MATCH (n) RETURN DISTINCT n.name ORDER BY n.age",
    "MATCH (n) WITH (n)-->() AS value RETURN value",
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


def _entity_value(value: Any) -> Any:
    if isinstance(value, Path):
        return {
            "nodes": [_entity_value(node) for node in value.nodes],
            "relationships": [_entity_value(edge) for edge in value.relationships],
            "directions": [
                edge.start_node == node
                for node, edge in zip(value.nodes[:-1], value.relationships, strict=True)
            ],
        }
    if isinstance(value, Node):
        return {"labels": sorted(value.labels), "properties": dict(value)}
    if isinstance(value, Relationship):
        return {"type": value.type, "properties": dict(value)}
    if isinstance(value, dict):
        from grapharrow.use_cases.cypher_path import PathResult

        if isinstance(value, PathResult):
            return {
                "nodes": [_entity_value(node) for node in value["nodes"]],
                "relationships": [_entity_value(edge) for edge in value["relationships"]],
                "directions": [
                    edge["source"] == node["id"]
                    for node, edge in zip(value["nodes"][:-1], value["relationships"], strict=True)
                ],
            }
        if "labels" in value and "properties" in value:
            return {"labels": sorted(value["labels"]), "properties": value["properties"]}
        if "source" in value and "target" in value and "properties" in value:
            return {"type": value["type"], "properties": value["properties"]}
        return {key: _entity_value(child) for key, child in value.items()}
    if isinstance(value, list):
        # These queries collect entities without defining collection order.
        return sorted(
            (_entity_value(child) for child in value), key=lambda v: json.dumps(v, sort_keys=True)
        )
    return value


@pytest.mark.parametrize(
    "query",
    [
        "MATCH (n:Person) RETURN count(n) AS total,collect(n) AS nodes",
        "MATCH (p:Person)-[r:KNOWS]->(q) RETURN p.name AS name,collect(r) AS relationships",
        "MATCH (p:Person)-[r:KNOWS*1..1]->(q) RETURN r",
        "MATCH p=(a:Person)<-[:KNOWS]-(b:Person) RETURN p,length(p) AS n",
        "OPTIONAL MATCH p=(:Person)-[:MISSING]->() RETURN p",
        "MATCH p=(:Person)-[:KNOWS*0..2]->(:Person) RETURN p,length(p) AS n",
        "MATCH p=(:Person)-[r:KNOWS*1..3]-(:Person) RETURN p,r",
        "MATCH p=(:Person)<-[:KNOWS*0..1]-()-[:KNOWS*0..1]->() RETURN p",
    ],
)
def test_entity_aggregate_and_list_values_match_neo4j(reference: Any, query: str) -> None:
    database, remote = reference
    expected = remote.run(query)
    columns = list(expected.keys())
    expected_rows = [_entity_value(dict(record)) for record in expected]
    with database.read_transaction() as session:
        actual = session.query(query)
    assert list(actual.columns) == columns
    assert Counter(
        json.dumps(_entity_value(row), sort_keys=True) for row in actual.rows
    ) == Counter(json.dumps(row, sort_keys=True) for row in expected_rows)


@pytest.mark.parametrize(
    "query",
    [
        "CREATE (:Scratch {p:missing})",
        "CREATE (:Scratch) WITH 0 AS x CREATE (:Other {p:1/x})",
        "CREATE (a)-[:R]->() DELETE a",
        "MATCH (n:Person) SET n.p=missing",
        "MATCH (n:Person) DETACH DELETE n RETURN n.age",
    ],
)
def test_invalid_writes_preserve_state_like_neo4j(reference: Any, query: str) -> None:
    database, remote = reference
    before = database._app.use_cases.graph.load_state()
    remote_before = remote.run("MATCH (n) RETURN count(n) AS total").single()["total"]
    with pytest.raises(Neo4jError):
        remote.run(query).consume()
    with database.write_transaction() as session, pytest.raises(QueryError):
        session.query(query)
    after = database._app.use_cases.graph.load_state()
    assert (
        after.snapshot == before.snapshot
        and after.nodes == before.nodes
        and after.edges == before.edges
    )
    assert remote.run("MATCH (n) RETURN count(n) AS total").single()["total"] == remote_before


@pytest.mark.parametrize(
    "setup,query,projection,expected",
    [
        (
            None,
            "CREATE (:FixA {run:$run})<-[:FIXR]-(:FixB {run:$run})",
            "MATCH (a:FixA)<-[:FIXR]-(:FixB) WHERE a.run=$run RETURN count(*) AS total",
            [{"total": 1}],
        ),
        (
            None,
            "CREATE (n:FixLoop {run:$run})-[:FIXR]->(n)",
            "MATCH (n:FixLoop)-[r:FIXR]-(n) WHERE n.run=$run RETURN count(r) AS total",
            [{"total": 1}],
        ),
        (
            "CREATE (:FixNull {run:$run,p:42})",
            "MATCH (n:FixNull {run:$run}) SET n.p=null RETURN n.p AS p",
            "MATCH (n:FixNull {run:$run}) RETURN keys(n) AS props",
            [{"props": ["run"]}],
        ),
        (
            None,
            "CREATE (n:FixWindow {run:$run}) RETURN n LIMIT 0",
            "MATCH (n:FixWindow {run:$run}) RETURN count(n) AS total",
            [{"total": 1}],
        ),
        (
            None,
            "UNWIND [1,2,3] AS x CREATE (n:FixPipeline {run:$run,x:x}) "
            "WITH n MATCH (n:FixPipeline) SET n:FixReady, n.value=n.x*10 "
            "REMOVE n.x RETURN n.value AS value ORDER BY value DESC LIMIT 1",
            "MATCH (n:FixReady {run:$run}) RETURN n.value AS value ORDER BY value",
            [{"value": 10}, {"value": 20}, {"value": 30}],
        ),
        (
            None,
            "CREATE (a:FixLeft {run:$run}) CREATE (b:FixRight {run:$run}) "
            "MERGE (a)<-[r:FIXPIPE]-(b) ON CREATE SET r.p=1 "
            "MERGE (a)-[s:FIXPIPE]-(b) ON MATCH SET s.p=s.p+1 RETURN s.p AS p",
            "MATCH (a:FixLeft)<-[r:FIXPIPE]-(:FixRight) WHERE a.run=$run RETURN r.p AS p",
            [{"p": 2}],
        ),
        (
            None,
            "CREATE (:MultiCreateA {run:$run})-[:FIRST]->(:MultiCreateB {run:$run})"
            "<-[:SECOND]-(:MultiCreateC {run:$run})",
            "MATCH p=(a:MultiCreateA {run:$run})-[:FIRST]->()<-[:SECOND]-() "
            "RETURN length(p) AS hops",
            [{"hops": 2}],
        ),
        (
            "CREATE (:MultiMergeA {run:$run})-[:FIRST]->(:MultiMergeB {run:$run})",
            "MATCH (a:MultiMergeA {run:$run}) "
            "UNWIND [1,2] AS i "
            "MERGE p=(a)-[:FIRST]->(b:MultiMergeB {run:$run})"
            "<-[:SECOND]-(:MultiMergeC {run:$run}) "
            "ON CREATE SET b.created=true ON MATCH SET b.matched=true "
            "RETURN i,length(p) AS hops ORDER BY i",
            "MATCH (a:MultiMergeA {run:$run})-[:FIRST]->(b:MultiMergeB) "
            "RETURN count(b) AS nodes,count(b.matched) AS matched",
            [{"nodes": 2, "matched": 1}],
        ),
        (
            None,
            "MERGE (n:FixMerge {run:$run}) RETURN n SKIP 1",
            "MATCH (n:FixMerge {run:$run}) RETURN count(n) AS total",
            [{"total": 1}],
        ),
    ],
)
def test_write_correctness_matches_neo4j(
    reference: Any, setup: str | None, query: str, projection: str, expected: list[dict[str, Any]]
) -> None:
    from uuid import uuid4

    database, remote = reference
    params = {"run": uuid4().hex}
    try:
        if setup:
            remote.run(setup, params).consume()
            with database.write_transaction() as session:
                session.query(setup, params)
        result = remote.run(query, params)
        columns = list(result.keys())
        expected_write = result.data()
        with database.write_transaction() as session:
            actual = session.query(query, params)
        assert list(actual.columns) == columns
        assert list(actual.rows) == expected_write
        assert remote.run(projection, params).data() == expected
        with database.read_transaction() as session:
            assert list(session.query(projection, params).rows) == expected
    finally:
        identifiers = [
            r["id"]
            for r in remote.run("MATCH (n) WHERE n.run=$run RETURN elementId(n) AS id", params)
        ]
        remote.run(
            "MATCH (n) WHERE elementId(n) IN $ids DETACH DELETE n", ids=identifiers
        ).consume()
