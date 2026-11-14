"""Validate archives and exercise the installed wheel outside the checkout."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tarfile
import zipfile
from email.parser import Parser
from pathlib import Path
from tempfile import TemporaryDirectory

SMOKE = r"""
import json
import os
from pathlib import Path
import secrets
import sys
import grapharrow
from importlib.metadata import version
from fastapi.testclient import TestClient
from grapharrow.app import build_application
from grapharrow.config import GraphArrowSettings
from grapharrow.interfaces.rest import create_rest_app
from importlib.util import find_spec

assert find_spec("agent_native_graph") is None
assert find_spec("neo4j") is None

assert Path(grapharrow.__file__).is_relative_to(Path(sys.prefix)), grapharrow.__file__
assert version('grapharrow') == os.environ['GRAPHARROW_EXPECTED_VERSION']
assert grapharrow.__version__ == version('grapharrow')
token = secrets.token_urlsafe(32)
auth = Path('clients.json')
auth.write_text(json.dumps({'clients': [{'id': 'smoke', 'api_key': token, 'profile': 'readonly'}]}))
auth.chmod(0o600)
os.environ['ANA_AUTH_FILE'] = str(auth.resolve())
application = build_application('db')
application.use_cases.graph.create_node(labels=('Device',), properties={'name': 'wheel-device'})
app = create_rest_app('db', settings=GraphArrowSettings(ana_max_handles_per_session=1))
assert app.version == version('grapharrow')
with TestClient(app) as client:
    assert client.get('/health').json() == {'status': 'ok'}
    headers = {'Authorization': 'Bearer ' + token}
    specs = client.get('/tools/specs', headers=headers).json()['tools']
    assert len(specs) == 35
    assert "fetch" in {spec["function"]["name"] for spec in specs}
    session = client.post('/sessions', headers=headers)
    assert session.status_code == 201, session.text
    headers['X-ANA-Session'] = session.json()['session_id']
    response = client.post('/tools/call', headers=headers, json={
        'tool': 'pattern_query', 'args': {'start_label': 'Device', 'hops': []}})
    assert response.status_code == 200, response.text
    handle = response.json()['result']['handle']
    assert handle == 'h1', handle
    response = client.post('/tools/call', headers=headers, json={
        'tool': 'fetch', 'args': {'from': handle}})
    assert response.json()['result']['rows'] == [['wheel-device']], response.text
    second = client.post('/sessions', headers={'Authorization': 'Bearer ' + token})
    assert second.status_code == 201
    second_headers = {
        'Authorization': 'Bearer ' + token,
        'X-ANA-Session': second.json()['session_id'],
    }
    response = client.post('/tools/call', headers=second_headers, json={
        'tool': 'count_nodes', 'args': {'label': 'Device'}})
    assert response.json()['result']['handle'] == 'h1'
    response = client.post('/tools/call', headers=second_headers, json={
        'tool': 'fetch', 'args': {'from': 'h1'}})
    assert response.json()['result']['rows'] == [[1]]
    response = client.post('/tools/call', headers=headers, json={
        'tool': 'fetch', 'args': {'from': handle}})
    assert response.json()['result']['rows'] == [['wheel-device']]
    assert client.delete('/sessions/current', headers=second_headers).status_code == 204
    response = client.post('/tools/call', headers=headers, json={
        'tool': 'count_nodes', 'args': {'label': 'Device'}})
    assert response.status_code == 429, response.text
    assert client.delete('/sessions/current', headers=headers).status_code == 204
    assert app.state.ana_resource_budget.used_bytes == 0
from grapharrow.core.errors import WriteConflictError
from grapharrow.use_cases import CypherQueryUseCase, GraphTransactionUseCase
transaction = GraphTransactionUseCase(application.use_cases.graph)
CypherQueryUseCase(transaction).execute("CREATE (:Discarded {name: 'stale'})")
application.use_cases.graph.create_node(labels=('Device',), properties={'name': 'committed'})
try:
    transaction.commit()
except WriteConflictError:
    pass
else:
    raise AssertionError('Expected a write conflict')
assert application.use_cases.admin.verify(verify_checksums=True).ok
from grapharrow.use_cases.ana import GraphArrowAgentBackend
application.settings.ana_read_cache_enabled = True
with GraphArrowAgentBackend(application) as cached:
    args = {'label': 'Device', 'text': 'wheel-device'}
    first = cached.call('entity_resolve', args)
    second = cached.call('entity_resolve', args)
    assert first['handle'] != second['handle']
    assert cached.resource_usage['cache_hits'] == 1
    assert cached.call('fetch', {'from': second['handle']})['rows'] == [['wheel-device']]
application.use_cases.indexes.create_index(
    name='device_name', kind='range', owner_kind='label',
    owner_name='Device', property_names=('name',))
query = 'MATCH (d:Device {name: $name}) RETURN d.name AS name'
for name in ['wheel-device', 'committed', 'wheel-device']:
    assert application.use_cases.cypher.execute(query, {'name': name}).rows == ({'name': name},)
assert application.use_cases.graph.property_indexes.builds == 1
assert application.use_cases.graph.property_indexes.hits >= 2
assert query in application.use_cases.cypher._parse_cache._entries
with GraphArrowAgentBackend(application) as indexed:
    found = indexed.call('node_search', {
        'label': 'Device', 'property': 'name', 'value': 'wheel-device'})
    assert indexed.call('fetch', {'from': found['handle']})['rows'] == [['wheel-device']]
assert application.use_cases.admin.verify(verify_checksums=True).ok
from grapharrow.domain.scan import ScanPredicate
projection = application.use_cases.graph.load_node_property_table(
    snapshot=application.repositories.snapshot.load_manifest(), label='Device', columns=['name'],
    predicates=(ScanPredicate('name', '=', 'wheel-device'),))
assert projection.to_pylist() == [{'name': 'wheel-device'}]
range_query = ('MATCH (d:Device) WHERE d.name >= $name AND d.name <= $name '
               'RETURN d.name AS name')
assert application.use_cases.cypher.execute(range_query, {'name': 'wheel-device'}).rows == (
    {'name': 'wheel-device'},)
import pandas as pd
frame = pd.DataFrame({'name': ['nullable', 'value'], 'value': pd.Series([None, 42], dtype='Int64')})
application.use_cases.graph.create_nodes_bulk(
    table=application.adapters.bulk_input.to_table(frame), default_labels=('Imported',))
assert application.use_cases.cypher.execute(
    'MATCH (n:Imported) RETURN n.value AS value ORDER BY n.name').rows == (
        {'value': None}, {'value': 42})
assert application.use_cases.admin.verify(verify_checksums=True).ok
from dataclasses import replace
from grapharrow.use_cases.ana_resources import AnaQuotaExceeded, retained_size
with GraphArrowAgentBackend(application) as atomic:
    first = atomic.call('count_nodes', {'label': 'Device'})['handle']
    atomic.call('fetch', {'from': first})
    previous_usage, previous_page = atomic.resource_usage, atomic.last_fetch
    previous_lineage = atomic._handle_store.lineage[first]
    limits = atomic._resources.limits
    reserve = retained_size([{'device_count': 2}])
    reserve += retained_size((None, 'table', ['device_count'], None))
    atomic._resources.limits = replace(limits, max_bytes=previous_usage['bytes'] + reserve + 64)
    try:
        atomic.call('count_nodes', {'label': 'Device'})
    except AnaQuotaExceeded:
        pass
    else:
        raise AssertionError('Expected new-handle admission failure')
    assert atomic.resource_usage == previous_usage
    atomic._resources.limits = replace(limits, max_bytes=previous_usage['bytes'])
    try:
        atomic.call('summarize_handle', {'from': first})
    except AnaQuotaExceeded:
        pass
    else:
        raise AssertionError('Expected lineage admission failure')
    assert atomic.resource_usage == previous_usage
    assert atomic.last_fetch is previous_page
    assert atomic._handle_store.lineage[first] is previous_lineage
    atomic._resources.limits = limits
    assert atomic.call('count_nodes', {'label': 'Device'})['handle'] == 'h3'
assert atomic._resources.budget.used_bytes == 0
application.use_cases.cypher.execute(
    "MATCH (d:Device {name: 'wheel-device'}) CREATE (d)-[:LINK]->(t:Target {name: 'target'})")
application.use_cases.cypher.execute(
    "MATCH (d:Device {name: 'wheel-device'}) CREATE (d)-[:LINK]->(d)")
with GraphArrowAgentBackend(application) as traversal:
    assert traversal._adjacency is not None and traversal._adjacency.outgoing is None
    seed = traversal.call('node_search', {'label': 'Device', 'value': 'wheel-device'})
    result = traversal.call('expand', {
        'from': seed['handle'], 'source': 'device', 'relationship_type': 'LINK',
        'target_label': 'Device', 'direction': 'both'})
    assert traversal.call('fetch', {'from': result['handle']})['rows'] == [['wheel-device']]
    pattern = traversal.call('multi_hop_query', {
        'start_label': 'Device', 'from': seed['handle'], 'hops': [
            {'relationship_type': 'LINK', 'target_label': 'Target'}]})
    assert traversal.call('fetch', {'from': pattern['handle']})['rows'] == [['target']]
    assert traversal._adjacency.outgoing is not None and traversal._adjacency.incoming is not None
    shared_index = traversal._adjacency
assert traversal._resources.budget.used_bytes == 0 and traversal._adjacency is None
shared_query = ("MATCH (d:Device {name:'wheel-device'})-[:LINK]->(t:Target) "
                "RETURN t.name AS name")
assert application.use_cases.cypher.execute(shared_query).rows == ({'name': 'target'},)
assert shared_index.build_count == 1
with GraphArrowAgentBackend(application) as another:
    assert another._adjacency is shared_index
transaction = GraphTransactionUseCase(application.use_cases.graph)
transaction_cypher = CypherQueryUseCase(transaction)
assert transaction_cypher.execute(shared_query).rows == ({'name': 'target'},)
transaction_cypher.execute(
    "MATCH (d:Device {name:'wheel-device'})-[r:LINK]->(t:Target) DELETE r")
assert transaction_cypher.execute(shared_query).rows == ()
assert transaction._adjacency_index is not None and transaction._adjacency_index is not shared_index
transaction.rollback()
assert transaction._adjacency_index is None
assert application.use_cases.cypher.execute(shared_query).rows == ({'name': 'target'},)
assert shared_index.build_count == 1
read_queries = [
    ("RETURN CASE WHEN true THEN coalesce(null, 7) ELSE 1/0 END AS n", ({'n': 7},)),
    ("WITH 1 AS n WITH n+1 AS m WITH m*2 AS n RETURN n", ({'n': 4},)),
    ("RETURN 1 AS n UNION RETURN 1 AS n", ({'n': 1},)),
    ("CALL () { RETURN 1 AS n UNION ALL RETURN 2 AS n } RETURN n ORDER BY n",
     ({'n': 1}, {'n': 2})),
    ("MATCH (d:Device) CALL (d) { MATCH (d)-[:LINK]->(t:Target) "
     "RETURN count(t) AS n } RETURN d.name AS name,n ORDER BY name",
     ({'name': 'committed', 'n': 0}, {'name': 'wheel-device', 'n': 1})),
    ("MATCH (d:Device) WHERE EXISTS { MATCH (d)-[:LINK]->(t:Target) "
     "WITH t WHERE t.name='target' } RETURN d.name AS name",
     ({'name': 'wheel-device'},)),
]
for statement, expected in read_queries:
    assert application.use_cases.cypher.execute(statement).rows == expected, statement
original_state_loader = application.use_cases.graph.load_state
def reject_canonical_state(*args, **kwargs):
    raise AssertionError('Eligible columnar read materialized canonical graph records')
application.use_cases.graph.load_state = reject_canonical_state
try:
    columnar_query = ("MATCH (d:Device) WHERE d.name='wheel-device' "
                      "WITH coalesce(d.name,'?') AS name WITH name RETURN name")
    assert application.use_cases.cypher.execute(columnar_query).rows == ({'name':'wheel-device'},)
    limited = "MATCH (d:Device) RETURN coalesce(d.name,'?') AS name ORDER BY name LIMIT 1"
    assert application.use_cases.cypher.execute(limited).rows == ({'name': 'committed'},)
    assert len(application.use_cases.cypher.execute(
        "MATCH (d:Device) RETURN coalesce(d.name,'?') AS name LIMIT 1"
    ).rows) == 1
finally:
    application.use_cases.graph.load_state = original_state_loader
with GraphArrowAgentBackend(application) as seeded:
    materialized = []
    original_materialize = seeded._materialize_nodes
    def materialize(ids):
        materialized.extend(ids)
        return original_materialize(ids)
    seeded._materialize_nodes = materialize
    selected = seeded.call('pattern_query', {
        'start_label': 'Imported', 'hops': [],
        'filters': [{'var': 'imported', 'property': 'value', 'value': 42}],
        'select': [{'var': 'imported', 'property': 'name', 'alias': 'name'}]})
    assert seeded.call('fetch', {'from': selected['handle']})['rows'] == [['value']]
    assert len(materialized) == 1
import pyarrow as pa
from grapharrow.use_cases import ana_search
application.use_cases.graph.create_nodes_bulk(table=pa.table({
    'name': [f'entity-{i:03d}' if i != 129 else 'İstanbul' for i in range(130)],
    'aliases': [[f'alias-{i:03d}'] for i in range(130)],
    'description': ['north'] * 130,
}), default_labels=('Candidate',))
application.settings.ana_read_cache_enabled = False
with GraphArrowAgentBackend(application) as ranked:
    scored = []
    original_search = ana_search._search_rows
    def property_rows(*args, **kwargs):
        for node_id, props in original_search(*args, **kwargs):
            scored.append(node_id)
            yield node_id, props
    ana_search._search_rows = property_rows
    try:
        result = ranked.call('entity_resolve', {'label': 'Candidate', 'text': 'entity-042'})
        assert ranked.call('fetch', {'from': result['handle']})['rows'] == [['entity-042']]
        assert 0 < len(scored) <= 13
        result = ranked.call('entity_resolve', {'label': 'Candidate', 'text': 'i̇stanbul'})
        assert ranked.call('fetch', {'from': result['handle']})['rows'] == [['İstanbul']]
    finally:
        ana_search._search_rows = original_search
assert ranked._resources.budget.used_bytes == 0
assert application.use_cases.admin.verify(verify_checksums=True).ok
print('Installed-wheel smoke passed: seed filters, ranked discovery, adjacency and isolation.')
"""


def check_names(names: list[str]) -> None:
    private = {
        ".local-notes",
        "investigation",
        ".local-secrets",
        ".env",
        "TODO.md",
        "adr",
        "cypher-engine-roadmap.md",
        "agent-first-graph-database.md",
        "agent-first-graph-appunti.md",
    }
    for name in names:
        if private.intersection(Path(name).parts):
            raise ValueError(f"Private working material included in distribution: {name}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--archives-only", action="store_true")
    args = parser.parse_args()
    directory = args.directory.resolve()
    wheels = list(directory.glob("grapharrow-*.whl"))
    sources = list(directory.glob("grapharrow-*.tar.gz"))
    if len(wheels) != 1 or len(sources) != 1:
        raise ValueError("Expected exactly one GraphArrow wheel and one source distribution")
    with zipfile.ZipFile(wheels[0]) as archive:
        names = archive.namelist()
        check_names(names)
        metadata_name = next(name for name in names if name.endswith(".dist-info/METADATA"))
        metadata = archive.read(metadata_name).decode()
        expected_version = Parser().parsestr(metadata)["Version"]
        assert expected_version
        assert "License-Expression: Apache-2.0" in metadata
        assert any(name.endswith("/licenses/LICENSE") for name in names)
        assert any(name.endswith("/licenses/THIRD_PARTY_NOTICES.md") for name in names)
        dependencies = [
            line.removeprefix("Requires-Dist: ").lower()
            for line in metadata.splitlines()
            if line.startswith("Requires-Dist: ")
        ]
        assert not any(dep.startswith(("agent-native-graph", "neo4j")) for dep in dependencies)
        assert "grapharrow/ana/tool_schemas.json" in names
        assert "grapharrow/use_cases/ana_patterns.py" in names
        assert "grapharrow/use_cases/ana_roles.py" in names
        assert "grapharrow/use_cases/ana_resources.py" in names
        assert "grapharrow/use_cases/ana_runtime.py" in names
        assert "grapharrow/use_cases/ana_schema_cache.py" in names
        assert "grapharrow/use_cases/ana_read_cache.py" in names
        assert "grapharrow/use_cases/ana_adjacency.py" in names
        assert "grapharrow/adapters/adjacency.py" in names
        assert "grapharrow/use_cases/ana_seed.py" in names
        assert "grapharrow/adapters/writer.py" in names
        assert "grapharrow/adapters/property_index.py" in names
        assert "grapharrow/adapters/query_cache.py" in names
        assert "grapharrow/use_cases/cypher_expression.py" in names
        assert "grapharrow/use_cases/cypher_pipeline.py" in names
        assert "grapharrow/use_cases/cypher_columnar.py" in names
        assert "grapharrow/use_cases/cypher_window.py" in names
        assert (
            "grapharrow/interfaces/opencypher/generated/vendor/opencypher/GraphArrowCypherParser.py"
        ) in names
        assert "grapharrow/domain/scan.py" in names
        assert "grapharrow/adapters/scan_filter.py" in names
    with tarfile.open(sources[0]) as archive:
        check_names(archive.getnames())
        assert any(name.endswith("/vendor/opencypher/Cypher.g4") for name in archive.getnames())
        assert any(
            name.endswith("/vendor/opencypher/GraphArrowCypher.g4") for name in archive.getnames()
        )
        assert any(
            name.endswith("/scripts/benchmark_cypher_columnar.py") for name in archive.getnames()
        )
        package_info = next(
            member
            for member in archive.getmembers()
            if member.name.count("/") == 1 and member.name.endswith("/PKG-INFO")
        )
        source_info = archive.extractfile(package_info)
        assert source_info is not None
        assert Parser().parsestr(source_info.read().decode())["Version"] == expected_version
    if args.archives_only:
        print(f"Distribution archive checks passed for GraphArrow {expected_version}.")
        return
    with TemporaryDirectory(prefix="grapharrow-distribution-") as temporary:
        root = Path(temporary)
        environment = root / "venv"
        subprocess.run(["uv", "venv", "--python", sys.executable, str(environment)], check=True)
        python = environment / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
        # HTTP clients are smoke-test transports and are not required by the wheel.
        subprocess.run(
            ["uv", "pip", "install", "--python", str(python), str(wheels[0]), "httpx", "httpx2"],
            check=True,
        )
        environment_variables = {**os.environ, "GRAPHARROW_EXPECTED_VERSION": expected_version}
        subprocess.run(
            [str(python), "-I", "-c", SMOKE], cwd=root, env=environment_variables, check=True
        )
    print(
        "Distribution checks passed: license, standalone ANA runtime, "
        "private-file exclusions and fresh install."
    )


if __name__ == "__main__":
    main()
