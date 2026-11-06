"""Validate archives and exercise the installed wheel outside the checkout."""

from __future__ import annotations

import argparse
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory

SMOKE = r"""
import json
import os
from pathlib import Path
import secrets
import sys
import grapharrow
from fastapi.testclient import TestClient
from grapharrow.app import build_application
from grapharrow.config import GraphArrowSettings
from grapharrow.interfaces.rest import create_rest_app
from importlib.util import find_spec

assert find_spec("agent_native_graph") is None
assert find_spec("neo4j") is None

assert Path(grapharrow.__file__).is_relative_to(Path(sys.prefix)), grapharrow.__file__
token = secrets.token_urlsafe(32)
auth = Path('clients.json')
auth.write_text(json.dumps({'clients': [{'id': 'smoke', 'api_key': token, 'profile': 'readonly'}]}))
auth.chmod(0o600)
os.environ['ANA_AUTH_FILE'] = str(auth.resolve())
application = build_application('db')
application.use_cases.graph.create_node(labels=('Device',), properties={'name': 'wheel-device'})
app = create_rest_app('db', settings=GraphArrowSettings(ana_max_handles_per_session=1))
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
    response = client.post('/tools/call', headers=headers, json={
        'tool': 'count_nodes', 'args': {'label': 'Device'}})
    assert response.status_code == 429, response.text
    assert client.delete('/sessions/current', headers=headers).status_code == 204
    assert app.state.ana_resource_budget.used_bytes == 0
print('Installed-wheel smoke passed: default ANA, native pattern/fetch, owned session cleanup.')
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
    with tarfile.open(sources[0]) as archive:
        check_names(archive.getnames())
        assert any(name.endswith("/vendor/opencypher/Cypher.g4") for name in archive.getnames())
    with TemporaryDirectory(prefix="grapharrow-distribution-") as temporary:
        root = Path(temporary)
        environment = root / "venv"
        subprocess.run(["uv", "venv", "--python", sys.executable, str(environment)], check=True)
        python = environment / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
        # HTTPX is the smoke client's transport. It is not required by the wheel.
        subprocess.run(
            ["uv", "pip", "install", "--python", str(python), str(wheels[0]), "httpx"],
            check=True,
        )
        subprocess.run([str(python), "-I", "-c", SMOKE], cwd=root, check=True)
    print(
        "Distribution checks passed: license, standalone ANA runtime, "
        "private-file exclusions and fresh install."
    )


if __name__ == "__main__":
    main()
