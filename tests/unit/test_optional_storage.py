"""Local applications do not need provider SDKs; missing extras explain installation."""

import subprocess
import sys
from pathlib import Path

import pytest

from grapharrow.adapters import object_pointer
from grapharrow.config import GraphArrowSettings
from grapharrow.core.errors import StorageError


@pytest.mark.parametrize(
    ("provider", "module"),
    [("s3", "boto3"), ("azure", "azure.storage.blob"), ("gcs", "google.cloud.storage")],
)
def test_missing_provider_sdk_explains_its_extra(
    monkeypatch: pytest.MonkeyPatch, provider: str, module: str
) -> None:
    def missing(name: str) -> None:
        assert name == module
        raise ModuleNotFoundError(name=name.split(".")[0])

    monkeypatch.setattr(object_pointer, "import_module", missing)
    settings = GraphArrowSettings()
    with pytest.raises(StorageError, match=rf"grapharrow\[{provider}\]"):
        if provider == "s3":
            object_pointer.S3Pointer(settings)
        elif provider == "azure":
            object_pointer.AzurePointer(settings, "account")
        else:
            object_pointer.GCSPointer(settings)


def test_an_internal_sdk_import_error_is_not_reported_as_a_missing_extra(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken(name: str) -> None:
        raise ModuleNotFoundError(name="internal_sdk_dependency")

    monkeypatch.setattr(object_pointer, "import_module", broken)
    with pytest.raises(ModuleNotFoundError) as error:
        object_pointer.S3Pointer(GraphArrowSettings())
    assert error.value.name == "internal_sdk_dependency"


@pytest.mark.parametrize("block_sdks", [False, True])
def test_local_rest_and_ana_work_without_importing_cloud_sdks(
    tmp_path: Path, block_sdks: bool
) -> None:
    script = r"""
import builtins
import sys
original = builtins.__import__
def no_cloud(name, globals=None, locals=None, fromlist=(), level=0):
    if level == 0 and name.split('.')[0] in {'boto3', 'botocore', 'azure', 'google'}:
        raise ModuleNotFoundError(name=name)
    return original(name, globals, locals, fromlist, level)
if sys.argv[2] == "blocked":
    builtins.__import__ = no_cloud
from fastapi.testclient import TestClient
from grapharrow.app import build_application
from grapharrow.config import GraphArrowSettings
from grapharrow.interfaces.rest import create_rest_app
from grapharrow.use_cases.ana import GraphArrowAgentBackend
app = create_rest_app(sys.argv[1], settings=GraphArrowSettings(ana_enabled=True))
with TestClient(app) as client:
    assert client.get('/health').status_code == 200
    response = client.post('/v1/query', json={'query': 'CREATE (:Person {name:"local"})'})
    assert response.status_code == 200, response.text
    with GraphArrowAgentBackend(app.state.grapharrow) as ana:
        handle = ana.call('count_nodes', {'label': 'Person'})['handle']
        assert ana.call('fetch', {'from': handle})['rows'] == [[1]]
sdk_modules = ('boto3', 'botocore', 'azure.identity', 'azure.storage.blob', 'google.cloud.storage')
assert not [name for name in sys.modules if any(
    name == sdk or name.startswith(sdk + '.') for sdk in sdk_modules
)]
"""
    subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(tmp_path / "local"),
            "blocked" if block_sdks else "installed",
        ],
        check=True,
    )


@pytest.mark.parametrize(
    ("provider", "module"),
    [("s3", "boto3"), ("azure", "azure.storage.blob"), ("gcs", "google.cloud.storage")],
)
def test_selecting_a_provider_imports_only_its_sdk(provider: str, module: str) -> None:
    pytest.importorskip(module)
    script = r"""
import sys
from grapharrow.adapters.object_pointer import S3Pointer, AzurePointer, GCSPointer
from grapharrow.config import GraphArrowSettings
settings = GraphArrowSettings()
provider = sys.argv[1]
if provider == 's3':
    S3Pointer(settings)
elif provider == 'azure':
    AzurePointer(settings, 'unused-account')
else:
    GCSPointer(settings)
modules = {'s3':'boto3', 'azure':'azure.storage.blob', 'gcs':'google.cloud.storage'}
assert modules[provider] in sys.modules
for other, module in modules.items():
    if other != provider:
        assert module not in sys.modules, (provider, module)
"""
    subprocess.run([sys.executable, "-c", script, provider], check=True)
