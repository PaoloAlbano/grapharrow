from pathlib import Path

from grapharrow.app import build_application
from grapharrow.config import GraphArrowSettings


def test_build_application_wires_core_components(tmp_path: Path) -> None:
    database_path = tmp_path / "example-db"
    app = build_application(database_path)

    assert app.layout.root == database_path.resolve()
    assert app.repositories.catalog is not None
    assert app.repositories.snapshot is not None
    assert app.repositories.graph is not None
    assert app.use_cases.admin is not None
    assert app.use_cases.graph is not None
    assert app.use_cases.cypher is not None
    assert app.use_cases.indexes is not None


def test_settings_can_be_provided_explicitly(tmp_path: Path) -> None:
    database_path = tmp_path / "example-db"
    settings = GraphArrowSettings(database_path=database_path, environment="test")

    app = build_application(database_path, settings=settings)

    assert app.settings.environment == "test"
    assert app.settings.database_path == database_path
