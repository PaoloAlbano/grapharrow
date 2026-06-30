import json
from pathlib import Path

from grapharrow.core import DatabaseLayout
from grapharrow.domain import (
    CatalogVersion,
    ConstraintDefinition,
    IndexDefinition,
    LabelDefinition,
    LabelId,
    PropertyDefinition,
    RelationshipTypeDefinition,
    RelationshipTypeId,
    VersionedCatalog,
)
from grapharrow.repositories import CatalogRepository


def test_catalog_store_can_write_and_load_a_catalog(tmp_path: Path) -> None:
    layout = DatabaseLayout(root=tmp_path / "example-db")
    store = CatalogRepository(layout=layout)
    catalog = VersionedCatalog(
        version=CatalogVersion(2),
        labels=(LabelDefinition(id=LabelId(1), name="Person"),),
        relationship_types=(RelationshipTypeDefinition(id=RelationshipTypeId(1), name="KNOWS"),),
        properties=(
            PropertyDefinition(
                name="name",
                owner_kind="label",
                owner_name="Person",
                data_type="string",
                nullable=False,
            ),
        ),
        constraints=(
            ConstraintDefinition(
                kind="required",
                owner_kind="label",
                owner_name="Person",
                property_name="name",
            ),
        ),
        indexes=(
            IndexDefinition(
                name="person_name_idx",
                kind="range",
                owner_kind="label",
                owner_name="Person",
                property_names=("name",),
            ),
        ),
    )

    store.write(catalog)
    loaded_catalog = store.load()

    assert loaded_catalog == catalog


def test_catalog_store_loads_legacy_catalogs_without_indexes(tmp_path: Path) -> None:
    layout = DatabaseLayout(root=tmp_path / "example-db")
    store = CatalogRepository(layout=layout)
    layout.ensure_base_directories()
    catalog_path = layout.catalog_dir / "catalog-000001.json"
    catalog_path.write_text(
        json.dumps(
            {
                "version": 1,
                "labels": [{"id": 1, "name": "Person"}],
                "relationship_types": [],
                "properties": [],
                "constraints": [],
            }
        )
    )
    layout.current_catalog_path.write_text(
        json.dumps({"version": 1, "path": layout.relative_path(catalog_path)})
    )

    loaded_catalog = store.load()

    assert loaded_catalog.indexes == ()


def test_catalog_store_bootstrap_creates_the_initial_catalog(tmp_path: Path) -> None:
    layout = DatabaseLayout(root=tmp_path / "example-db")
    store = CatalogRepository(layout=layout)

    catalog = store.bootstrap()

    assert catalog.version == CatalogVersion(1)
    assert store.exists() is True
    assert isinstance(layout.current_catalog_path, Path)
    assert layout.current_catalog_path.exists()
