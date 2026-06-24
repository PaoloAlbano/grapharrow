from pathlib import Path

from grapharrow.core import DatabaseLayout
from grapharrow.domain import (
    CatalogVersion,
    ConstraintDefinition,
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
    )

    store.write(catalog)
    loaded_catalog = store.load()

    assert loaded_catalog == catalog


def test_catalog_store_bootstrap_creates_the_initial_catalog(tmp_path: Path) -> None:
    layout = DatabaseLayout(root=tmp_path / "example-db")
    store = CatalogRepository(layout=layout)

    catalog = store.bootstrap()

    assert catalog.version == CatalogVersion(1)
    assert store.exists() is True
    assert isinstance(layout.current_catalog_path, Path)
    assert layout.current_catalog_path.exists()
