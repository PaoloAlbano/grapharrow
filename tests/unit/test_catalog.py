import pytest

from grapharrow.core.errors import CatalogError
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


def test_catalog_can_resolve_label_and_relationship_type() -> None:
    catalog = VersionedCatalog(
        version=CatalogVersion(1),
        labels=(LabelDefinition(id=LabelId(1), name="Person"),),
        relationship_types=(RelationshipTypeDefinition(id=RelationshipTypeId(1), name="KNOWS"),),
    )

    assert catalog.get_label("Person").id == LabelId(1)
    assert catalog.get_relationship_type("KNOWS").id == RelationshipTypeId(1)


def test_catalog_raises_for_unknown_entries() -> None:
    catalog = VersionedCatalog(version=CatalogVersion(1))

    with pytest.raises(CatalogError):
        catalog.get_label("Missing")

    with pytest.raises(CatalogError):
        catalog.get_relationship_type("Missing")


def test_catalog_can_filter_properties_by_owner() -> None:
    catalog = VersionedCatalog(
        version=CatalogVersion(1),
        properties=(
            PropertyDefinition(
                name="name",
                owner_kind="label",
                owner_name="Person",
                data_type="string",
            ),
            PropertyDefinition(
                name="created_at",
                owner_kind="relationship_type",
                owner_name="KNOWS",
                data_type="timestamp",
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

    properties = catalog.properties_for_owner(owner_kind="label", owner_name="Person")

    assert [definition.name for definition in properties] == ["name"]
