from pathlib import Path

import pytest

from grapharrow import Database
from grapharrow.core.errors import SchemaError
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


def test_required_label_property_is_enforced_on_write(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")
    database.initialize()
    database.write_catalog(
        VersionedCatalog(
            version=CatalogVersion(2),
            labels=(LabelDefinition(id=LabelId(1), name="Person"),),
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
    )

    with database.write_transaction() as session:
        with pytest.raises(SchemaError, match="required"):
            session.add_node(labels=["Person"], properties={"external_id": "alice"})


def test_unique_label_property_is_enforced_on_write(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")
    database.initialize()
    database.write_catalog(
        VersionedCatalog(
            version=CatalogVersion(2),
            labels=(LabelDefinition(id=LabelId(1), name="Person"),),
            properties=(
                PropertyDefinition(
                    name="external_id",
                    owner_kind="label",
                    owner_name="Person",
                    data_type="string",
                    nullable=False,
                ),
            ),
            constraints=(
                ConstraintDefinition(
                    kind="unique",
                    owner_kind="label",
                    owner_name="Person",
                    property_name="external_id",
                ),
            ),
        )
    )

    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"external_id": "alice"})

    with database.write_transaction() as session:
        with pytest.raises(SchemaError, match="Duplicate value"):
            session.add_node(labels=["Person"], properties={"external_id": "alice"})


def test_relationship_endpoint_and_required_property_constraints_are_enforced(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "example-db")
    database.initialize()
    database.write_catalog(
        VersionedCatalog(
            version=CatalogVersion(2),
            labels=(
                LabelDefinition(id=LabelId(1), name="Person"),
                LabelDefinition(id=LabelId(2), name="Company"),
            ),
            relationship_types=(
                RelationshipTypeDefinition(id=RelationshipTypeId(1), name="WORKS_AT"),
            ),
            properties=(
                PropertyDefinition(
                    name="created_at",
                    owner_kind="relationship_type",
                    owner_name="WORKS_AT",
                    data_type="timestamp",
                    nullable=False,
                ),
            ),
            constraints=(
                ConstraintDefinition(
                    kind="required",
                    owner_kind="relationship_type",
                    owner_name="WORKS_AT",
                    property_name="created_at",
                ),
                ConstraintDefinition(
                    kind="endpoint",
                    owner_kind="relationship_type",
                    owner_name="WORKS_AT",
                    source_labels=("Person",),
                    target_labels=("Company",),
                ),
            ),
        )
    )

    with database.write_transaction() as session:
        person = session.add_node(labels=["Person"], properties={"name": "Alice"})
        company = session.add_node(labels=["Company"], properties={"name": "Acme"})

        with pytest.raises(SchemaError, match="required"):
            session.add_edge(
                src=person["id"],
                rel_type="WORKS_AT",
                dst=company["id"],
                properties={},
            )

        session.add_edge(
            src=person["id"],
            rel_type="WORKS_AT",
            dst=company["id"],
            properties={"created_at": "2026-03-31T10:00:00Z"},
        )

        with pytest.raises(SchemaError, match="source labels"):
            session.add_edge(
                src=company["id"],
                rel_type="WORKS_AT",
                dst=person["id"],
                properties={"created_at": "2026-03-31T10:00:00Z"},
            )
