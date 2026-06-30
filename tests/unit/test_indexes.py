from pathlib import Path

import pytest

from grapharrow.core import DatabaseLayout
from grapharrow.core.errors import CatalogError
from grapharrow.domain import CatalogVersion
from grapharrow.repositories import CatalogRepository
from grapharrow.use_cases import IndexCatalogUseCase


def test_create_index_adds_a_catalog_declaration(tmp_path: Path) -> None:
    store = CatalogRepository(DatabaseLayout(root=tmp_path / "example-db"))
    use_case = IndexCatalogUseCase(catalog_store=store)

    index = use_case.create_index(
        name="person_name_idx",
        kind="range",
        owner_kind="label",
        owner_name="Person",
        property_names=("name",),
    )

    catalog = store.load()
    assert index.name == "person_name_idx"
    assert catalog.version == CatalogVersion(2)
    assert catalog.indexes == (index,)
    assert use_case.list_indexes() == (index,)


def test_create_index_if_not_exists_returns_matching_existing_index(tmp_path: Path) -> None:
    store = CatalogRepository(DatabaseLayout(root=tmp_path / "example-db"))
    use_case = IndexCatalogUseCase(catalog_store=store)
    first_index = use_case.create_index(
        name="person_name_idx",
        kind="text",
        owner_kind="label",
        owner_name="Person",
        property_names=("name",),
    )

    second_index = use_case.create_index(
        name="person_name_idx",
        kind="text",
        owner_kind="label",
        owner_name="Person",
        property_names=("name",),
        if_not_exists=True,
    )

    assert second_index == first_index
    assert store.load().version == CatalogVersion(2)


def test_create_index_rejects_duplicate_name_or_schema(tmp_path: Path) -> None:
    store = CatalogRepository(DatabaseLayout(root=tmp_path / "example-db"))
    use_case = IndexCatalogUseCase(catalog_store=store)
    use_case.create_index(
        name="person_name_idx",
        kind="range",
        owner_kind="label",
        owner_name="Person",
        property_names=("name",),
    )

    with pytest.raises(CatalogError, match="Index already exists"):
        use_case.create_index(
            name="person_name_idx",
            kind="range",
            owner_kind="label",
            owner_name="Person",
            property_names=("age",),
        )

    with pytest.raises(CatalogError, match="Index already exists"):
        use_case.create_index(
            name="person_name_idx_2",
            kind="range",
            owner_kind="label",
            owner_name="Person",
            property_names=("name",),
        )


def test_create_index_validates_required_fields(tmp_path: Path) -> None:
    store = CatalogRepository(DatabaseLayout(root=tmp_path / "example-db"))
    use_case = IndexCatalogUseCase(catalog_store=store)

    with pytest.raises(CatalogError, match="Index name cannot be empty"):
        use_case.create_index(
            name=" ",
            kind="range",
            owner_kind="label",
            owner_name="Person",
            property_names=("name",),
        )

    with pytest.raises(CatalogError, match="at least one property"):
        use_case.create_index(
            name="person_name_idx",
            kind="range",
            owner_kind="label",
            owner_name="Person",
            property_names=(),
        )


def test_drop_index_removes_catalog_declaration_and_versions_catalog(tmp_path: Path) -> None:
    store = CatalogRepository(DatabaseLayout(root=tmp_path / "example-db"))
    use_case = IndexCatalogUseCase(catalog_store=store)
    index = use_case.create_index(
        name="person_name_idx",
        kind="range",
        owner_kind="label",
        owner_name="Person",
        property_names=("name",),
    )

    removed = use_case.drop_index(name="person_name_idx")

    assert removed == index
    assert store.load().version == CatalogVersion(3)
    assert use_case.list_indexes() == ()


def test_drop_index_if_exists_is_idempotent_for_missing_indexes(tmp_path: Path) -> None:
    store = CatalogRepository(DatabaseLayout(root=tmp_path / "example-db"))
    use_case = IndexCatalogUseCase(catalog_store=store)

    removed = use_case.drop_index(name="missing_idx", if_exists=True)

    assert removed is None
    assert store.load().version == CatalogVersion(1)


def test_drop_index_rejects_missing_index_without_if_exists(tmp_path: Path) -> None:
    store = CatalogRepository(DatabaseLayout(root=tmp_path / "example-db"))
    use_case = IndexCatalogUseCase(catalog_store=store)

    with pytest.raises(CatalogError, match="Unknown index"):
        use_case.drop_index(name="missing_idx")
