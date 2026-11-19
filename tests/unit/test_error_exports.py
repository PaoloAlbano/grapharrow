"""Compatibility checks for applications importing GraphArrow exceptions."""

from grapharrow import errors
from grapharrow.core import errors as core_errors


def test_legacy_error_names_refer_to_the_core_exception_classes() -> None:
    for name in (
        "CatalogError",
        "GraphArrowError",
        "NotFoundError",
        "QueryError",
        "SchemaError",
        "StorageError",
        "TransactionError",
    ):
        assert name in errors.__all__
        assert getattr(errors, name) is getattr(core_errors, name)
