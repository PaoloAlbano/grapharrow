import pytest

from grapharrow.domain import (
    CatalogVersion,
    EdgeId,
    LabelId,
    NodeId,
    RelationshipTypeId,
    SnapshotId,
)


@pytest.mark.parametrize(
    ("identifier_type", "value"),
    [
        (NodeId, 1),
        (EdgeId, 2),
        (LabelId, 3),
        (RelationshipTypeId, 4),
        (CatalogVersion, 5),
        (SnapshotId, 6),
    ],
)
def test_identifiers_accept_non_negative_values(
    identifier_type: type[object],
    value: int,
) -> None:
    identifier = identifier_type(value)  # type: ignore[call-arg]

    assert identifier.value == value  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    "identifier_type",
    [NodeId, EdgeId, LabelId, RelationshipTypeId, CatalogVersion, SnapshotId],
)
def test_identifiers_reject_negative_values(identifier_type: type[object]) -> None:
    with pytest.raises(ValueError):
        identifier_type(-1)  # type: ignore[call-arg]
