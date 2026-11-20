"""Keep active ANA node lookups shared and bound retention between sessions."""

from weakref import ref

import pyarrow as pa
import pytest

from grapharrow.domain import DatasetFile
from grapharrow.use_cases.ana_node_lookup import AnaNodeLookupCache


@pytest.mark.parametrize("max_bytes", [0, 128, 4_096])
def test_ana_node_lookup_retains_only_a_recent_entry_that_fits(max_bytes: int) -> None:
    table = pa.table({"node_id": [1], "labels": [["Person"]]})
    first = (DatasetFile("first.parquet", 1, "nodes"),)
    second = (DatasetFile("second.parquet", 1, "nodes"),)
    cache = AnaNodeLookupCache(max_bytes)

    lookup = cache.get_or_create(first, table)
    assert cache.get_or_create(first, table) is lookup
    first_lookup = ref(lookup)
    del lookup
    assert (first_lookup() is not None) == (max_bytes == 4_096)

    lookup = cache.get_or_create(second, table)
    assert first_lookup() is None
    assert lookup.positions == {1: 0}
