"""Exercise verified ANA relationship statistics across canonical table changes."""

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from grapharrow.adapters.filesystem import FileSystemAdapter
from grapharrow.core import property_codec
from grapharrow.use_cases import ana_edge_statistics
from grapharrow.use_cases.ana_edge_statistics import EdgePropertyStatisticsCache


def _edges(items: list[tuple[int, str, dict[str, Any]]]) -> pa.Table:
    return pa.table(
        {
            "edge_id": pa.array([item[0] for item in items], type=pa.int64()),
            "rel_type": pa.array([item[1] for item in items], type=pa.string()),
            "properties_json": pa.array(
                [property_codec.dumps(item[2]) for item in items], type=pa.string()
            ),
        }
    )


@pytest.mark.parametrize("storage", ["disk", "memory"])
@pytest.mark.parametrize(
    ("change", "rescanned_ids"),
    [
        ("append", [[7]]),
        ("float", [[1, 2]]),
        ("delete", [[3]]),
        ("drop_range", []),
        ("reorder", []),
        ("empty", []),
    ],
)
def test_only_changed_edge_ranges_are_decoded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    change: str,
    rescanned_ids: list[list[int]],
    storage: str,
) -> None:
    monkeypatch.setattr(ana_edge_statistics, "_EDGE_ID_RANGE_SIZE", 2)
    original = [
        (1, "R", {"p": 1}),
        (2, "R", {"p": None}),
        (3, "S", {"p": 5}),
        (4, "R", {}),
        (5, "T", {"flag": True}),
        (6, "T", {"flag": False}),
    ]
    disk = (tmp_path / "stats.json", FileSystemAdapter()) if storage == "disk" else None
    cache = EdgePropertyStatisticsCache(disk, 16_384, retain_in_memory=storage == "memory")
    cache.discover(_edges(original))

    items = {
        "append": [*original, (7, "R", {"p": "extra"})],
        "float": [(1, "R", {"p": 1.0}), *original[1:]],
        "delete": [*original[:3], *original[4:]],
        "drop_range": original[:4],
        "reorder": list(reversed(original)),
        "empty": [],
    }[change]
    # Change the Arrow chunk boundaries as well as the input rows.
    changed = pa.concat_tables([_edges(items[:1]), _edges(items[1:])])
    expected = EdgePropertyStatisticsCache(None, 0).discover(changed)

    collected: list[list[int]] = []
    collect = ana_edge_statistics._collect_properties

    def record(rows: pa.Table) -> ana_edge_statistics._RangeStats:
        collected.append(rows["edge_id"].to_pylist())
        return collect(rows)

    monkeypatch.setattr(ana_edge_statistics, "_collect_properties", record)
    if disk is not None:
        cache = EdgePropertyStatisticsCache(disk, 16_384)
    actual = cache.discover(changed)
    assert collected == rescanned_ids
    assert actual == expected
    if change == "float":
        assert actual[1][0] == {
            "relType": "R",
            "propertyName": "p",
            "propertyTypes": ["Any", "Float"],
            "mandatory": False,
        }
    elif change == "drop_range":
        assert actual[0] == ["R", "S"]


@pytest.mark.parametrize(
    "condition", ["disabled", "oversized", "cannot_fit", "malformed", "checksum", "write_error"]
)
def test_edge_statistics_fall_back_to_canonical_rows(tmp_path: Path, condition: str) -> None:
    table = _edges([(1, "R", {"p": 1}), (2, "R", {"p": 2})])
    expected = EdgePropertyStatisticsCache(None, 0).discover(table)
    path = tmp_path / "stats.json"
    disk = path, FileSystemAdapter()
    initial = EdgePropertyStatisticsCache(disk, 16_384)
    initial.discover(table)
    valid = path.read_bytes()
    if condition == "disabled":
        path.unlink()
        budget = 0
    elif condition == "oversized":
        budget = len(valid) - 1
    elif condition == "cannot_fit":
        path.unlink()
        budget = 10
    else:
        budget = 16_384
        if condition == "malformed":
            path.write_bytes(b"{")
        elif condition == "checksum":
            path.write_bytes(valid.replace(b'"Integer"', b'"String"'))
        else:
            path.unlink()
            path.mkdir()

    assert EdgePropertyStatisticsCache(disk, budget).discover(table) == expected
    if condition in {"disabled", "cannot_fit"}:
        assert not path.exists()
    elif condition == "write_error":
        assert path.is_dir()
    else:
        assert path.read_bytes() == valid
