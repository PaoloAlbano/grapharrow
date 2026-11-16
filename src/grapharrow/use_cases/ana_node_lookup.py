"""Share immutable ANA node lookups across workflows reading the same node files."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from threading import Lock
from types import MappingProxyType
from weakref import WeakValueDictionary

import pyarrow as pa  # type: ignore[import-untyped]

from grapharrow.domain import DatasetFile
from grapharrow.use_cases.ana_resources import retained_size


@dataclass(frozen=True, slots=True, weakref_slot=True)
class AnaNodeLookup:
    positions: Mapping[int, int]
    label_ids: Mapping[str, tuple[int, ...]]
    retained_bytes: int


class AnaNodeLookupCache:
    """Keep lookups while at least one workflow uses the immutable node files."""

    def __init__(self) -> None:
        self._entries: WeakValueDictionary[tuple[DatasetFile, ...], AnaNodeLookup] = (
            WeakValueDictionary()
        )
        self._lock = Lock()

    def get_or_create(self, files: tuple[DatasetFile, ...], nodes: pa.Table) -> AnaNodeLookup:
        with self._lock:
            lookup = self._entries.get(files)
            if lookup is None:
                lookup = _build_lookup(nodes)
                self._entries[files] = lookup
            return lookup


def _build_lookup(nodes: pa.Table) -> AnaNodeLookup:
    positions = {}
    labels: dict[str, list[int]] = defaultdict(list)
    for position, (node_id, node_labels) in enumerate(
        zip(nodes["node_id"].to_pylist(), nodes["labels"].to_pylist(), strict=True)
    ):
        positions[node_id] = position
        for label in node_labels:
            labels[label].append(node_id)
    label_ids = {label: tuple(ids) for label, ids in labels.items()}
    return AnaNodeLookup(
        MappingProxyType(positions),
        MappingProxyType(label_ids),
        retained_size((positions, label_ids)),
    )
