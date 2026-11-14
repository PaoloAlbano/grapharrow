"""Serialize stored properties without conflating Cypher temporals and JSON."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from grapharrow.core.temporal import Duration, Instant, TemporalResult

_TEMPORAL_PREFIX = "T1:"


def contains_temporal(value: object) -> bool:
    if isinstance(value, Instant | Duration):
        return True
    if isinstance(value, Mapping):
        return any(contains_temporal(item) for item in value.values())
    if isinstance(value, list | tuple):
        return any(contains_temporal(item) for item in value)
    return False


def dumps(value: Any) -> str:
    temporals: list[list[Any]] = []

    def pack(item: Any, path: list[str | int]) -> Any:
        if isinstance(item, Instant):
            temporals.append([path, item.kind, item.day, item.clock, item.offset, item.zone])
            return None
        if isinstance(item, Duration):
            temporals.append([path, "duration", item.months, item.days, item.nanos])
            return None
        if isinstance(item, Mapping):
            return {key: pack(element, [*path, key]) for key, element in sorted(item.items())}
        if isinstance(item, list | tuple):
            return [pack(element, [*path, index]) for index, element in enumerate(item)]
        return item

    def encode_temporal(item: object) -> Any:
        if isinstance(item, Instant | Duration):
            raise _TemporalProperty
        raise TypeError(f"Object of type {type(item).__name__} is not JSON serializable")

    try:
        return json.dumps(value, sort_keys=True, default=encode_temporal)
    except _TemporalProperty:
        return _TEMPORAL_PREFIX + json.dumps([pack(value, []), temporals], sort_keys=True)


def loads(payload: str, *, public: bool = False) -> Any:
    if not isinstance(payload, str) or not payload.startswith(_TEMPORAL_PREFIX):
        return json.loads(payload)
    value, temporals = json.loads(payload[len(_TEMPORAL_PREFIX) :])
    for path, kind, *components in temporals:
        temporal: Instant | Duration | TemporalResult
        temporal = Duration(*components) if kind == "duration" else Instant(kind, *components)
        if public:
            temporal = TemporalResult(temporal)
        if not path:
            return temporal
        parent = value
        for part in path[:-1]:
            parent = parent[part]
        parent[path[-1]] = temporal
    return value


class _TemporalProperty(Exception):
    """Select the typed encoding when the JSON encoder encounters a temporal."""
