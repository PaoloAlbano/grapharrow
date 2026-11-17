"""Serialize stored properties without conflating Cypher temporals and JSON."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from typing import Any

import orjson

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


def loads_for_schema(payload: str) -> Any:
    """Decode canonical property objects quickly while preserving observed types."""
    if payload.startswith("{"):
        try:
            properties = orjson.loads(payload)
            if not _has_ambiguous_float(properties.values()):
                return properties
        except (orjson.JSONDecodeError, RecursionError):
            pass
    return loads(payload)


def _has_ambiguous_float(values: Iterable[Any]) -> bool:
    # orjson can promote integers outside its native range to float.
    for value in values:
        if type(value) is float and abs(value) >= 2**63:
            return True
        if isinstance(value, list) and _has_ambiguous_float(value):
            return True
        if isinstance(value, dict) and _has_ambiguous_float(value.values()):
            return True
    return False


class _TemporalProperty(Exception):
    """Select the typed encoding when the JSON encoder encounters a temporal."""
