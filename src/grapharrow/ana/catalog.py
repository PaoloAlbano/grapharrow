"""Local tool schemas and argument validation for the public ANA v0 protocol."""

from __future__ import annotations

import json
from collections.abc import Callable
from copy import deepcopy
from importlib.resources import files
from typing import Any, cast

_TOOLS = cast(
    list[dict[str, Any]],
    json.loads(files(__package__).joinpath("tool_schemas.json").read_text(encoding="utf-8")),
)
_BY_NAME = {tool["name"]: tool for tool in _TOOLS}
SUPPORTED_ANA_TOOLS = frozenset(_BY_NAME)
_SCHEMA_MODES = {
    "overview",
    "overview_light",
    "targeted_first",
    "overview_expand_first",
    "overview_light_expand_first",
}


def get_tool_contract(status: str | None = None) -> list[dict[str, Any]]:
    return [
        {
            "name": tool["name"],
            "status": tool["status"],
            "profile": tool["profile"],
            "purpose": tool["description"],
            "when_to_use": tool["description"],
        }
        for tool in sorted(_TOOLS, key=lambda item: item["name"])
        if status is None or status == tool["status"]
    ]


def get_tool_specs(
    *,
    schema_entry: str = "overview",
    enable_planning_tools: bool = False,
) -> list[dict[str, Any]]:
    if schema_entry not in _SCHEMA_MODES:
        raise ValueError(f"Unknown ANA schema entry: {schema_entry!r}")
    specs: list[dict[str, Any]] = [
        {
            "type": "function",
            "function": {
                "name": tool["name"],
                "description": tool["description"],
                "parameters": deepcopy(tool["parameters"]),
            },
        }
        for tool in _TOOLS
        if enable_planning_tools or tool["availability"] == "default"
    ]
    if schema_entry == "targeted_first":
        specs.sort(key=lambda spec: spec["function"]["name"] != "schema_search")
    if "light" in schema_entry:
        for spec in specs:
            if spec["function"]["name"] == "schema_overview":
                spec["function"]["description"] = "Get a compact graph schema map."
    if "expand_first" in schema_entry:
        for spec in specs:
            if spec["function"]["name"] == "expand":
                spec["function"]["description"] += " Prefer one bounded hop at a time."
    return specs


def _has_type(value: Any, schema: dict[str, Any]) -> bool:
    if "anyOf" in schema:
        return any(_has_type(value, choice) for choice in schema["anyOf"])
    expected = schema.get("type")
    if isinstance(expected, list):
        return any(_has_type(value, {"type": kind}) for kind in expected)
    checks: dict[str, Callable[[Any], bool]] = {
        "array": lambda item: isinstance(item, list),
        "object": lambda item: isinstance(item, dict),
        "string": lambda item: isinstance(item, str),
        "integer": lambda item: type(item) is int,
        "number": lambda item: type(item) in (int, float),
        "boolean": lambda item: type(item) is bool,
        "null": lambda item: item is None,
    }
    return checks[expected](value) if expected in checks else True


def validate_args(name: str, args: dict[str, Any]) -> dict[str, Any]:
    if name not in _BY_NAME:
        raise ValueError(f"Unknown tool: {name}")
    if not isinstance(args, dict):
        raise ValueError(f"{name}: arguments must be an object")
    schema = _BY_NAME[name]["parameters"]
    properties = schema.get("properties", {})
    normalized: dict[str, Any] = {}
    for key, value in args.items():
        public = {"from_": "from", "as_": "as"}.get(key, key)
        if public not in properties:
            raise ValueError(f"{name}: unsupported parameter {key!r}; use {', '.join(properties)}")
        if public in normalized:
            raise ValueError(f"{name}: conflicting aliases for {public!r}")
        spec = properties[public]
        if value is not None and not _has_type(value, spec):
            raise ValueError(f"{name}: invalid type for {public!r}")
        if value is not None and "enum" in spec and value not in spec["enum"]:
            raise ValueError(f"{name}: unsupported value for {public!r}")
        normalized[public] = value
    if name in {"expand", "expand_aggregate", "optional_expand_count"}:
        normalized.setdefault("direction", "out")
    missing = [key for key in schema.get("required", []) if key not in normalized]
    if missing:
        raise ValueError(f"{name}: required parameters missing: {', '.join(missing)}")
    return {
        key: value
        for key, value in normalized.items()
        if value is not None or key in schema.get("required", [])
    }
