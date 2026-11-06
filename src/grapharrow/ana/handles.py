"""Workflow-local rows, opaque entity identity and handle lineage."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4


@dataclass(frozen=True)
class EntityReference:
    owner: str
    key: str


class EntityPayload(dict[str, Any]):
    """JSON-visible entity fields with an out-of-band identity reference."""

    reference: EntityReference | None = None


def entity_identity(value: Any) -> EntityReference | None:
    return value.reference if isinstance(value, EntityPayload) else None


def _columns(rows: list[dict[str, Any]]) -> list[str]:
    return list(dict.fromkeys(key for row in rows for key in row if not key.startswith("__")))


def display(value: Any) -> Any:
    if isinstance(value, EntityPayload):
        props = value["properties"]
        return props.get("name", props.get("id", props))
    return value


def _preview(
    rows: list[dict[str, Any]],
    focus: str | None,
    limit: int = 10,
    columns: list[str] | None = None,
) -> list[Any]:
    return [
        display(row.get(focus))
        if focus
        else {key: display(row.get(key)) for key in columns or _columns([row])}
        for row in rows[:limit]
    ]


@dataclass
class Handle:
    id: str
    rows: list[dict[str, Any]]
    focus: str | None
    kind: str
    columns: list[str]
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def variables(self) -> list[str]:
        return list(
            dict.fromkeys(
                name
                for row in self.rows
                for name, value in row.items()
                if isinstance(value, EntityPayload)
            )
        )


class HandleStore:
    """Retain materialized results within one workflow."""

    def __init__(self) -> None:
        self.namespace = uuid4().hex
        self.counter = 0
        self.handles: dict[str, Handle] = {}
        self.lineage: dict[str, list[dict[str, Any]]] = {}

    def store(
        self,
        rows: list[dict[str, Any]],
        *,
        focus: str | None,
        kind: str,
        columns: list[str] | None,
        metadata: dict[str, Any] | None,
    ) -> dict[str, Any]:
        self.counter += 1
        name = f"h{self.counter}"
        handle = Handle(
            name, rows, focus, kind, _columns(rows) if columns is None else columns, metadata or {}
        )
        self.handles[name] = handle
        entity_hint = (
            "Use the entity variables in this handle for additional graph operations."
            if handle.variables
            else "Fetch or aggregate these scalar rows."
        )
        return {
            **self.summary(handle),
            "columns": handle.columns if kind == "table" else [],
            "matched_count": len(rows),
            "truncated": len(rows) > 10,
            "composition_hint": entity_hint,
        }

    def get(self, name: str) -> Handle:
        try:
            return self.handles[name]
        except KeyError:
            latest = next(reversed(self.handles), None)
            hint = f"; latest available handle is {latest!r}" if latest else ""
            raise ValueError(f"Unknown handle {name!r}{hint}") from None

    def summary(self, handle: Handle) -> dict[str, Any]:
        return {
            "handle": handle.id,
            "kind": handle.kind,
            "focus": handle.focus,
            "row_count": len(handle.rows),
            "entity_variables": handle.variables,
            "columns": handle.columns,
            "preview": _preview(handle.rows, handle.focus, columns=handle.columns),
        }

    def record_lineage(self, tool: str, args: dict[str, Any], result: dict[str, Any]) -> None:
        handle = result.get("handle")
        if handle not in self.handles or tool in {"fetch", "handle_recap"}:
            return
        inputs = [args.get(key) for key in ("from", "left", "right", "seed_from")]
        inputs.extend(
            item.get("handle") for item in args.get("operands") or [] if isinstance(item, dict)
        )
        inputs.extend(
            item.get("handle") for item in args.get("inputs") or [] if isinstance(item, dict)
        )
        parents = list(
            dict.fromkeys(name for name in inputs if isinstance(name, str) and name in self.handles)
        )
        lineage: list[dict[str, Any]] = []
        for name in parents:
            for step in self.lineage.get(name, []):
                if step not in lineage:
                    lineage.append(step)
        lineage.append(
            {
                "tool": tool,
                "args": args,
                "parents": parents,
                "produced": self.summary(self.handles[handle]),
            }
        )
        self.lineage[handle] = lineage[-24:]

    def clear(self) -> None:
        self.handles.clear()
        self.lineage.clear()
