"""Typed, application-local read procedure definitions for Cypher."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Literal

from grapharrow.core.errors import QueryError

ProcedureType = Literal["ANY", "BOOLEAN", "FLOAT", "INTEGER", "NUMBER", "STRING"]


@dataclass(frozen=True, slots=True)
class ProcedureField:
    name: str
    type: ProcedureType
    nullable: bool = True

    def coerce(self, value: Any, *, phase: Literal["compile", "runtime"]) -> Any:
        """Validate a value against this signature field and widen integers to floats."""
        if value is None:
            if self.nullable:
                return None
        elif self.type == "ANY":
            return value
        elif self.type == "FLOAT" and type(value) in {int, float}:
            return float(value)
        elif self.type == "NUMBER" and type(value) in {int, float}:
            return value
        elif type(value) is {"BOOLEAN": bool, "INTEGER": int, "STRING": str}.get(self.type):
            return value
        raise QueryError.invalid_type(
            f"Procedure field {self.name!r} requires {self.type}.", phase=phase
        )


@dataclass(frozen=True, slots=True)
class ReadProcedure:
    inputs: tuple[ProcedureField, ...]
    outputs: tuple[ProcedureField, ...]
    handler: Callable[[tuple[Any, ...]], Iterable[Mapping[str, Any]]]

    def __post_init__(self) -> None:
        for fields in (self.inputs, self.outputs):
            names = {field.name for field in fields}
            if "" in names or len(names) != len(fields):
                raise ValueError("Procedure field names must be nonempty and unique.")
