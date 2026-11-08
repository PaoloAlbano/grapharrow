"""Storage-neutral conjunctive predicates offered to a dataset reader."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

ScanOperator = Literal["=", "!=", "<", "<=", ">", ">=", "in", "is_null", "is_not_null"]


@dataclass(frozen=True, slots=True)
class ScanPredicate:
    """An optional prefilter; callers must retain their authoritative residual filter."""

    column: str
    operator: ScanOperator
    value: object = None
