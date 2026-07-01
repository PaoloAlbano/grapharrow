"""Logical plan primitives for the Cypher read path."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypeAlias


@dataclass(frozen=True, slots=True)
class NodeScanPlan:
    variable: str
    labels: tuple[str, ...]
    properties: dict[str, object]
    requested_fields: set[str] | None
    pushdown_predicate: object | None = None


@dataclass(frozen=True, slots=True)
class ExpandPlan:
    input: LogicalPlan
    start_variable: str
    edge_variable: str
    previous_edge_variables: tuple[str, ...]
    direction: str
    rel_type: str | None
    rel_type_alternatives: tuple[str, ...]
    edge_properties: dict[str, object]
    edge_requested_fields: set[str] | None
    end_variable: str
    end_labels: tuple[str, ...]
    end_properties: dict[str, object]
    end_requested_fields: set[str] | None
    edge_pushdown_predicate: object | None = None
    end_pushdown_predicate: object | None = None


@dataclass(frozen=True, slots=True)
class FilterPlan:
    input: LogicalPlan
    predicate: object


@dataclass(frozen=True, slots=True)
class AggregateCountPlan:
    input: LogicalPlan
    return_items: tuple[object, ...]


@dataclass(frozen=True, slots=True)
class OptionalMatchPlan:
    input: LogicalPlan | None
    patterns: tuple[object, ...]
    where_predicate: object | None
    required_fields: dict[str, set[str]] | None
    null_columns: tuple[str, ...] | None


@dataclass(frozen=True, slots=True)
class WithAggregatePlan:
    input: LogicalPlan
    grouping_items: tuple[object, ...]
    aggregate_items: tuple[object, ...]


@dataclass(frozen=True, slots=True)
class WithProjectPlan:
    input: LogicalPlan
    with_items: tuple[object, ...]


@dataclass(frozen=True, slots=True)
class DistinctPlan:
    input: LogicalPlan


@dataclass(frozen=True, slots=True)
class ReturnProjectPlan:
    input: LogicalPlan
    return_items: tuple[object, ...]


@dataclass(frozen=True, slots=True)
class ProjectPlan:
    input: LogicalPlan
    columns: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SortPlan:
    input: LogicalPlan
    order_by: tuple[object, ...]


@dataclass(frozen=True, slots=True)
class SlicePlan:
    input: LogicalPlan
    skip: int | None
    limit: int | None


LogicalPlan: TypeAlias = (
    NodeScanPlan
    | ExpandPlan
    | FilterPlan
    | AggregateCountPlan
    | OptionalMatchPlan
    | WithAggregatePlan
    | WithProjectPlan
    | DistinctPlan
    | ReturnProjectPlan
    | ProjectPlan
    | SortPlan
    | SlicePlan
)


@dataclass(frozen=True, slots=True)
class MatchLogicalPlan:
    root: LogicalPlan
    planned_order_window: bool
    planned_distinct: bool = False
    debug_name: str = "match-node-scan"


@dataclass(frozen=True, slots=True)
class MatchLogicalPlanResult:
    table: Any | None
    query_result: Any | None
    planned_order_window: bool
    planned_distinct: bool
