"""Schema discovery and bounded planning over the current workflow snapshot."""

from __future__ import annotations

import re
from collections import deque
from typing import Any

from grapharrow.ana.catalog import SUPPORTED_ANA_TOOLS, validate_args
from grapharrow.ana.values import _int_arg, _validate_membership_operand


class SchemaTools:
    _schema: dict[str, Any]

    def _label_counts(self, labels: list[str]) -> dict[str, int]:
        raise NotImplementedError

    def _node_property_samples(self, label: str, prop: str, *, limit: int = 3) -> list[Any]:
        raise NotImplementedError

    def _relationship_property_samples(
        self,
        rel_type: str,
        prop: str,
        *,
        limit: int = 3,
    ) -> list[Any]:
        raise NotImplementedError

    def _properties(self, context: dict[str, Any]) -> dict[str, list[str]]:
        if context["kind"] == "node":
            return {
                entry["propertyName"]: entry["propertyTypes"]
                for entry in self._schema["node_properties"]
                if context.get("label") in entry["nodeLabels"]
            }
        return {
            entry["propertyName"]: entry["propertyTypes"]
            for entry in self._schema["relationship_properties"]
            if context.get("relationship_type") == entry["relType"]
        }

    def _normalize_filter_for_context(
        self,
        item: dict[str, Any],
        *,
        contexts: dict[str, Any],
        context: str,
    ) -> dict[str, Any]:
        binding = contexts.get(str(item.get("var")))
        if binding is None or not item.get("property"):
            raise ValueError(f"{context}: filter requires a known variable and property")
        spec = self._properties(binding)
        prop = item["property"]
        op = str(item.get("op", "eq"))
        if op in {"eq", "neq"} and any(kind.startswith("List<") for kind in spec.get(prop, [])):
            if not isinstance(item.get("value"), list):
                op = "in" if op == "eq" else "not_in"
        _validate_membership_operand(op, item.get("value"))
        return {**item, "op": op}

    def _validate_projection_context_properties(
        self,
        args: dict[str, Any],
        contexts: dict[str, Any],
        *,
        context: str,
    ) -> None:
        for key in ("select", "group_by", "order_by"):
            for item in args.get(key) or []:
                if "var" in item and item["var"] not in contexts:
                    raise ValueError(f"{context}: {key} references an unknown variable")
                if key != "order_by" and not item.get("property"):
                    raise ValueError(f"{context}: {key} requires a property")

    def _validate_relationship_pattern(
        self,
        *,
        source_label: str,
        relationship_type: str,
        direction: str,
        target_label: str,
    ) -> None:
        if direction not in {"out", "in", "both"}:
            raise ValueError(f"Unsupported direction: {direction}")
        if relationship_type not in self._schema["relationship_types"]:
            raise ValueError(f"Unknown relationship_type {relationship_type!r}")

    def _label_entry(self, label: str, count: int) -> dict[str, Any]:
        names = list(self._properties({"kind": "node", "label": label}))
        return {
            "label": label,
            "count": count,
            "property_names": names,
            "lookup_properties": [key for key in ("name", "id", "aliases") if key in names],
        }

    def _traversals(self, label: str) -> list[dict[str, str]]:
        traversals = []
        for edge in self._schema["label_relationships"]:
            for current, adjacent, direction in (
                ("source_label", "target_label", "out"),
                ("target_label", "source_label", "in"),
            ):
                if edge[current] == label:
                    traversals.append(
                        {
                            "relationship_type": edge["relationship_type"],
                            "direction": direction,
                            "target_label": edge[adjacent],
                        }
                    )
        return traversals

    def _navigation(self, edge: dict[str, str]) -> dict[str, Any]:
        source, rel, target = (
            edge["source_label"],
            edge["relationship_type"],
            edge["target_label"],
        )
        return {
            "pattern": f"(:{source})-[:{rel}]->(:{target})",
            "relationship_type": rel,
            "direction_rule": f"Traverse out from {source} and in from {target}.",
            "forward_expand": {**edge, "direction": "out"},
            "reverse_expand": {
                "source_label": target,
                "relationship_type": rel,
                "direction": "in",
                "target_label": source,
            },
            "semantic_direction_examples": [
                {
                    "phrase_shape": f"{source} to {target}",
                    "meaning": "Follow the stored direction",
                    "use": {"source_label": source, "direction": "out", "target_label": target},
                },
                {
                    "phrase_shape": f"{target} to {source}",
                    "meaning": "Follow the reverse direction",
                    "use": {"source_label": target, "direction": "in", "target_label": source},
                },
            ],
        }

    def _temporal(self, rel: str) -> list[dict[str, Any]]:
        properties = self._properties({"kind": "relationship", "relationship_type": rel})
        start = next(
            (key for key in ("since", "valid_from", "start_date") if key in properties), None
        )
        end = next((key for key in ("until", "valid_to", "end_date") if key in properties), None)
        if not start and not end:
            return []
        filters = [
            {"property": name, "op": op, "value": "YEAR"}
            for name, op in ((start, "lte"), (end, "gte_or_null"))
            if name
        ]
        return [
            {
                "relationship_type": rel,
                "kind": "interval",
                "granularity": "date",
                "start_property": start,
                "end_property": end,
                "active_at_year_rule": "Check the relationship's available time bounds.",
                "active_at_year_filter_template": filters,
                "tool_hint": "Use relationship properties to constrain the traversal date.",
            }
        ]

    def _schema_overview(self, _: dict[str, Any]) -> dict[str, Any]:
        counts = self._label_counts(self._schema["labels"])
        labels = []
        for label, count in counts.items():
            entry = self._label_entry(label, count)
            entry["safe_start_score"] = (10 if entry["lookup_properties"] else 1) / max(count, 1)
            labels.append(entry)
        starts = [
            {
                "label": item["label"],
                "count": item["count"],
                "lookup_properties": item["lookup_properties"],
                "reason": "Selective starting label",
            }
            for item in sorted(labels, key=lambda item: -item["safe_start_score"])[:8]
        ]
        return {
            "labels": labels,
            "relationship_types": self._schema["relationship_types"],
            "relationship_patterns": self._schema["label_relationships"],
            "relationship_navigation": [
                self._navigation(edge) for edge in self._schema["label_relationships"]
            ],
            "safe_start_points": starts,
            "temporal_relationships": [
                item for rel in self._schema["relationship_types"] for item in self._temporal(rel)
            ],
            "set_operation_patterns": [
                {
                    "intent": "OR",
                    "flow": ["Build entity handles", "Union them", "Fetch the result"],
                },
                {
                    "intent": "AND",
                    "flow": ["Build entity handles", "Intersect them", "Fetch the result"],
                },
            ],
            "next_actions": [
                {
                    "when": "Need matching labels or properties",
                    "tool": "schema_search",
                    "args": {"query": "<search terms>"},
                }
            ],
            "planning_hints": ["Check relationship direction relative to the current entity."],
        }

    def _property_entry(
        self,
        name: str,
        types: list[str],
        examples: list[Any],
        *,
        mandatory: bool = False,
    ) -> dict[str, Any]:
        inferred = next((kind.casefold() for kind in types if kind != "List<Any>"), "any")
        if inferred.startswith("list<"):
            inferred = "list[" + inferred[5:-1] + "]"
            operators = ["in", "not_in", "is_null", "is_not_null"]
        elif inferred in {"integer", "float", "date"}:
            operators = ["eq", "neq", "gt", "gte", "lt", "lte", "is_null", "is_not_null"]
        elif inferred == "string":
            operators = ["eq", "neq", "contains", "not_contains"]
        else:
            operators = ["eq", "neq", "is_null", "is_not_null"]
        return {
            "name": name,
            "tokens": _tokens(name),
            "types": types,
            "mandatory": mandatory,
            "inferred_type": inferred,
            "operators": operators,
            "examples": examples,
        }

    def _label_properties(self, label: str) -> list[dict[str, Any]]:
        return [
            self._property_entry(
                item["propertyName"],
                item["propertyTypes"],
                self._node_property_samples(label, item["propertyName"]),
                mandatory=item.get("mandatory", False),
            )
            for item in self._schema["node_properties"]
            if label in item["nodeLabels"]
        ]

    def _relationship_properties(self, rel: str) -> list[dict[str, Any]]:
        return [
            self._property_entry(
                item["propertyName"],
                item["propertyTypes"],
                self._relationship_property_samples(rel, item["propertyName"]),
                mandatory=item.get("mandatory", False),
            )
            for item in self._schema["relationship_properties"]
            if item["relType"] == rel
        ]

    def _describe_pattern(self, edge: dict[str, str]) -> dict[str, Any]:
        rel = edge["relationship_type"]
        return {
            **edge,
            **self._navigation(edge),
            "tokens": _tokens(rel),
            "properties": self._relationship_properties(rel),
            "temporal": self._temporal(rel),
        }

    def _schema_describe_label(self, args: dict[str, Any]) -> dict[str, Any]:
        label = args["label"]
        if label not in self._schema["labels"]:
            raise ValueError(f"Unknown label: {label!r}")
        count = self._label_counts([label])[label]
        lookup = self._label_entry(label, count)["lookup_properties"]
        return {
            "label": label,
            "tokens": _tokens(label),
            "count": count,
            "lookup_properties": lookup,
            "properties": self._label_properties(label),
            "safe_start_score": (10 if lookup else 1) / max(count, 1),
            "relationship_traversals": [
                self._describe_pattern(edge)
                for edge in self._schema["label_relationships"]
                if label in (edge["source_label"], edge["target_label"])
            ],
            "ready_to_use": {
                "entity_lookup": {
                    "tool": "entity_resolve",
                    "args": {"label": label, "text": "<entity name>"},
                },
                "exact_name_lookup": {
                    "tool": "node_search",
                    "args": {
                        "label": label,
                        "property": lookup[0] if lookup else "name",
                        "value": "<property value>",
                    },
                },
            },
            "planning_hints": ["Use a selective lookup before expanding related nodes."],
        }

    def _schema_describe_relationship(self, args: dict[str, Any]) -> dict[str, Any]:
        rel = args["relationship_type"]
        if rel not in self._schema["relationship_types"]:
            raise ValueError(f"Unknown relationship_type {rel!r}")
        patterns = [
            self._describe_pattern(edge)
            for edge in self._schema["label_relationships"]
            if edge["relationship_type"] == rel
        ]
        ready = {}
        if patterns:
            for suffix, field in (("source", "forward_expand"), ("target", "reverse_expand")):
                expand = {k: v for k, v in patterns[0][field].items() if k != "source_label"}
                ready[f"from_{suffix}_label"] = {**expand, "as": expand["target_label"].lower()}
        return {
            "relationship_type": rel,
            "patterns": patterns,
            "properties": [
                {
                    **{
                        key: value
                        for key, value in item.items()
                        if key not in {"name", "mandatory"}
                    },
                    "relationship_type": rel,
                    "property": item["name"],
                }
                for item in self._relationship_properties(rel)
            ],
            "temporal": self._temporal(rel),
            "canonical_pattern": patterns[0]["pattern"] if patterns else None,
            "ready_to_use": ready,
            "next_actions": [
                {"when": key.removeprefix("from_"), "copy_args": val} for key, val in ready.items()
            ],
            "planning_hints": ["Choose the direction from the current source label."],
        }

    def _schema_search(self, args: dict[str, Any]) -> dict[str, Any]:
        query = args["query"]
        words = set(_tokens(query))
        limit = _int_arg(args, "limit", 10, min_value=1)

        def score(*names: str) -> int:
            return 2 * sum(word in _tokens(name) for word in words for name in names)

        def ranked(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return sorted(
                (item for item in items if item["score"]), key=lambda item: -item["score"]
            )[:limit]

        labels = []
        node_props = []
        rel_props = []
        for label, count in self._label_counts(self._schema["labels"]).items():
            entry = self._label_entry(label, count)
            labels.append({**entry, "score": score(label)})
            for item in self._label_properties(label):
                node_props.append(
                    {
                        "label": label,
                        "property": item["name"],
                        "lookup_candidate": item["name"] in entry["lookup_properties"],
                        "score": score(label, item["name"]),
                        **{key: item[key] for key in ("inferred_type", "operators", "examples")},
                    }
                )
        for rel in self._schema["relationship_types"]:
            for item in self._relationship_properties(rel):
                rel_props.append(
                    {
                        "relationship_type": rel,
                        "property": item["name"],
                        "score": score(rel, item["name"]),
                        **{key: item[key] for key in ("inferred_type", "operators", "examples")},
                    }
                )
        relationships = [
            {**edge, **self._navigation(edge), "score": score(*edge.values())}
            for edge in self._schema["label_relationships"]
        ]
        return {
            "query": query,
            "labels": ranked(labels),
            "relationships": ranked(relationships),
            "node_properties": ranked(node_props),
            "relationship_properties": ranked(rel_props),
            "next_actions": [
                "Inspect matching labels or relationships before running a graph query."
            ],
            "planning_hints": ["Confirm property types before choosing filters."],
        }

    def _inspect_paths(self, args: dict[str, Any]) -> dict[str, Any]:
        source, target = args["source_label"], args.get("target_label")
        rel = args.get("relationship_type")
        limit = _int_arg(args, "limit", 20, min_value=1)
        depth = min(_int_arg(args, "max_hops", 2, min_value=1), 4)
        queue: deque[tuple[str, list[dict[str, str]], frozenset[str], str]] = deque(
            [(source, [], frozenset({source}), f"(:{source})")]
        )
        paths: list[dict[str, Any]] = []
        while queue and len(paths) < limit:
            label, steps, visited, shape = queue.popleft()
            if steps and (target is None or target == label):
                paths.append(
                    {
                        "source_label": source,
                        "end_label": label,
                        "hop_count": len(steps),
                        "hops": steps,
                        "cypher_shape": shape,
                        "use_with": {"pattern_query": {"start_label": source, "hops": steps}},
                    }
                )
                if target is not None:
                    continue
            if len(steps) == depth:
                continue
            for traversal in self._traversals(label):
                relation = traversal["relationship_type"]
                next_label = traversal["target_label"]
                if (rel and relation != rel) or next_label in visited:
                    continue
                link = (
                    f"-[:{relation}]->" if traversal["direction"] == "out" else f"<-[:{relation}]-"
                )
                step = {**traversal, "as": next_label.lower()}
                queue.append(
                    (
                        next_label,
                        [*steps, step],
                        visited | {next_label},
                        f"{shape}{link}(:{next_label})",
                    )
                )
        return {
            "source_label": source,
            "target_label": target,
            "relationship_type": rel,
            "max_hops": depth,
            "paths": paths,
            "planning_hints": ["Use the returned hops as the pattern_query traversal."],
        }

    def _draft_tool_plan(self, args: dict[str, Any]) -> dict[str, Any]:
        question = args["question"]
        words = set(_tokens(question))
        features = {
            "count": bool(words & {"count", "many", "number"}),
            "grouped_count": bool(words & {"each", "per"}),
            "or": "or" in words,
            "and": "and" in words,
            "comparison": bool(words & {"compare", "than"}),
            "superlative": bool(words & {"most", "least", "top", "highest", "lowest"}),
            "multi_hop": bool(words & {"through", "via"}),
        }
        discovered = self._schema_search({"query": question, "limit": 5})
        tools = ["schema_overview"]
        if features["or"] or features["and"]:
            tools.append("entity_set_operation")
        tools.append("count_handle" if features["count"] else "project")
        tools.append("fetch")
        maximum = _int_arg(args, "max_steps", len(tools), min_value=1)
        outline = [
            {
                "phase": "schema" if tool.startswith("schema") else "answer",
                "tool": tool,
                "why": "Use the results of the preceding step.",
            }
            for tool in tools[:maximum]
        ]
        return {
            "question": question,
            "features": features,
            "plan_outline": outline,
            "candidate_schema": {
                "labels": discovered["labels"],
                "properties": discovered["node_properties"],
                "relationship_types": list(
                    dict.fromkeys(row["relationship_type"] for row in discovered["relationships"])
                ),
                "named_entity_likely": False,
                "named_entity_examples": [],
            },
            "recommended_tools": tools[1:-1],
            "validation_required": "entity_set_operation" in tools,
            "risks": [],
            "planning_hints": ["Resolve the input handles and validate concrete arguments."],
        }

    def _validate_tool_plan(self, args: dict[str, Any]) -> dict[str, Any]:
        warnings = []
        plan = args["plan"]
        for index, step in enumerate(plan, start=1):
            try:
                if not isinstance(step, dict) or step.get("tool") not in SUPPORTED_ANA_TOOLS:
                    raise ValueError("Unknown tool")
                validate_args(step["tool"], step.get("args", {}))
            except ValueError as exc:
                warnings.append({"step": index, "severity": "error", "message": str(exc)})
        valid = not warnings
        if not plan or not isinstance(plan[-1], dict) or plan[-1].get("tool") != "fetch":
            warnings.append({"step": None, "severity": "warning", "message": "End with a fetch."})
        return {
            "valid": valid,
            "warnings": warnings,
            "plan_length": len(plan),
            "planning_hints": ["Resolve errors before executing the plan."],
        }

    def _repair_empty_result(self, args: dict[str, Any]) -> dict[str, Any]:
        failed = args["args"]
        label = failed.get("label") or failed.get("start_label")
        suggestion: dict[str, Any] = {
            "action": "schema_overview",
            "args": {},
            "why": "Verify labels, properties and relationship direction.",
        }
        if label and label not in self._schema["labels"]:
            suggestion.update(action="schema_search", args={"query": label})
        return {
            "failed_tool": args["tool"],
            "reason": args.get("reason"),
            "error": args.get("error"),
            "suggestions": [suggestion],
            "general_hints": ["Check the first binding and each hop before broadening a filter."],
        }


def _tokens(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", text.casefold())
