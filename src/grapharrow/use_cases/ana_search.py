"""Native entity and scalar-value discovery for the pinned ANA contract."""

from __future__ import annotations

from typing import Any

from grapharrow.ana.values import (
    _bool_arg,
    _distinctive_entity_search_tokens,
    _int_arg,
    _token_variants,
)


def entity_resolve(backend: Any, args: dict[str, Any]) -> dict[str, Any]:
    label = str(args["label"])
    text = str(args["text"])
    variable = str(args.get("as") or label.lower())
    limit = _int_arg(args, "limit", 5, min_value=1)
    if limit > 10 and not _bool_arg(args.get("allow_large")):
        limit = 10
    context = args.get("context_terms") or []
    if isinstance(context, str):
        context = [context]
    terms = [str(term).strip().lower() for term in context if str(term).strip()]
    search_terms = sorted(_token_variants(_distinctive_entity_search_tokens(text)))
    hits = []
    candidates = []
    needle = text.lower()
    for node_id in backend._nodes(label)["node_id"].to_pylist():
        node = backend._node(node_id)
        props = node["properties"]
        name = props.get("name")
        lowered_name = scalar_text(name).lower() if name is not None else None
        aliases = [scalar_text(value).lower() for value in props.get("aliases") or []]
        score = 99
        if name == text:
            score = 0
        elif lowered_name == needle:
            score = 1
        elif lowered_name is not None and needle in lowered_name:
            score = 2
        elif any(needle in alias for alias in aliases):
            score = 3
        haystack = [lowered_name or "", *aliases, scalar_text(props.get("description", "")).lower()]
        matched = [any(term in item for item in haystack) for term in terms]
        context_score = 0 if not terms or all(matched) else 1 if any(matched) else 2
        if score < 99:
            hits.append((context_score, score, scalar_text(name) if name is not None else "", node))
        candidate_score = score
        if lowered_name is not None and lowered_name in needle:
            candidate_score = min(candidate_score, 2)
        if any(alias in needle for alias in aliases):
            candidate_score = min(candidate_score, 3)
        if any(term in item for term in search_terms for item in haystack):
            candidate_score = min(candidate_score, 4)
        if candidate_score < 99:
            candidates.append(
                (
                    context_score,
                    candidate_score,
                    scalar_text(name) if name is not None else "",
                    node,
                )
            )
    hits.sort(key=lambda hit: hit[:3])
    if hits:
        best = hits[0][:2]
        hits = [hit for hit in hits if hit[:2] == best][:limit]
    rows = [{variable: hit[3], "__rel_ids": []} for hit in hits]
    result = dict(backend._store(rows, focus=variable))
    if terms:
        result["context_terms"] = terms
    selected = set(result.get("preview") or [])
    alternatives = []
    for context_score, score, _, node in sorted(candidates, key=lambda hit: hit[:3])[
        : max(limit, 8)
    ]:
        props = node["properties"]
        name = props.get("name") or props.get("id")
        if name not in selected:
            alternatives.append(
                {
                    "name": name,
                    "labels": node["labels"],
                    "score": score,
                    "context_score": context_score,
                    "properties": {
                        key: props[key]
                        for key in ("name", "aliases", "description")
                        if key in props
                    },
                }
            )
    if alternatives:
        result["alternative_candidates"] = alternatives[:5]
        result["candidate_hint"] = (
            "If a traversal from the selected candidate returns zero rows, "
            "retry entity_resolve with context_terms from the original question "
            "or resolve a more exact candidate from alternative_candidates."
        )
    if len(rows) > 1:
        result["ambiguity_hint"] = (
            "Multiple candidates matched. If the question includes a country, "
            "parenthetical, or other qualifier, pass it in context_terms."
        )
    return result


def value_search(backend: Any, args: dict[str, Any]) -> dict[str, Any]:
    label, prop, text = str(args["label"]), str(args["property"]), str(args["text"])
    variable = str(args.get("as") or label.lower())
    limit = _int_arg(args, "limit", 20, min_value=1, max_value=100)
    mode = str(args.get("match_mode") or "auto").lower()
    keep = _bool_arg(args.get("keep_entities", True))
    if mode not in {"auto", "exact", "iexact", "contains", "prefix", "suffix"}:
        raise ValueError(
            "value_search match_mode must be auto, exact, iexact, contains, prefix or suffix"
        )
    hits = []
    for node_id in backend._nodes(label)["node_id"].to_pylist():
        node = backend._node(node_id)
        value = node["properties"].get(prop)
        if value is None:
            continue
        rendered = scalar_text(value)
        lowered, needle = rendered.lower(), text.lower()
        score = 99
        if mode in {"auto", "exact"} and rendered == text:
            score = 0
        elif mode in {"auto", "iexact"} and lowered == needle:
            score = 1
        elif mode in {"auto", "contains"} and needle in lowered:
            score = 2
        elif mode == "prefix" and lowered.startswith(needle):
            score = 3
        elif mode == "suffix" and lowered.endswith(needle):
            score = 4
        if score < 99:
            hits.append((score, rendered, node, value))
    hits.sort(key=lambda hit: hit[:2])
    rows = []
    for score, _, node, value in hits[:limit]:
        row = {"value": value, "match_score": score}
        if keep:
            row.update({variable: node, "__rel_ids": []})
        rows.append(row)
    result = dict(
        backend._store(
            rows,
            focus=variable if keep else None,
            kind="rows" if keep else "table",
            columns=["value", "match_score"],
        )
    )
    result["matched_property"] = {"label": label, "property": prop, "text": text}
    result["planning_hints"] = [
        "Use the returned value exactly in a later filter when you need a property value.",
        "If keep_entities=true, the returned handle can also be used directly by expand.",
        "Use entity_resolve instead when resolving a specific named company/person/place node.",
    ]
    return result


def scalar_text(value: Any) -> str:
    """Render scalar property values using Cypher's boolean spelling."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, dict)):
        raise ValueError("Value search requires a scalar property")
    return str(value)
