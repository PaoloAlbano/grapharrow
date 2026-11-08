"""Native entity and scalar-value discovery for the pinned ANA contract."""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from heapq import nsmallest
from typing import Any, cast

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.compute as pc  # type: ignore[import-untyped]

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
    hits, candidates = backend._cached_read(
        ("entity_resolve", label, text, tuple(terms), limit),
        lambda: _entity_candidates(backend, label, text, terms, search_terms, limit),
    )
    rows = [{variable: backend._node(hit[3]), "__rel_ids": []} for hit in hits]
    result = dict(backend._store(rows, focus=variable))
    if terms:
        result["context_terms"] = terms
    selected = set(result.get("preview") or [])
    alternatives = []
    for context_score, score, _, node_id in candidates:
        node = backend._node(node_id)
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


def _entity_candidates(
    backend: Any, label: str, text: str, terms: list[str], search_terms: list[str], limit: int
) -> tuple[tuple[tuple[int, int, str, int], ...], tuple[tuple[int, int, str, int], ...]]:
    hits = []
    candidates = []
    needle = text.lower()
    node_ids = _entity_prefilter(backend, label, text, terms, search_terms, limit)
    for node_id, props in _search_rows(
        backend, label, ("name", "aliases", "description"), node_ids=node_ids
    ):
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
            hits.append(
                (context_score, score, scalar_text(name) if name is not None else "", node_id)
            )
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
                    node_id,
                )
            )
    hits = nsmallest(limit, hits, key=lambda hit: hit[:3])
    if hits:
        best = hits[0][:2]
        hits = [hit for hit in hits if hit[:2] == best]
    return tuple(hits), tuple(nsmallest(max(limit, 8), candidates, key=lambda hit: hit[:3]))


_STRING_LIST_JSON = (
    r'^[ \t\r\n]*\[[ \t\r\n]*(?:"(?:[^"\\\x00-\x1f]|\\.)*"[ \t\r\n]*'
    r'(?:,[ \t\r\n]*"(?:[^"\\\x00-\x1f]|\\.)*"[ \t\r\n]*)*)?\][ \t\r\n]*$'
)


def _entity_prefilter(
    backend: Any, label: str, text: str, terms: list[str], search_terms: list[str], limit: int
) -> list[int] | None:
    """Keep every possible ranked finalist before Python scoring.

    ASCII strings use Arrow kernels. Unicode and nonstandard aliases remain
    candidates for the authoritative Python scorer, including invalid scalars
    whose errors must not disappear merely because their name does not match.
    """
    ids = backend._label_ids.get(label, [])
    needle = text.lower()
    if not ids:
        return []
    if len(ids) < 64:
        return None
    if len(needle) > 64 or len(search_terms) > 16:
        return None
    table = backend._property_tables.get(label)
    if (
        table is None
        or "node_id" not in table.column_names
        or not pa.types.is_int64(table["node_id"].type)
        or table.schema.field("node_id").metadata
    ):
        return None
    known = {
        item["propertyName"]
        for item in backend._schema["node_properties"]
        if label in item["nodeLabels"]
    }
    if any(
        prop in known and prop not in table.column_names
        for prop in ("name", "aliases", "description")
    ):
        return None
    for prop in ("name", "description"):
        if prop in table.column_names:
            field = table.schema.field(prop)
            if (field.metadata or {}).get(b"grapharrow.encoding") != b"native" or not (
                pa.types.is_string(field.type) or pa.types.is_null(field.type)
            ):
                return None
    aliases = None
    if "aliases" in table.column_names:
        field = table.schema.field("aliases")
        if (field.metadata or {}).get(b"grapharrow.encoding") == b"json" and pa.types.is_string(
            field.type
        ):
            aliases = table["aliases"]
        elif not (
            pa.types.is_null(field.type)
            and (field.metadata or {}).get(b"grapharrow.encoding") == b"native"
        ):
            return None
    keys = pa.array(ids, type=pa.int64())
    positions = pc.index_in(keys, value_set=table["node_id"])
    if positions.null_count:
        return None
    # Reverse matches (a whole stored name/alias inside the question) matter for
    # alternatives. Cap the question length above before enumerating substrings.
    parts = sorted(
        {
            needle[start:end]
            for start in range(len(needle) + 1)
            for end in range(start, len(needle) + 1)
        }
    )
    try:
        name = (
            table["name"].cast(pa.string())
            if "name" in table.column_names
            else pa.nulls(table.num_rows, type=pa.string())
        )
        lowered = pc.ascii_lower(name)
        ranked = _rank_entity_rows(
            table, keys, positions, name, lowered, aliases, text, terms, search_terms, parts, limit
        )
        if ranked is not None:
            return ranked
        masks = [
            pc.match_substring(lowered, needle),
            pc.is_in(lowered, value_set=pa.array(parts, type=pa.string())),
            pc.match_substring_regex(name, pattern=r"[^\x00-\x7f]"),
        ]
        if "description" in table.column_names:
            description = table["description"].cast(pa.string())
            masks.append(pc.match_substring_regex(description, pattern=r"[^\x00-\x7f]"))
            description = pc.ascii_lower(pc.fill_null(description, "none"))
        else:
            description = None
        for term in search_terms:
            masks.append(pc.match_substring(lowered, term))
            if description is not None:
                masks.append(pc.match_substring(description, term))
        if aliases is not None:
            raw = pc.ascii_lower(aliases)
            masks.extend(
                [
                    pc.and_kleene(
                        pc.is_valid(raw),
                        pc.invert(pc.match_substring_regex(raw, pattern=_STRING_LIST_JSON)),
                    ),
                    pc.match_substring(raw, "\\u"),
                    pc.match_substring_regex(raw, pattern=r"[^\x00-\x7f]"),
                    pc.match_substring(raw, json.dumps(needle)[1:-1]),
                ]
            )
            for term in search_terms:
                masks.append(pc.match_substring(raw, json.dumps(term)[1:-1]))
            reverse = '"(?:' + "|".join(re.escape(json.dumps(part)[1:-1]) for part in parts) + ')"'
            masks.append(pc.match_substring_regex(raw, pattern=reverse))
        mask = masks[0]
        for part in masks[1:]:
            mask = pc.or_kleene(mask, part)
        return cast(
            list[int], pc.filter(keys, pc.fill_null(pc.take(mask, positions), False)).to_pylist()
        )
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError, UnicodeError):
        return None


def _rank_entity_rows(
    table: Any,
    keys: Any,
    positions: Any,
    name: Any,
    lowered: Any,
    aliases: Any,
    text: str,
    terms: list[str],
    search_terms: list[str],
    parts: list[str],
    limit: int,
) -> list[int] | None:
    """Bound exact ASCII candidates in Arrow, retaining every exceptional row.

    Keeping the top K safe rows in each ranking is sufficient when merging all
    exceptional rows. Canonical order is an explicit last key; the Python scorer
    remains authoritative for the selected superset and all Unicode/unsafe data.
    """
    needle = text.lower()
    if not needle.strip() or any(re.search(r'["\\,\[\]]', item) for item in [needle, *terms]):
        return None

    def truth(value: Any) -> Any:
        return pc.fill_null(value, False)

    false = pc.is_null(table["node_id"])
    unsafe = truth(pc.match_substring_regex(name, pattern=r"[^\x00-\x7f]"))
    description = None
    if "description" in table.column_names:
        column = table["description"].cast(pa.string())
        unsafe = pc.or_(
            unsafe,
            pc.or_(
                pc.is_null(column), truth(pc.match_substring_regex(column, pattern=r"[^\x00-\x7f]"))
            ),
        )
        description = pc.ascii_lower(pc.fill_null(column, ""))
    raw = pc.ascii_lower(aliases) if aliases is not None else None
    if raw is not None:
        unsafe = pc.or_(
            unsafe,
            pc.or_(
                pc.and_(
                    pc.is_valid(raw),
                    pc.invert(truth(pc.match_substring_regex(raw, pattern=_STRING_LIST_JSON))),
                ),
                pc.or_(
                    truth(pc.match_substring(raw, "\\")),
                    truth(pc.match_substring_regex(raw, pattern=r"[^\x00-\x7f]")),
                ),
            ),
        )
    alias_hit = truth(pc.match_substring(raw, needle)) if raw is not None else false
    score = pc.if_else(
        truth(pc.equal(name, text)),
        0,
        pc.if_else(
            truth(pc.equal(lowered, needle)),
            1,
            pc.if_else(truth(pc.match_substring(lowered, needle)), 2, pc.if_else(alias_hit, 3, 99)),
        ),
    )

    def contains(term: str) -> Any:
        result = truth(pc.match_substring(lowered, term))
        if description is not None:
            result = pc.or_(result, truth(pc.match_substring(description, term)))
        if raw is not None:
            result = pc.or_(result, truth(pc.match_substring(raw, term)))
        return result

    context = pc.if_else(false, 0, 0)
    if terms:
        all_terms, any_terms = pc.invert(false), false
        for term in terms:
            matched = contains(term)
            all_terms = pc.and_(all_terms, matched)
            any_terms = pc.or_(any_terms, matched)
        context = pc.if_else(all_terms, 0, pc.if_else(any_terms, 1, 2))
    reverse_name = pc.is_in(lowered, value_set=pa.array(parts, type=pa.string()))
    reverse_alias = false
    if raw is not None:
        pattern = '"(?:' + "|".join(re.escape(part) for part in parts) + ')"'
        reverse_alias = truth(pc.match_substring_regex(raw, pattern=pattern))
    token_hit = false
    for term in search_terms:
        token_hit = pc.or_(token_hit, contains(term))
    candidate = pc.min_element_wise(
        score,
        pc.if_else(reverse_name, 2, 99),
        pc.if_else(reverse_alias, 3, 99),
        pc.if_else(token_hit, 4, 99),
    )
    ranking = pa.table(
        {
            "node_id": keys,
            "score": pc.take(score, positions),
            "candidate": pc.take(candidate, positions),
            "context": pc.take(context, positions),
            "name": pc.take(pc.fill_null(name, ""), positions),
            "order": pa.array(range(len(keys)), type=pa.int64()),
        }
    )
    exceptional = truth(pc.take(unsafe, positions))
    selected = [pc.filter(keys, exceptional)]
    for column, count in (("score", limit), ("candidate", max(limit, 8))):
        eligible = ranking.filter(pc.and_(pc.invert(exceptional), pc.less(ranking[column], 99)))
        if eligible.num_rows:
            order = pc.select_k_unstable(
                eligible,
                k=min(count, eligible.num_rows),
                sort_keys=[
                    ("context", "ascending"),
                    (column, "ascending"),
                    ("name", "ascending"),
                    ("order", "ascending"),
                ],
            )
            selected.append(pc.take(eligible["node_id"], order))
    candidates = pa.chunked_array(selected, type=pa.int64())
    return cast(list[int], pc.filter(keys, pc.is_in(keys, value_set=candidates)).to_pylist())


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
    hits = backend._cached_read(
        ("value_search", label, prop, text, mode, limit),
        lambda: _value_candidates(backend, label, prop, text, mode, limit),
    )
    rows = []
    for score, _, node_id in hits:
        node = backend._node(node_id)
        row = {"value": node["properties"][prop], "match_score": score}
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


def _value_candidates(
    backend: Any, label: str, prop: str, text: str, mode: str, limit: int
) -> tuple[tuple[int, str, int], ...]:
    table = backend._property_tables.get(label)
    if (
        table is not None
        and prop != "node_id"
        and prop in table.column_names
        and "node_id" in table.column_names
        and pa.types.is_int64(table["node_id"].type)
        and not table.schema.field("node_id").metadata
    ):
        field = table.schema.field(prop)
        if (field.metadata or {}).get(b"grapharrow.encoding") == b"native" and pa.types.is_string(
            field.type
        ):
            # Unicode lowercasing retains Python semantics through the canonical path.
            if (
                mode == "exact"
                or text.isascii()
                and not pc.any(
                    pc.match_substring_regex(table[prop], pattern=r"[^\x00-\x7f]")
                ).as_py()
            ):
                return _string_value_candidates(backend, label, table, prop, text, mode, limit)
    hits = []
    for node_id, props in _search_rows(backend, label, (prop,)):
        value = props.get(prop)
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
            hits.append((score, rendered, node_id))
    return tuple(nsmallest(limit, hits, key=lambda hit: hit[:2]))


def scalar_text(value: Any) -> str:
    """Render scalar property values using Cypher's boolean spelling."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, dict)):
        raise ValueError("Value search requires a scalar property")
    return str(value)


def _search_rows(
    backend: Any,
    label: str,
    properties: tuple[str, ...],
    *,
    node_ids: list[int] | None = None,
    require_all_properties: bool = False,
) -> Iterator[tuple[int, dict[str, Any]]]:
    """Read narrow columns, preserving canonical types, missing fields, and input order."""
    ids = backend._label_ids.get(label, []) if node_ids is None else node_ids
    table = backend._property_tables.get(label)
    projected = [prop for prop in properties if table is not None and prop in table.column_names]
    spec = backend._properties({"kind": "node", "label": label}) if require_all_properties else {}
    # Older scalar-subclass projections can mark raw strings as JSON. Seed
    # predicates must retain canonical values even if that text parses as JSON.
    ambiguous_scalar = require_all_properties and any(
        (table.schema.field(prop).metadata or {}).get(b"grapharrow.encoding") == b"json"
        and "String" in spec.get(prop, [])
        and set(spec[prop]) <= {"String", "Any"}
        for prop in projected
    )
    if (
        table is not None
        and not ambiguous_scalar
        and (not require_all_properties or len(projected) == len(properties))
        and "node_id" not in projected
        and "node_id" in table.column_names
        and pa.types.is_int64(table["node_id"].type)
        and not table.schema.field("node_id").metadata
        and all(
            (table.schema.field(prop).metadata or {}).get(b"grapharrow.encoding")
            in {b"native", b"json"}
            for prop in projected
        )
    ):
        positions = pc.index_in(pa.array(ids, type=pa.int64()), value_set=table["node_id"])
        if positions.null_count == 0:
            encoded = {
                prop
                for prop in projected
                if table.schema.field(prop).metadata[b"grapharrow.encoding"] == b"json"
            }
            for row in table.select(["node_id", *projected]).take(positions).to_pylist():
                node_id = row.pop("node_id")
                if any(value is None for value in row.values()):
                    # Projections conflate absent properties and explicit nulls.
                    raw = backend._state.node_table["properties_json"][backend._positions[node_id]]
                    yield node_id, json.loads(raw.as_py())
                else:
                    try:
                        for prop in encoded:
                            row[prop] = json.loads(row[prop])
                    except (ValueError, TypeError):
                        if not require_all_properties:
                            raise
                        # Seed predicates previously read canonical payloads.
                        # Keep that authority if a historical projection cannot
                        # reconstruct a value (including scalar subclasses).
                        raw = backend._state.node_table["properties_json"][
                            backend._positions[node_id]
                        ]
                        yield node_id, json.loads(raw.as_py())
                    else:
                        yield node_id, row
            return
    # Historical and lossy projections use canonical JSON without allocating entity payloads.
    positions = pa.array([backend._positions[node_id] for node_id in ids], type=pa.int64())
    for row in (
        backend._state.node_table.select(["node_id", "properties_json"]).take(positions).to_pylist()
    ):
        yield row["node_id"], json.loads(row["properties_json"])


def _string_value_candidates(
    backend: Any, label: str, table: Any, prop: str, text: str, mode: str, limit: int
) -> tuple[tuple[int, str, int], ...]:
    if mode == "exact":
        indexed = backend._application.use_cases.graph.lookup_node_property_table(
            snapshot=backend._state.snapshot,
            catalog=backend._state.catalog,
            label=label,
            equalities={prop: text},
            table=table,
        )
        if indexed is not None:
            table = indexed
    values = table[prop]
    lowered, needle = pc.utf8_lower(values), text.lower()
    if mode == "exact":
        scores = pc.if_else(pc.equal(values, text), 0, 99)
    elif mode == "iexact":
        scores = pc.if_else(pc.equal(lowered, needle), 1, 99)
    elif mode == "contains":
        scores = pc.if_else(pc.match_substring(lowered, needle), 2, 99)
    elif mode == "prefix":
        scores = pc.if_else(pc.starts_with(lowered, needle), 3, 99)
    elif mode == "suffix":
        scores = pc.if_else(pc.ends_with(lowered, needle), 4, 99)
    else:
        scores = pc.if_else(
            pc.equal(values, text),
            0,
            pc.if_else(
                pc.equal(lowered, needle), 1, pc.if_else(pc.match_substring(lowered, needle), 2, 99)
            ),
        )
    candidates = pa.table({"score": scores, "value": values, "node_id": table["node_id"]}).filter(
        pc.less(scores, 99)
    )
    # Canonical order breaks otherwise equal keys, including historical reordered projections.
    order = {
        node_id: position for position, node_id in enumerate(backend._label_ids.get(label, []))
    }
    hits = [
        (row["score"], row["value"], row["node_id"])
        for row in candidates.to_pylist()
        if row["node_id"] in order
    ]
    return tuple(nsmallest(limit, hits, key=lambda hit: (hit[:2], order[hit[2]])))
