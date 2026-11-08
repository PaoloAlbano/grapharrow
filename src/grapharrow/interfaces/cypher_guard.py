# mypy: disable-error-code=no-untyped-call
"""Helpers shared by interfaces that need Cypher read/write classification."""

from __future__ import annotations

import re

from grapharrow.adapters import parse_cypher


def query_requires_write(query: str) -> bool:
    """Report whether a Cypher query contains an updating clause."""

    if _is_ddl_compatibility_statement(query):
        return True

    if not re.search(r"\b(CREATE|MERGE|SET|DELETE|REMOVE)\b", query, flags=re.IGNORECASE):
        return False

    tree = parse_cypher(query)
    pending = [tree]
    while pending:
        node = pending.pop()
        if node.parser.ruleNames[node.getRuleIndex()] == "oC_UpdatingClause":
            return True
        pending.extend(child for child in node.children or [] if hasattr(child, "getRuleIndex"))
    return False


def _is_ddl_compatibility_statement(query: str) -> bool:
    """Recognize DDL statements handled outside the openCypher parser."""
    normalized = " ".join(query.strip().rstrip(";").split())
    return bool(
        re.fullmatch(r"create\s+(text\s+)?index\s+.+", normalized, flags=re.IGNORECASE)
        or re.fullmatch(r"drop\s+index\s+.+", normalized, flags=re.IGNORECASE)
    )
