# mypy: disable-error-code=no-untyped-call
"""Helpers shared by interfaces that need Cypher read/write classification."""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

from grapharrow.adapters import parse_cypher


def query_requires_write(query: str) -> bool:
    """Report whether a Cypher query contains an updating clause."""

    if _is_ddl_compatibility_statement(query):
        return True

    if not re.search(r"\b(CREATE|MERGE|SET|DELETE|REMOVE)\b", query, flags=re.IGNORECASE):
        return False

    tree = parse_cypher(query)
    statement = tree.oC_Statement()
    if statement is None:
        return False
    query_ctx = statement.oC_Query()
    if query_ctx is None:
        return False
    regular_query = query_ctx.oC_RegularQuery()
    if regular_query is None or regular_query.oC_Union():
        return False
    single_query = regular_query.oC_SingleQuery()
    if single_query is None:
        return False
    multi_part = single_query.oC_MultiPartQuery()
    if multi_part is not None:
        if _updating_clauses_require_write(multi_part.oC_UpdatingClause()):
            return True
        single_part = multi_part.oC_SinglePartQuery()
        return (
            single_part is not None
            and _updating_clauses_require_write(single_part.oC_UpdatingClause())
        )
    single_part = single_query.oC_SinglePartQuery()
    if single_part is None:
        return False
    return _updating_clauses_require_write(single_part.oC_UpdatingClause())


def _is_ddl_compatibility_statement(query: str) -> bool:
    """Recognize DDL statements handled outside the openCypher parser."""
    normalized = " ".join(query.strip().rstrip(";").split())
    return bool(
        re.fullmatch(r"create\s+index\s+on\s+.+", normalized, flags=re.IGNORECASE)
        or re.fullmatch(r"drop\s+index\s+.+", normalized, flags=re.IGNORECASE)
    )


def _updating_clauses_require_write(updating_clauses: Iterable[Any]) -> bool:
    """Return true when any parsed updating clause mutates graph state."""
    return any(
        clause.oC_Create() is not None
        or clause.oC_Merge() is not None
        or clause.oC_Set() is not None
        or clause.oC_Delete() is not None
        or clause.oC_Remove() is not None
        for clause in updating_clauses
    )
