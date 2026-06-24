# mypy: disable-error-code=no-untyped-call
"""Helpers shared by interfaces that need Cypher read/write classification."""

from __future__ import annotations

import re

from grapharrow.adapters import parse_cypher


def query_requires_write(query: str) -> bool:
    """Report whether a Cypher query contains an updating clause."""

    if not re.search(r"\b(CREATE|MERGE|SET|DELETE)\b", query, flags=re.IGNORECASE):
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
    if single_query is None or single_query.oC_MultiPartQuery() is not None:
        return False
    single_part = single_query.oC_SinglePartQuery()
    if single_part is None:
        return False
    return any(
        clause.oC_Create() is not None
        or clause.oC_Merge() is not None
        or clause.oC_Set() is not None
        or clause.oC_Delete() is not None
        for clause in single_part.oC_UpdatingClause()
    )
