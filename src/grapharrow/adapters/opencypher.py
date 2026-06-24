"""Adapter around the vendored official openCypher grammar."""

from __future__ import annotations

import re
from typing import cast

from antlr4 import CommonTokenStream, InputStream

from grapharrow.core.errors import QueryError
from grapharrow.interfaces.opencypher.generated.vendor.opencypher.CypherLexer import CypherLexer
from grapharrow.interfaces.opencypher.generated.vendor.opencypher.CypherParser import CypherParser


class OpenCypherAdapter:
    """Parse queries with the official openCypher grammar."""

    def parse(self, query: str) -> CypherParser.OC_CypherContext:
        """Parse ``query`` and return the ANTLR parse tree root."""

        normalized_query = _normalize_legacy_exists_syntax(query)

        lexer = CypherLexer(InputStream(normalized_query))
        lexer.removeErrorListeners()
        lexer.addErrorListener(_RaisingErrorListener())

        parser = CypherParser(CommonTokenStream(lexer))
        parser.removeErrorListeners()
        parser.addErrorListener(_RaisingErrorListener())

        return cast(CypherParser.OC_CypherContext, parser.oC_Cypher())  # type: ignore[no-untyped-call]


def parse_cypher(query: str) -> CypherParser.OC_CypherContext:
    """Convenience wrapper to parse ``query`` with the default adapter."""

    return OpenCypherAdapter().parse(query)


def _normalize_legacy_exists_syntax(query: str) -> str:
    # The vendored grammar does not accept the legacy EXISTS(n.prop) form, so
    # we rewrite the narrow property-existence form into equivalent openCypher
    # null checks before handing the query to ANTLR.
    query = re.sub(
        r"(?i)NOT\s+EXISTS\s*\(\s*([A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*)\s*\)",
        r"\1 IS NULL",
        query,
    )
    return re.sub(
        r"(?i)EXISTS\s*\(\s*([A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*)\s*\)",
        r"\1 IS NOT NULL",
        query,
    )


class _RaisingErrorListener:
    """Convert ANTLR syntax callbacks into GraphArrow query errors."""

    def syntaxError(
        self,
        recognizer: object,
        offendingSymbol: object,
        line: int,
        column: int,
        msg: str,
        e: Exception | None,
    ) -> None:
        _ = (recognizer, offendingSymbol, e)
        raise QueryError(f"openCypher syntax error at {line}:{column}: {msg}")
