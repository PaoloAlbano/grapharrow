"""Adapter for the vendored openCypher grammar and GraphArrow read extensions."""

from __future__ import annotations

import re
from typing import cast

from antlr4 import CommonTokenStream, InputStream
from antlr4.error.ErrorListener import ErrorListener  # type: ignore[import-untyped]

from grapharrow.core.errors import QueryError
from grapharrow.interfaces.opencypher.generated.vendor.opencypher.GraphArrowCypherLexer import (
    GraphArrowCypherLexer as CypherLexer,
)
from grapharrow.interfaces.opencypher.generated.vendor.opencypher.GraphArrowCypherParser import (
    GraphArrowCypherParser as CypherParser,
)


class OpenCypherAdapter:
    """Parse queries with the derived GraphArrow Cypher grammar."""

    def parse(self, query: str) -> CypherParser.OC_CypherContext:
        """Parse ``query`` and return the ANTLR parse tree root."""

        normalized_query = _normalize_legacy_exists_syntax(query)

        lexer = CypherLexer(InputStream(normalized_query))
        lexer.removeErrorListeners()
        lexer.addErrorListener(_RaisingErrorListener())

        parser = CypherParser(CommonTokenStream(lexer))
        parser.removeErrorListeners()
        parser.addErrorListener(_RaisingErrorListener())

        try:
            return cast(CypherParser.OC_CypherContext, parser.oC_Cypher())  # type: ignore[no-untyped-call]
        except RecursionError as error:
            raise QueryError(
                "Query nesting exceeds the supported parser limit.",
                category="UnsupportedFeature",
                phase="compile",
                detail="NestingLimit",
            ) from error


def parse_cypher(query: str) -> CypherParser.OC_CypherContext:
    """Convenience wrapper to parse ``query`` with the default adapter."""

    return OpenCypherAdapter().parse(query)


def _normalize_legacy_exists_syntax(query: str) -> str:
    # The vendored grammar does not accept the legacy EXISTS(n.prop) form, so
    # we rewrite the narrow property-existence form into equivalent openCypher
    # null checks before handing the query to ANTLR.
    if "exists" not in query.lower():
        return query
    # Literal strings, escaped identifiers and comments are not executable text.
    segments = re.split(
        r"('(?:\\.|[^'\\])*'|\"(?:\\.|[^\"\\])*\"|`(?:``|[^`])*`|//[^\r\n]*|/\*[\s\S]*?\*/)",
        query,
    )
    for index in range(0, len(segments), 2):
        segment = re.sub(
            r"(?i)NOT\s+EXISTS\s*\(\s*([A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*)\s*\)",
            r"\1 IS NULL",
            segments[index],
        )
        segments[index] = re.sub(
            r"(?i)EXISTS\s*\(\s*([A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*)\s*\)",
            r"\1 IS NOT NULL",
            segment,
        )
    return "".join(segments)


class _RaisingErrorListener(ErrorListener):  # type: ignore[misc]
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
        raise QueryError(
            f"openCypher syntax error at {line}:{column}: {msg}",
            category="SyntaxError",
            phase="compile",
            detail="UnexpectedSyntax",
            line=line,
            column=column,
        )
