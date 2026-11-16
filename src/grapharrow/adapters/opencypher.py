"""Adapter for the vendored openCypher grammar and GraphArrow read extensions."""

from __future__ import annotations

import re
from typing import cast

from antlr4 import CommonTokenStream, InputStream
from antlr4.error.ErrorListener import ErrorListener  # type: ignore[import-untyped]
from antlr4.error.Errors import LexerNoViableAltException  # type: ignore[import-untyped]
from antlr4.Token import Token  # type: ignore[import-untyped]

from grapharrow.core.errors import QueryError
from grapharrow.interfaces.opencypher.generated.vendor.opencypher.GraphArrowCypherLexer import (
    GraphArrowCypherLexer as CypherLexer,
)
from grapharrow.interfaces.opencypher.generated.vendor.opencypher.GraphArrowCypherParser import (
    GraphArrowCypherParser as CypherParser,
)

_RELATIONSHIP_DASH = r"[-\u00ad\u2010-\u2015\u2212\ufe58\ufe63\uff0d]"
_RELATIONSHIP_TAIL = re.compile(rf"[^\[\]]*\]\s*{_RELATIONSHIP_DASH}\s*[>⟩〉﹥＞]?\s*\(")
_RELATIONSHIP_DETAIL_PREFIX = r"n?(?::n(?:\|:?n)*)?"
_STRING_ESCAPE = re.compile(r"\\([uU][0-9a-fA-F]{4}|.)", re.DOTALL)


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


def _malformed_number(recognizer: object, token: object) -> bool:
    if (
        not isinstance(recognizer, CypherParser)
        or not isinstance(token, Token)
        or token.type != CypherParser.UnescapedSymbolicName
        or token.tokenIndex < 1
    ):
        return False
    previous = recognizer.getInputStream().get(token.tokenIndex - 1)
    return previous.stop + 1 == token.start and previous.type in {
        CypherParser.DecimalInteger,
        CypherParser.HexInteger,
        CypherParser.OctalInteger,
        CypherParser.RegularDecimalReal,
        CypherParser.ExponentDecimalReal,
    }


def _malformed_relationship_range(recognizer: CypherParser, token: Token) -> bool:
    """Recognize a range error before ANTLR has selected a relationship rule."""
    if token.text not in {"..", "-"}:
        return False
    if not _RELATIONSHIP_TAIL.match(token.getInputStream().strdata, token.start):
        return False

    stream = recognizer.getInputStream()
    preceding = (
        previous
        for index in range(token.tokenIndex - 1, -1, -1)
        if (previous := stream.get(index)).type != CypherParser.SP
    )
    prefix = []
    for previous in preceding:
        if previous.text == "[":
            break
        if previous.text in {":", "|", "*", ".."}:
            prefix.append(previous.text)
        elif previous.type in {
            CypherParser.DecimalInteger,
            CypherParser.HexInteger,
            CypherParser.OctalInteger,
        }:
            prefix.append("0")
        elif previous.type == CypherParser.EscapedSymbolicName or previous.text.isidentifier():
            prefix.append("n")
        else:
            return False
    dash = next(preceding, None)
    if dash is None or not re.fullmatch(_RELATIONSHIP_DASH, dash.text):
        return False
    left = next(preceding, None)
    if left is not None and left.text in {"<", "⟨", "〈", "﹤", "＜"}:
        left = next(preceding, None)
    if left is None or left.text != ")":
        return False

    expected = _RELATIONSHIP_DETAIL_PREFIX
    if token.text == "-":
        expected += r"\*(?:0?\.\.)?"
    return re.fullmatch(expected, "".join(reversed(prefix))) is not None


def _syntax_detail(recognizer: object, token: object, error: Exception | None) -> str:
    if isinstance(error, LexerNoViableAltException):
        stream = error.input
        fragment = stream.getText(error.startIndex, stream.index)
        if fragment.startswith(("'", '"')) and any(
            escape.group(1) in {"u", "U"} for escape in _STRING_ESCAPE.finditer(fragment)
        ):
            return "InvalidUnicodeLiteral"
        if len(fragment) == 1 and not fragment.isascii():
            return "InvalidUnicodeCharacter"
    if _malformed_number(recognizer, token):
        return "InvalidNumberLiteral"
    if isinstance(recognizer, CypherParser) and isinstance(token, Token):
        # These dash variants are grammar tokens for relationships only.
        if not token.text.isascii() and re.fullmatch(_RELATIONSHIP_DASH, token.text):
            return "InvalidUnicodeCharacter"
        if _malformed_relationship_range(recognizer, token):
            return "InvalidRelationshipPattern"
        context = recognizer._ctx
        if isinstance(context, CypherParser.OC_CypherContext) and token.type in {
            CypherParser.RETURN,
            CypherParser.WITH,
        }:
            statement = context.oC_Statement()  # type: ignore[no-untyped-call]
            query = statement.oC_Query() if statement is not None else None
            call = query.oC_StandaloneCall() if query is not None else None
            if call is not None and call.oC_ImplicitProcedureInvocation() is not None:
                return "InvalidArgumentPassingMode"
    return "UnexpectedSyntax"


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
        raise QueryError(
            f"openCypher syntax error at {line}:{column}: {msg}",
            category="SyntaxError",
            phase="compile",
            detail=_syntax_detail(recognizer, offendingSymbol, e),
            line=line,
            column=column,
        )
