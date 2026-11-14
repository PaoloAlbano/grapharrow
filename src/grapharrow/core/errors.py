"""Project-specific exception hierarchy."""

from __future__ import annotations

from typing import Literal

QueryPhase = Literal["compile", "runtime"]


class GraphArrowError(Exception):
    """Base exception for GraphArrow."""


class CatalogError(GraphArrowError):
    """Raised when catalog metadata is invalid or unavailable."""


class NotFoundError(GraphArrowError):
    """Raised when a requested entity or resource does not exist."""


class QueryError(GraphArrowError):
    """Raised when query parsing or execution fails."""

    def __init__(
        self,
        message: str,
        *,
        category: str = "QueryError",
        phase: QueryPhase | None = None,
        detail: str = "Unclassified",
        line: int | None = None,
        column: int | None = None,
    ) -> None:
        super().__init__(message)
        self.category = category
        self.phase = phase
        self.detail = detail
        self.line = line
        self.column = column

    @property
    def code(self) -> str:
        return {
            "SyntaxError": "Neo.ClientError.Statement.SyntaxError",
            "SemanticError": "Neo.ClientError.Statement.SemanticError",
            "TypeError": "Neo.ClientError.Statement.TypeError",
            "ArithmeticError": "Neo.ClientError.Statement.ArithmeticError",
            "ArgumentError": "Neo.ClientError.Statement.ArgumentError",
            "ParameterMissing": "Neo.ClientError.Statement.ParameterMissing",
            "EntityNotFound": "Neo.ClientError.Statement.EntityNotFound",
            "ConstraintVerificationFailed": "Neo.ClientError.Schema.ConstraintValidationFailed",
            "UnsupportedFeature": "GraphArrow.UnsupportedFeature",
        }.get(self.category, "GraphArrow.QueryError")

    def metadata(self) -> dict[str, str | int | None]:
        fields: dict[str, str | int | None] = {
            "code": self.code,
            "category": self.category,
            "phase": self.phase,
            "detail": self.detail,
        }
        if self.line is not None and self.column is not None:
            fields.update(line=self.line, column=self.column)
        return fields

    @classmethod
    def syntax(cls, message: str, detail: str) -> QueryError:
        return cls(message, category="SyntaxError", phase="compile", detail=detail)

    @classmethod
    def invalid_type(cls, message: str, *, phase: QueryPhase) -> QueryError:
        category = "SyntaxError" if phase == "compile" else "TypeError"
        return cls(message, category=category, phase=phase, detail="InvalidArgumentType")

    @classmethod
    def missing_parameter(cls, name: str) -> QueryError:
        return cls(
            f"Missing parameter: ${name}",
            category="ParameterMissing",
            phase="compile",
            detail="MissingParameter",
        )

    @classmethod
    def unsupported(cls, message: str) -> QueryError:
        return cls(message, category="UnsupportedFeature", detail="UnsupportedFeature")


class SchemaError(GraphArrowError):
    """Raised when schema validation fails."""


class StorageError(GraphArrowError):
    """Raised when storage operations fail."""


class TransactionError(GraphArrowError):
    """Raised when transactional operations fail."""


class WriteConflictError(TransactionError):
    """The database changed after a writer captured its base state."""
