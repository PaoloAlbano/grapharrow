"""Project-specific exception hierarchy."""


class GraphArrowError(Exception):
    """Base exception for GraphArrow."""


class CatalogError(GraphArrowError):
    """Raised when catalog metadata is invalid or unavailable."""


class NotFoundError(GraphArrowError):
    """Raised when a requested entity or resource does not exist."""


class QueryError(GraphArrowError):
    """Raised when query parsing or execution fails."""


class SchemaError(GraphArrowError):
    """Raised when schema validation fails."""


class StorageError(GraphArrowError):
    """Raised when storage operations fail."""


class TransactionError(GraphArrowError):
    """Raised when transactional operations fail."""
