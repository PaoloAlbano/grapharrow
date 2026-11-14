"""Public package exports for GraphArrow."""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _distribution_version

from grapharrow.app import build_application
from grapharrow.config import GraphArrowSettings
from grapharrow.database import Database
from grapharrow.session import Session

try:
    __version__ = _distribution_version("grapharrow")
except PackageNotFoundError:
    __version__ = "0+unknown"

__all__ = ["Database", "GraphArrowSettings", "Session", "__version__", "build_application"]
