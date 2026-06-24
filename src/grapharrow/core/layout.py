"""Filesystem layout helpers for a GraphArrow database."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TypeAlias

Location: TypeAlias = str | Path


@dataclass(frozen=True, slots=True)
class DatabaseLayout:
    """Resolved on-disk paths for a single GraphArrow database."""

    root: Location

    def _join(self, *parts: str) -> Location:
        if isinstance(self.root, Path):
            path = self.root
            for part in parts:
                path = path / part
            return path

        base = self.root.rstrip("/")
        suffix = "/".join(part.strip("/") for part in parts)
        return f"{base}/{suffix}" if suffix else base

    def relative_path(self, location: Location) -> str:
        if isinstance(self.root, Path):
            resolved = location if isinstance(location, Path) else Path(location)
            return str(resolved.relative_to(self.root))

        root_text = self.root.rstrip("/")
        location_text = str(location)
        prefix = f"{root_text}/"
        if location_text.startswith(prefix):
            return location_text.removeprefix(prefix)
        if location_text == root_text:
            return ""
        return location_text

    @property
    def catalog_dir(self) -> Location:
        return self._join("catalog")

    @property
    def manifests_dir(self) -> Location:
        return self._join("manifests")

    @property
    def staged_dir(self) -> Location:
        return self._join("staged")

    @property
    def data_dir(self) -> Location:
        return self._join("data")

    @property
    def nodes_dir(self) -> Location:
        return self._join("data", "nodes")

    @property
    def edges_dir(self) -> Location:
        return self._join("data", "edges")

    @property
    def derived_dir(self) -> Location:
        return self._join("derived")

    @property
    def node_labels_dir(self) -> Location:
        return self._join("derived", "node_labels")

    @property
    def node_properties_dir(self) -> Location:
        return self._join("derived", "node_properties")

    @property
    def adjacency_dir(self) -> Location:
        return self._join("derived", "adjacency")

    @property
    def adjacency_by_source_dir(self) -> Location:
        return self._join("derived", "adjacency", "by_source")

    @property
    def adjacency_by_dest_dir(self) -> Location:
        return self._join("derived", "adjacency", "by_dest")

    @property
    def edge_properties_dir(self) -> Location:
        return self._join("derived", "edge_properties")

    @property
    def current_snapshot_path(self) -> Location:
        return self._join("current_snapshot.json")

    @property
    def current_catalog_path(self) -> Location:
        return self._join("catalog", "current_catalog.json")

    def ensure_base_directories(self) -> None:
        """Create the base directory layout for a database."""

        if not isinstance(self.root, Path):
            return

        root = self.root
        for path in (
            root,
            self.catalog_dir,
            self.manifests_dir,
            self.staged_dir,
            self.data_dir,
            self.nodes_dir,
            self.edges_dir,
            self.derived_dir,
            self.node_labels_dir,
            self.node_properties_dir,
            self.adjacency_dir,
            self.adjacency_by_source_dir,
            self.adjacency_by_dest_dir,
            self.edge_properties_dir,
        ):
            assert isinstance(path, Path)
            path.mkdir(parents=True, exist_ok=True)
