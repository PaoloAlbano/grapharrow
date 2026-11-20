"""Administrative operations routed to the selected Storage Backend."""

from __future__ import annotations

from pathlib import Path
from typing import cast

from grapharrow.adapters import FileSystemAdapter
from grapharrow.core import DatabaseLayout
from grapharrow.domain import SnapshotManifest
from grapharrow.repositories import CatalogRepository, GraphRepository, SnapshotRepository
from grapharrow.storage.contracts import CatalogStore, GraphStore, SnapshotStore, StorageBackend
from grapharrow.storage.models import (
    DeltaPinRecoveryResult,
    DeltaRetentionResult,
    DeltaStorageInventory,
    RetentionPlan,
)
from grapharrow.storage.models import InitializationResult as InitializationResult
from grapharrow.storage.models import RecoveryReport as RecoveryReport
from grapharrow.storage.models import VerificationReport as VerificationReport


class DatabaseAdminUseCase:
    """Expose backend maintenance without depending on its physical layout."""

    def __init__(
        self,
        *,
        storage_backend: StorageBackend | None = None,
        layout: DatabaseLayout | None = None,
        catalog_store: CatalogStore | None = None,
        snapshot_store: SnapshotStore | None = None,
        graph_store: GraphStore | None = None,
        filesystem: FileSystemAdapter | None = None,
    ) -> None:
        if storage_backend is not None:
            if any(
                value is not None
                for value in (layout, catalog_store, snapshot_store, graph_store, filesystem)
            ):
                raise ValueError("Pass a Storage Backend or individual stores, not both.")
            self._maintenance = storage_backend.maintenance
        else:
            if (
                layout is None
                or catalog_store is None
                or snapshot_store is None
                or graph_store is None
            ):
                raise TypeError("A Storage Backend or all native stores are required.")
            from grapharrow.storage.native_maintenance import NativeStorageMaintenance

            self._maintenance = NativeStorageMaintenance(
                layout=layout,
                catalog_store=cast(CatalogRepository, catalog_store),
                snapshot_store=cast(SnapshotRepository, snapshot_store),
                graph_store=cast(GraphRepository, graph_store),
                filesystem=filesystem,
            )

    def initialize(self) -> InitializationResult:
        return self._maintenance.initialize()

    def verify(self, *, verify_checksums: bool = False) -> VerificationReport:
        return self._maintenance.verify(verify_checksums=verify_checksums)

    def recover(
        self,
        *,
        dry_run: bool = True,
        cleanup_orphan_staged: bool = True,
        verify_checksums: bool = False,
    ) -> RecoveryReport:
        return self._maintenance.recover(
            dry_run=dry_run,
            cleanup_orphan_staged=cleanup_orphan_staged,
            verify_checksums=verify_checksums,
        )

    def compact(self) -> SnapshotManifest:
        return self._maintenance.compact()

    def vacuum(self) -> tuple[str, ...]:
        return self._maintenance.vacuum()

    def storage_inventory(self) -> DeltaStorageInventory:
        """Describe physical storage against the published snapshot."""
        return self._maintenance.storage_inventory()

    def apply_retention(
        self, *, minimum_snapshot_id: int, online: bool = False
    ) -> DeltaRetentionResult:
        """Publish a Delta retention floor and reclaim older Parquet files."""
        return self._maintenance.apply_retention(
            minimum_snapshot_id=minimum_snapshot_id, online=online
        )

    def recover_retention_pins(
        self, incarnation_id: str, *, verifier: Path | None = None
    ) -> DeltaPinRecoveryResult:
        return self._maintenance.recover_retention_pins(incarnation_id, verifier=verifier)

    def plan_retention(
        self,
        *,
        keep_last: int,
        min_age_hours: int = 0,
        protect_from_snapshot_id: int | None = None,
    ) -> RetentionPlan:
        """Preview data retention against the published head."""
        return self._maintenance.plan_retention(
            keep_last=keep_last,
            min_age_hours=min_age_hours,
            protect_from_snapshot_id=protect_from_snapshot_id,
        )


__all__ = ["DatabaseAdminUseCase", "InitializationResult", "RecoveryReport", "VerificationReport"]
