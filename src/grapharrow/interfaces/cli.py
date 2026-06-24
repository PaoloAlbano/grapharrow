"""Command-line entrypoints for GraphArrow development workflows."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path

import pyarrow.csv as pacsv  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from grapharrow.domain import DatasetFile
from grapharrow.interfaces.embedded import Database


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level command-line parser."""

    parser = argparse.ArgumentParser(prog="grapharrow")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init", help="Initialize a GraphArrow database path.")
    init_parser.add_argument("path", help="Database directory to create or bootstrap.")

    show_catalog_parser = subparsers.add_parser(
        "show-catalog",
        help="Print the current catalog as formatted JSON.",
    )
    show_catalog_parser.add_argument("path", help="Database directory to inspect.")

    show_snapshot_parser = subparsers.add_parser(
        "show-snapshot",
        help="Print the current snapshot manifest as formatted JSON.",
    )
    show_snapshot_parser.add_argument("path", help="Database directory to inspect.")

    verify_parser = subparsers.add_parser(
        "verify",
        help="Verify the current catalog and snapshot pointers.",
    )
    verify_parser.add_argument("path", help="Database directory to inspect.")
    verify_parser.add_argument(
        "--verify-checksums",
        action="store_true",
        help="Verify SHA-256 checksums for manifest-referenced files when present.",
    )

    recover_parser = subparsers.add_parser(
        "recover",
        help="Run a conservative recovery pass.",
    )
    recover_parser.add_argument("path", help="Database directory to inspect.")
    recover_parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply cleanup actions instead of running in dry-run mode.",
    )
    recover_parser.add_argument(
        "--no-cleanup-orphan-staged",
        action="store_true",
        help="Do not clean orphan staged paths.",
    )
    recover_parser.add_argument(
        "--verify-checksums",
        action="store_true",
        help="Verify SHA-256 checksums for manifest-referenced files when present.",
    )

    compact_parser = subparsers.add_parser(
        "compact",
        help="Rewrite the current graph state into a fresh snapshot.",
    )
    compact_parser.add_argument("path", help="Database directory to compact.")

    vacuum_parser = subparsers.add_parser(
        "vacuum",
        help="Remove obsolete manifests, Parquet files, and orphan staged directories.",
    )
    vacuum_parser.add_argument("path", help="Database directory to vacuum.")

    bulk_nodes_parser = subparsers.add_parser(
        "bulk-add-nodes",
        help="Create multiple nodes from a JSON or JSONL file.",
    )
    bulk_nodes_parser.add_argument("path", help="Database directory to update.")
    bulk_nodes_parser.add_argument("input", help="Path to a JSON array or JSONL file.")
    bulk_nodes_parser.add_argument(
        "--default-label",
        action="append",
        default=[],
        help="Default label to apply when a row does not contain a labels field.",
    )

    bulk_edges_parser = subparsers.add_parser(
        "bulk-add-edges",
        help="Create multiple edges from a JSON or JSONL file.",
    )
    bulk_edges_parser.add_argument("path", help="Database directory to update.")
    bulk_edges_parser.add_argument("input", help="Path to a JSON array or JSONL file.")
    bulk_edges_parser.add_argument(
        "--source-label",
        help="Source node label used for business-key edge references.",
    )
    bulk_edges_parser.add_argument(
        "--source-key",
        help="Source node property key used for business-key edge references.",
    )
    bulk_edges_parser.add_argument(
        "--target-label",
        help="Target node label used for business-key edge references.",
    )
    bulk_edges_parser.add_argument(
        "--target-key",
        help="Target node property key used for business-key edge references.",
    )

    import_nodes_parser = subparsers.add_parser(
        "import-nodes",
        help="Import nodes from a JSON, JSONL, CSV, or Parquet file.",
    )
    import_nodes_parser.add_argument("path", help="Database directory to update.")
    import_nodes_parser.add_argument("input", help="Path to a JSON, JSONL, CSV, or Parquet file.")
    import_nodes_parser.add_argument(
        "--default-label",
        action="append",
        default=[],
        help="Default label to apply when a row does not contain a labels field.",
    )

    import_edges_parser = subparsers.add_parser(
        "import-edges",
        help="Import edges from a JSON, JSONL, CSV, or Parquet file.",
    )
    import_edges_parser.add_argument("path", help="Database directory to update.")
    import_edges_parser.add_argument("input", help="Path to a JSON, JSONL, CSV, or Parquet file.")
    import_edges_parser.add_argument(
        "--source-label",
        help="Source node label used for business-key edge references.",
    )
    import_edges_parser.add_argument(
        "--source-key",
        help="Source node property key used for business-key edge references.",
    )
    import_edges_parser.add_argument(
        "--target-label",
        help="Target node label used for business-key edge references.",
    )
    import_edges_parser.add_argument(
        "--target-key",
        help="Target node property key used for business-key edge references.",
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the GraphArrow command-line interface."""

    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "init":
        database = Database(args.path)
        result = database.initialize()
        print(
            f"Initialized GraphArrow database at {database.path} "
            f"with catalog version {result.catalog.version.value} and "
            f"snapshot {result.snapshot.snapshot_id.value}."
        )
        return 0

    if args.command == "show-catalog":
        database = Database(args.path)
        catalog = database.load_catalog()
        payload = {
            "version": catalog.version.value,
            "labels": [
                {"id": definition.id.value, "name": definition.name}
                for definition in catalog.labels
            ],
            "relationship_types": [
                {"id": definition.id.value, "name": definition.name}
                for definition in catalog.relationship_types
            ],
            "properties": [
                {
                    "name": definition.name,
                    "owner_kind": definition.owner_kind,
                    "owner_name": definition.owner_name,
                    "data_type": definition.data_type,
                    "nullable": definition.nullable,
                    "deprecated": definition.deprecated,
                }
                for definition in catalog.properties
            ],
            "constraints": [
                {
                    "kind": definition.kind,
                    "owner_kind": definition.owner_kind,
                    "owner_name": definition.owner_name,
                    "property_name": definition.property_name,
                    "source_labels": list(definition.source_labels),
                    "target_labels": list(definition.target_labels),
                }
                for definition in catalog.constraints
            ],
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    if args.command == "show-snapshot":
        database = Database(args.path)
        pointer = database.load_current_snapshot_pointer()
        manifest = database.load_snapshot()
        payload = {
            "current_pointer": {
                "snapshot_id": pointer.snapshot_id.value,
                "manifest_path": pointer.manifest_path,
                "commit_txid": pointer.commit_txid,
                "created_at": pointer.created_at,
            },
            "manifest": {
                "snapshot_id": manifest.snapshot_id.value,
                "parent_snapshot_id": (
                    manifest.parent_snapshot_id.value
                    if manifest.parent_snapshot_id is not None
                    else None
                ),
                "commit_txid": manifest.commit_txid,
                "created_at": manifest.created_at,
                "catalog_version": manifest.catalog_version.value,
                "files": {
                    "nodes": [_dataset_file_payload(item) for item in manifest.nodes],
                    "edges": [_dataset_file_payload(item) for item in manifest.edges],
                    "node_labels": [_dataset_file_payload(item) for item in manifest.node_labels],
                    "node_properties": [
                        _dataset_file_payload(item) for item in manifest.node_properties
                    ],
                    "adjacency_by_source": [
                        _dataset_file_payload(item) for item in manifest.adjacency_by_source
                    ],
                    "adjacency_by_dest": [
                        _dataset_file_payload(item) for item in manifest.adjacency_by_dest
                    ],
                    "edge_properties": [
                        _dataset_file_payload(item) for item in manifest.edge_properties
                    ],
                },
            },
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    if args.command == "verify":
        database = Database(args.path)
        verification_report = database.verify(verify_checksums=args.verify_checksums)
        payload = {
            "ok": verification_report.ok,
            "catalog_version": (
                verification_report.catalog_version.value
                if verification_report.catalog_version is not None
                else None
            ),
            "snapshot_pointer": (
                {
                    "snapshot_id": verification_report.snapshot_pointer.snapshot_id.value,
                    "manifest_path": verification_report.snapshot_pointer.manifest_path,
                    "commit_txid": verification_report.snapshot_pointer.commit_txid,
                    "created_at": verification_report.snapshot_pointer.created_at,
                }
                if verification_report.snapshot_pointer is not None
                else None
            ),
            "missing_paths": list(verification_report.missing_paths),
            "checksum_failures": list(verification_report.checksum_failures),
            "warnings": list(verification_report.warnings),
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    if args.command == "recover":
        database = Database(args.path)
        recovery_report = database.recover(
            dry_run=not args.apply,
            cleanup_orphan_staged=not args.no_cleanup_orphan_staged,
            verify_checksums=args.verify_checksums,
        )
        payload = {
            "verification": {
                "ok": recovery_report.verification.ok,
                "catalog_version": (
                    recovery_report.verification.catalog_version.value
                    if recovery_report.verification.catalog_version is not None
                    else None
                ),
                "snapshot_pointer": (
                    {
                        "snapshot_id": (
                            recovery_report.verification.snapshot_pointer.snapshot_id.value
                        ),
                        "manifest_path": (
                            recovery_report.verification.snapshot_pointer.manifest_path
                        ),
                        "commit_txid": recovery_report.verification.snapshot_pointer.commit_txid,
                        "created_at": recovery_report.verification.snapshot_pointer.created_at,
                    }
                    if recovery_report.verification.snapshot_pointer is not None
                    else None
                ),
                "missing_paths": list(recovery_report.verification.missing_paths),
                "checksum_failures": list(recovery_report.verification.checksum_failures),
                "warnings": list(recovery_report.verification.warnings),
            },
            "orphan_staged_paths": list(recovery_report.orphan_staged_paths),
            "removed_paths": list(recovery_report.removed_paths),
            "warnings": list(recovery_report.warnings),
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    if args.command == "compact":
        database = Database(args.path)
        manifest = database.compact()
        print(
            json.dumps(
                {
                    "snapshot_id": manifest.snapshot_id.value,
                    "parent_snapshot_id": (
                        manifest.parent_snapshot_id.value
                        if manifest.parent_snapshot_id is not None
                        else None
                    ),
                    "catalog_version": manifest.catalog_version.value,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    if args.command == "vacuum":
        database = Database(args.path)
        removed_paths = database.vacuum()
        print(json.dumps({"removed_paths": list(removed_paths)}, indent=2, sort_keys=True))
        return 0

    if args.command in {"bulk-add-nodes", "import-nodes"}:
        database = Database(args.path)
        rows = _load_bulk_input_file(args.input)
        with database.write_transaction() as session:
            created_nodes = session.add_nodes_bulk(rows, default_labels=list(args.default_label))
        print(
            json.dumps(
                {
                    "row_count": len(created_nodes),
                    "nodes": list(created_nodes),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    if args.command in {"bulk-add-edges", "import-edges"}:
        database = Database(args.path)
        rows = _load_bulk_input_file(args.input)
        source_key = _reference_key_tuple(args.source_label, args.source_key, role="source")
        target_key = _reference_key_tuple(args.target_label, args.target_key, role="target")
        with database.write_transaction() as session:
            created_edges = session.add_edges_bulk(
                rows,
                source_key=source_key,
                target_key=target_key,
            )
        print(
            json.dumps(
                {
                    "row_count": len(created_edges),
                    "edges": list(created_edges),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    parser.error("Unknown command.")
    return 2


def _dataset_file_payload(dataset_file: DatasetFile) -> dict[str, object]:
    """Handle this internal helper step for the surrounding public workflow."""
    return {
        "path": dataset_file.path,
        "row_count": dataset_file.row_count,
        "kind": dataset_file.kind,
        "checksum": dataset_file.checksum,
        "checksum_algorithm": dataset_file.checksum_algorithm,
        "min_key": dataset_file.min_key,
        "max_key": dataset_file.max_key,
    }


def _load_rows_file(path: str) -> list[dict[str, object]]:
    """Handle this internal helper step for the surrounding public workflow."""
    file_path = Path(path)
    if file_path.suffix == ".jsonl":
        rows: list[dict[str, object]] = []
        for raw_line in file_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            payload = json.loads(line)
            if not isinstance(payload, dict):
                raise ValueError("JSONL input must contain one object per line.")
            rows.append(payload)
        return rows

    payload = json.loads(file_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list) or not all(isinstance(row, dict) for row in payload):
        raise ValueError("Bulk input files must contain a JSON array of objects or JSONL objects.")
    return payload


def _load_bulk_input_file(path: str) -> object:
    """Handle this internal helper step for the surrounding public workflow."""
    file_path = Path(path)
    suffix = file_path.suffix.lower()
    if suffix in {".json", ".jsonl"}:
        return _load_rows_file(path)
    if suffix == ".csv":
        return pacsv.read_csv(file_path)
    if suffix == ".parquet":
        return pq.read_table(file_path)
    raise ValueError("Supported bulk input formats are .json, .jsonl, .csv, and .parquet.")


def _reference_key_tuple(
    label: str | None,
    key: str | None,
    *,
    role: str,
) -> tuple[str, str] | None:
    """Handle this internal helper step for the surrounding public workflow."""
    if label is None and key is None:
        return None
    if label is None or key is None:
        raise ValueError(
            f"Both --{role}-label and --{role}-key are required for business-key edge ingest."
        )
    return (label, key)


if __name__ == "__main__":
    raise SystemExit(main())
