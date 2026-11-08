"""Measure Parquet predicate pushdown with production and experimental row groups."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import math
import os
import platform
import statistics
import time
from collections.abc import Callable
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Lock
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from grapharrow.adapters import FileSystemAdapter, ParquetAdapter
from grapharrow.adapters.scan_filter import dataset_filter
from grapharrow.app import build_application
from grapharrow.config import GraphArrowSettings
from grapharrow.domain.scan import ScanPredicate


class _CountedFile(io.RawIOBase):
    def __init__(self, file: Any, counter: _ReadCounter) -> None:
        super().__init__()
        self.file, self.counter = file, counter

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        data = self.file.read(size)
        with self.counter.lock:
            self.counter.bytes += len(data)
            self.counter.calls += 1
        return data

    def readinto(self, target: Any) -> int:
        data = self.read(len(target))
        target[: len(data)] = data
        return len(data)

    def seek(self, offset: int, whence: int = 0) -> int:
        return self.file.seek(offset, whence)

    def tell(self) -> int:
        return self.file.tell()

    def close(self) -> None:
        self.file.close()
        super().close()


class _ReadCounter(pafs.FileSystemHandler):
    """Count delivered local file bytes separately from the timed measurements."""

    def __init__(self) -> None:
        self.fs = pafs.LocalFileSystem()
        self.bytes = self.calls = 0
        self.lock = Lock()

    def get_type_name(self) -> str:
        return "counted-local"

    def normalize_path(self, path: str) -> str:
        return self.fs.normalize_path(path)

    def get_file_info(self, paths: Any) -> Any:
        return self.fs.get_file_info(paths)

    def get_file_info_selector(self, selector: Any) -> Any:
        return self.fs.get_file_info(selector)

    def open_input_file(self, path: str) -> Any:
        return pa.PythonFile(_CountedFile(self.fs.open_input_file(path), self), mode="r")

    def open_input_stream(self, path: str) -> Any:
        return self.open_input_file(path)

    def _readonly(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("Read-only benchmark filesystem")

    create_dir = delete_dir = delete_dir_contents = delete_root_dir_contents = _readonly
    delete_file = move = copy_file = open_output_stream = open_append_stream = _readonly


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def measure(operation: Callable[[], Any], repeats: int) -> dict[str, Any]:
    samples, signatures = [], set()
    for _ in range(repeats + 2):
        start = time.perf_counter_ns()
        value = operation()
        samples.append((time.perf_counter_ns() - start) / 1e6)
        signatures.add(digest(value))
    assert len(signatures) == 1
    return {
        "warm_ms": samples[2:],
        "median_ms": statistics.median(samples[2:]),
        "result_sha256": signatures.pop(),
    }


def residual(table: Any, case: str, target: str, middle: int) -> Any:
    if case == "name":
        mask = pc.equal(table["name"], target)
    elif case == "range":
        mask = pc.and_(
            pc.greater_equal(table["value"], middle), pc.less(table["value"], middle + 10)
        )
    elif case == "missing":
        mask = pc.equal(table["name"], "missing-device")
    else:
        mask = pc.greater_equal(table["value"], 0)
    return (
        table.filter(mask)
        .select(["node_id", "name", "value"])
        .sort_by([("node_id", "ascending")])
        .to_pylist()
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodes", type=int, default=100000)
    parser.add_argument("--repeats", type=int, default=15)
    parser.add_argument("--row-group-size", type=int, default=4096)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert args.nodes > 100 and args.repeats > 0 and args.row_group_size > 0
    results: dict[str, Any] = {
        "python": platform.python_version(),
        "pyarrow": pa.__version__,
        "platform": platform.platform(),
        "nodes": args.nodes,
        "repeats": args.repeats,
        "experimental_row_group_size": args.row_group_size,
    }
    with TemporaryDirectory(prefix="grapharrow-parquet-pushdown-") as temporary:
        root = Path(temporary)
        app = build_application(
            root / "db",
            settings=GraphArrowSettings(property_index_max_bytes=0, ana_read_cache_enabled=False),
        )
        # Permute names to distinguish row-group sizing from clustering by the search key.
        multiplier = 7919
        while math.gcd(multiplier, args.nodes) != 1:
            multiplier += 1
        names = [f"device-{(i * multiplier) % args.nodes:06d}" for i in range(args.nodes)]
        app.use_cases.graph.create_nodes_bulk(
            table=pa.table(
                {
                    "name": names,
                    "value": list(range(args.nodes)),
                    "description": ["x" * 160] * args.nodes,
                }
            ),
            default_labels=("Device",),
        )
        manifest = app.repositories.snapshot.load_manifest()
        projection = root / "db" / manifest.node_properties[0].path
        table = pq.read_table(projection)
        variants = {"production": projection}
        for label, sort_by in [("small_groups", None), ("name_sorted_groups", "name")]:
            variant = root / f"{label}.parquet"
            copy = table.sort_by([(sort_by, "ascending")]) if sort_by else table
            started = time.perf_counter_ns()
            pq.write_table(copy, variant, row_group_size=args.row_group_size)
            variants[label] = variant
            results.setdefault("experimental_writes", {})[label] = {
                "write_ms_excluding_sort": (time.perf_counter_ns() - started) / 1e6,
                "file_bytes": variant.stat().st_size,
            }
        target, middle = names[42], args.nodes // 2
        cases = {
            "name": (ScanPredicate("name", "=", target),),
            "range": (
                ScanPredicate("value", ">=", middle),
                ScanPredicate("value", "<", middle + 10),
            ),
            "missing": (ScanPredicate("name", "=", "missing-device"),),
            "broad": (ScanPredicate("value", ">=", 0),),
        }
        reader_results = {}
        for label, path in variants.items():
            dataset = ds.dataset(path, format="parquet")
            fragment = next(dataset.get_fragments())
            total_groups = len(fragment.split_by_row_group())
            record = {"file_bytes": path.stat().st_size, "row_groups": total_groups, "cases": {}}
            for case, predicates in cases.items():
                measurements = {}
                for enabled in [False, True]:
                    reader = ParquetAdapter(filter_pushdown_enabled=enabled)

                    def read(reader=reader, path=path, predicates=predicates, case=case):
                        return residual(
                            reader.read_dataset_table(
                                [path], columns=["node_id", "name", "value"], predicates=predicates
                            ),
                            case,
                            target,
                            middle,
                        )

                    stats = measure(read, args.repeats)
                    counter = _ReadCounter()
                    counted_fs = pafs.PyFileSystem(counter)
                    instrumented = FileSystemAdapter(
                        mounts={
                            "bench://local": (
                                pafs.SubTreeFileSystem(str(path.parent), counted_fs),
                                "",
                            )
                        }
                    )
                    counted = ParquetAdapter(instrumented, filter_pushdown_enabled=enabled)
                    candidates = counted.read_dataset_table(
                        [f"bench://local/{path.name}"],
                        columns=["node_id", "name", "value"],
                        predicates=predicates,
                    )
                    assert (
                        digest(residual(candidates, case, target, middle)) == stats["result_sha256"]
                    )
                    stats.update(
                        candidate_rows=candidates.num_rows,
                        candidate_arrow_bytes=candidates.nbytes,
                        file_bytes_delivered=counter.bytes,
                        file_read_calls=counter.calls,
                    )
                    expression = dataset_filter(dataset.schema, predicates) if enabled else None
                    stats["eligible_row_groups"] = len(
                        fragment.split_by_row_group(filter=expression)
                    )
                    measurements["on" if enabled else "off"] = stats
                assert measurements["off"]["result_sha256"] == measurements["on"]["result_sha256"]
                record["cases"][case] = measurements
            reader_results[label] = record
        results["reader"] = reader_results
        queries = {
            "name": (
                "MATCH (d:Device {name: $name}) RETURN d.name AS name, d.value AS value",
                {"name": target},
            ),
            "range": (
                "MATCH (d:Device) WHERE d.value >= $low AND d.value < $high "
                "RETURN d.name AS name ORDER BY name",
                {"low": middle, "high": middle + 10},
            ),
            "missing": (
                "MATCH (d:Device {name: $name}) RETURN d.name AS name",
                {"name": "missing-device"},
            ),
            "broad": ("MATCH (d:Device) WHERE d.value >= 0 RETURN count(d) AS count", {}),
        }
        results["cypher"] = {}
        for planner in [True, False]:
            previous = os.environ.get("GRAPHARROW_DISABLE_CYPHER_PLANNER")
            os.environ["GRAPHARROW_DISABLE_CYPHER_PLANNER"] = "0" if planner else "1"
            try:
                phase = {}
                for case, (query, params) in queries.items():
                    modes = {}
                    for enabled in [False, True]:
                        app.adapters.parquet.filter_pushdown_enabled = enabled
                        modes["on" if enabled else "off"] = measure(
                            lambda query=query, params=params: (
                                app.use_cases.cypher.execute(query, params).rows
                            ),
                            args.repeats,
                        )
                    assert modes["off"]["result_sha256"] == modes["on"]["result_sha256"]
                    phase[case] = modes
                results["cypher"]["planner_on" if planner else "planner_off"] = phase
            finally:
                if previous is None:
                    os.environ.pop("GRAPHARROW_DISABLE_CYPHER_PLANNER", None)
                else:
                    os.environ["GRAPHARROW_DISABLE_CYPHER_PLANNER"] = previous
        assert app.use_cases.admin.verify(verify_checksums=True).ok
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(results, indent=2) + "\n")
    print(f"Pushdown benchmark saved to {args.output}")


if __name__ == "__main__":
    main()
