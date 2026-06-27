"""Benchmark JSON serialization used for nested tabular join payloads."""

from __future__ import annotations

import argparse
import json
import statistics
import time
import zlib
from collections.abc import Callable
from dataclasses import dataclass

import orjson

Payload = dict[str, object]
Encoder = Callable[[Payload], str | bytes]


@dataclass(frozen=True, slots=True)
class BenchmarkResult:
    name: str
    median_seconds: float
    best_seconds: float
    worst_seconds: float
    runs: tuple[float, ...]
    speedup_vs_stdlib: float | None
    encoded_bytes: int
    checksum: int


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark stdlib json vs orjson for tabular nested join payloads."
    )
    parser.add_argument("--rows", type=int, default=50_000, help="Number of payload rows.")
    parser.add_argument("--repeats", type=int, default=7, help="Measured runs per encoder.")
    parser.add_argument("--warmups", type=int, default=2, help="Warmup runs per encoder.")
    args = parser.parse_args()

    payloads = build_payloads(args.rows)
    encoders: dict[str, Encoder] = {
        "stdlib_json_dumps": json.dumps,
        "orjson_dumps_decode": lambda value: orjson.dumps(value).decode("utf-8"),
        "orjson_dumps_bytes": orjson.dumps,
    }

    results: list[BenchmarkResult] = []
    stdlib_median: float | None = None
    for name, encoder in encoders.items():
        result = measure_encoder(
            name=name,
            payloads=payloads,
            encoder=encoder,
            repeats=args.repeats,
            warmups=args.warmups,
            stdlib_median=stdlib_median,
        )
        if name == "stdlib_json_dumps":
            stdlib_median = result.median_seconds
            result = BenchmarkResult(
                name=result.name,
                median_seconds=result.median_seconds,
                best_seconds=result.best_seconds,
                worst_seconds=result.worst_seconds,
                runs=result.runs,
                speedup_vs_stdlib=1.0,
                encoded_bytes=result.encoded_bytes,
                checksum=result.checksum,
            )
        results.append(result)

    print_report(rows=args.rows, results=results)


def build_payloads(rows: int) -> list[Payload]:
    """Build deterministic nested values similar to Arrow list/struct payloads."""
    return [
        {
            "labels": ["Person", "Benchmark"] if index % 2 == 0 else ["Company"],
            "properties": {
                "external_id": f"node-{index:08d}",
                "score": (index % 10_000) / 10_000,
                "active": index % 5 != 0,
                "tags": [f"tag-{index % 17}", f"tag-{index % 31}"],
            },
            "events": [
                {"kind": "seen", "weight": index % 7},
                {"kind": "linked", "weight": index % 13},
            ],
        }
        for index in range(rows)
    ]


def measure_encoder(
    *,
    name: str,
    payloads: list[Payload],
    encoder: Encoder,
    repeats: int,
    warmups: int,
    stdlib_median: float | None,
) -> BenchmarkResult:
    for _ in range(warmups):
        encode_all(payloads, encoder)

    runs: list[float] = []
    encoded_bytes = 0
    checksum = 0
    for _ in range(repeats):
        started = time.perf_counter()
        encoded_bytes, checksum = encode_all(payloads, encoder)
        runs.append(time.perf_counter() - started)

    median_seconds = statistics.median(runs)
    return BenchmarkResult(
        name=name,
        median_seconds=median_seconds,
        best_seconds=min(runs),
        worst_seconds=max(runs),
        runs=tuple(runs),
        speedup_vs_stdlib=None if stdlib_median is None else stdlib_median / median_seconds,
        encoded_bytes=encoded_bytes,
        checksum=checksum,
    )


def encode_all(payloads: list[Payload], encoder: Encoder) -> tuple[int, int]:
    """Encode every payload and checksum outputs so work cannot be optimized away."""
    total_bytes = 0
    checksum = 1
    for payload in payloads:
        encoded = encoder(payload)
        raw = encoded if isinstance(encoded, bytes) else encoded.encode("utf-8")
        total_bytes += len(raw)
        checksum = zlib.adler32(raw, checksum)
    return total_bytes, checksum


def print_report(*, rows: int, results: list[BenchmarkResult]) -> None:
    print(f"Tabular nested JSON serialization benchmark (rows={rows})")
    print("")
    print("| encoder | median | best | worst | speedup vs stdlib | encoded MB | checksum |")
    print("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
    for result in results:
        speedup = "-" if result.speedup_vs_stdlib is None else f"{result.speedup_vs_stdlib:.2f}x"
        print(
            f"| {result.name} "
            f"| {result.median_seconds:.4f}s "
            f"| {result.best_seconds:.4f}s "
            f"| {result.worst_seconds:.4f}s "
            f"| {speedup} "
            f"| {result.encoded_bytes / 1_000_000:.2f} "
            f"| {result.checksum} |"
        )


if __name__ == "__main__":
    main()
