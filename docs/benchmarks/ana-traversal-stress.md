# Concurrent ANA Traversals on High-Degree Nodes

Measured on November 7, 2026, with Python 3.12.11 and PyArrow 23.0.1 on
macOS arm64. The baseline includes the previous
[read-path optimizations](read-path-performance.md). This experiment measures
native ANA calls in separate workflows running in one process.

## Fixture and measurement

The snapshot contains 10,258 nodes and 160,750 `LINK` relationships. Eight
hubs each have 20,000 outgoing relationships to 10,000 targets, including
parallel edges. Another 250 seeds each have three outgoing relationships.
The low-degree control reads the same snapshot as the hub cases.

Each case runs in a fresh worker process with one or four independent
workflows sharing the server byte budget. Sessions and seed handles are
created before measurement. A barrier synchronizes each round of calls.
There are two warmup rounds and 21 measured rounds per workflow, giving
21 or 84 latency samples per case. The p95 uses the nearest-rank percentile.
Fetch, assertions, session creation, and barrier waiting are outside the timer.

The parent process samples worker RSS with `ps` approximately every 15 ms.
The reported growth is the highest sampled RSS minus RSS after session and
seed setup. It includes retained handles, warmup calls, Python allocator
retention, Arrow buffers, and temporary allocations during the whole run.
All returned handles are retained until workflow shutdown. Sampling can miss
short allocation peaks. The JSON also records the process lifetime RSS
high-water mark before and after the run.

## Results

Cells show baseline and optimized measurements. Latencies are milliseconds,
and RSS growth is MiB for the worker process.

| Workload | p95, one workflow | p95, four workflows | RSS growth, four workflows |
| --- | ---: | ---: | ---: |
| Expand one hub, limit 1 | 20.42 → 10.32 | 72.38 → 33.69 | 81.8 → 66.7 |
| Expand eight hubs, limit 10 | 107.21 → 10.61 | 449.16 → 35.66 | 258.0 → 107.5 |
| Expand eight hubs, limit 100 | 146.08 → 12.23 | 425.23 → 40.72 | 269.6 → 94.0 |
| Expand 250 low-degree seeds | 17.00 → 19.96 | 107.68 → 111.13 | 127.4 → 123.7 |
| One-hop pattern from a hub, limit 10 | 255.95 → 240.51 | 1001.26 → 1065.45 | 264.2 → 183.1 |

The low-degree control's median decreased from 15.00 to 13.74 ms with one
workflow and from 50.82 to 47.99 ms with four, while its p95 increased.
One worker run per configuration and these sample counts characterize this
local workload. They do not establish a production latency bound or a
statistically significant improvement for the control and pattern cases.

## Changes and correctness

If a grouped candidate set exceeds the larger of 1,024 rows and the requested
output limit, `expand` uses lazy per-source iteration and decodes batches of
at most 256 edges. It can stop decoding once the requested output is available.
Smaller candidate sets keep the grouped execution path.
Projection removes unused edge
columns before filtering, and Arrow deduplicates target IDs before decoding
the complete union-count branch.

The output limit continues to apply to returned rows. Full branch membership
is retained for compatible union counts, so an expansion returning 10 rows
can still represent 10,000 distinct reachable targets. That accounting needs
additional work and memory even for a small result limit.

Every timed call verifies the returned row count, complete branch membership,
and ownership of returned entity references. Fetched result digests match
between baseline and optimized runs for all ten configurations. Retained
handle and row counts match, and the shared byte budget returns to zero after
workflow cleanup. Unit regressions additionally cover incoming and
bidirectional edges, self-loops, wrong target labels, parallel edges, repeated
inputs, and full union counts after a limited expansion.

The pattern control invokes `multi_hop_query` with one hop. The general
pattern executor still constructs intermediate binding rows before final
projection, deduplication, ordering, and limit. A small final limit therefore
does not bound its intermediate memory. Retained-state quotas measure
admitted session data, rather than peak process RSS. This experiment measures
native thread concurrency. HTTP admission, scheduling, and network latency
are outside this workload.

## Reproduction

Run the benchmark from the repository root with development dependencies
available. Use a new local database path for fixture creation.

```sh
uv run --group dev python scripts/benchmark_ana_traversals.py \
  --database .benchmarks/ana-hubs \
  --build-fixture \
  --degree 20000 \
  --sessions 1 4 \
  --warmups 2 \
  --repeats 21 \
  --output .benchmarks/ana-traversals.json
```

Omit `--build-fixture` to reuse the same snapshot with another implementation.
Keep `--degree` equal to the fixture's hub degree. The sampler supports macOS
and Linux and needs permission to inspect the worker process's RSS.

[Supporting measurements](ana-traversal-stress.json)
