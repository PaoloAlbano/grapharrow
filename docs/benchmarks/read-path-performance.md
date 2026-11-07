# ANA, Cypher, and Parquet Read Performance

Measured on November 6, 2026, on macOS arm64 with Python 3.12.11 and
PyArrow 23.0.1. The baseline was `6c4075e`. Both versions read the same local
snapshot for each workload. Times are warm medians in milliseconds. Fixture
generation, server startup, and profiling overhead are excluded from the
latency measurements.

## ANA

Each node in the synthetic graph has three outgoing `LINK` relationships with
empty property maps. The workload uses the same fixture shape as
[`benchmark_ana_sessions.py`](../../scripts/benchmark_ana_sessions.py).
It opens a fresh session, looks up a node, scans and projects 1,000 nodes, fetches
100 rows, expands one and 250 seeds, and runs a two-hop pattern from the 250
seeds with a 500-row limit. HTTP measurements use the server over loopback.
Each cell reports baseline and optimized medians over seven repetitions.

| Operation | Native, 5k nodes | Native, 25k nodes | HTTP, 25k nodes |
| --- | ---: | ---: | ---: |
| Open session | 48.82 → 35.29 | 241.14 → 171.86 | 242.58 → 171.17 |
| Scan 1,000 nodes | 13.25 → 9.00 | 13.85 → 9.17 | 14.85 → 10.01 |
| Expand 250 seeds | 18.10 → 12.51 | 20.76 → 13.87 | 22.03 → 14.83 |
| Two-hop pattern from 250 seeds | 52.77 → 29.40 | 64.65 → 31.97 | 66.38 → 33.56 |
| Fetch 100 rows | 0.096 → 0.096 | 0.098 → 0.096 | 0.802 → 0.800 |

Node scans materialize selected rows with a single Arrow take. Larger
traversals group candidate edges by source, preserving edge order and input
multiplicity. Session discovery skips decoding empty property maps and reads
the edge columns it needs directly. Both fixture sizes produced identical
native result payloads, including fetched pages.

Session-opening gains depend on the distribution of edge properties. The
fixtures use uniform out-degree and the benchmark measures single-session
latency. Peak memory and throughput under concurrent traversals are outside
this measurement.

## Cypher

The [Cypher benchmark](../../scripts/benchmark_cypher_engine.py) supplies the
fixtures and queries. The 20,000-node snapshot has 120,000 relationships. The
larger run reuses a 100,000-node, 600,000-relationship snapshot from an earlier
benchmark, with one relationship type and a source adjacency projection.
Both runs use seed 42. Each case uses a read transaction, one warmup, and
five measured calls. The table shows selected results with the planner enabled.

| Query | 20k nodes | 100k nodes |
| --- | ---: | ---: |
| Property equality lookup | 47.86 → 6.15 | 222.75 → 23.65 |
| Relationship property filter, sort and limit | 102.19 → 46.36 | 466.00 → 225.70 |
| Group and collect neighbors | 142.75 → 105.49 | 681.62 → 536.31 |
| Optional match projection | 96.10 → 45.63 | 497.70 → 206.61 |
| Variable-length wildcard traversal, sort and limit | 166.76 → 99.06 | 665.54 → 322.84 |

Profiles on the smaller graph showed that building node bindings dominated
several queries. The executor now filters restrictive property candidates
before encoding labels, tests canonical label membership in Arrow, and
reuses JSON encodings for repeated label lists. The property lookup still
loads the snapshot and scans the property projection.

All 27 queries on the smaller snapshot and all five on the larger snapshot
returned matching result digests with the planner enabled. The smaller
snapshot was also measured with the planner disabled. One recommendation
query with `collect()` and unordered input produced a different raw digest
in that mode on the initial run. A separate comparison returned the same 100
ordered result rows, including the lists.

## Parquet layout

The layout experiment used copies of the existing files. It compared Snappy
and Zstandard level 1 with the default row group size, then sorted copies
with 4,096 rows per group. The filtered reads use a single equality predicate
on `edge_id`, `external_id`, or `src_id`, respectively, through
`pyarrow.parquet.read_table(..., filters=...)`. Each read case uses one warmup
and 21 measured calls. File writes use one warmup and seven measured calls.
Write times exclude sorting and publishing a GraphArrow snapshot.

Selected results on the 100,000-node snapshot follow. Sizes use decimal MB.
The Snappy baseline was re-encoded with the same default row group size for
a comparable write measurement.

| Dataset | Variant | Size (MB) | Full read (ms) | Filtered read (ms) | Write (ms) |
| --- | --- | ---: | ---: | ---: | ---: |
| Canonical edges | Snappy | 15.96 | 14.76 | 14.48 | 70.66 |
| Canonical edges | Zstandard | 9.08 | 17.54 | 16.79 | 73.83 |
| Canonical edges | Zstandard, sorted groups | 13.08 | 9.15 | 1.68 | 150.48 |
| Person properties | Snappy | 2.89 | 1.66 | 1.64 | 15.46 |
| Person properties | Zstandard, sorted groups | 1.66 | 1.93 | 0.52 | 23.07 |
| Source adjacency | Snappy | 6.15 | 3.50 | 3.45 | 29.04 |
| Source adjacency | Zstandard, sorted groups | 6.22 | 4.67 | 1.32 | 71.50 |

The smaller snapshot has about 10.42 MB of Parquet files in its current
manifest, including 4.13 MB of canonical data and 6.29 MB of derived
projections and indexes. Those sizes exclude previous snapshots and metadata.
The projections trade space and write work for access paths used by reads.

The measurements support keeping the current layout for beta. Sorted groups
helped selective Parquet reads, while the effect on scans and writes depended
on the dataset. The tested ANA and Cypher paths apply predicates after
loading the relevant data. A future layout experiment can pair reader
predicate pushdown with row group tuning and measure end-to-end latency,
snapshot publication cost, and remote I/O together.

## Follow-up on avoidable reads

The baseline for this round includes the ANA and Cypher optimizations above.
On the 100,000-node snapshot, node-only `MATCH` queries skip the canonical
relationship table. The typed endpoint-range fast path reads its adjacency
projection directly. Ordinary traversals and relationship predicates such
as `EXISTS` still have access to canonical edges.

| Cypher query | Planner on (ms) | Planner off (ms) |
| --- | ---: | ---: |
| Property equality in `WHERE` | 21.36 → 8.19 | 21.17 → 8.31 |
| Inline property equality | 22.03 → 8.46 | 22.17 → 8.33 |
| Scalar filters, sort and limit | 35.64 → 22.31 | 35.56 → 22.43 |
| Prefix filter, sort and limit | 23.77 → 10.26 | 101.93 → 90.00 |
| Optional match projection | 209.64 → 214.14 | 242.17 → 246.53 |
| Variable-length wildcard traversal | 325.74 → 324.22 | 327.65 → 332.96 |

The six cases produced identical result digests before and after the change,
with the planner enabled and disabled. The relationship queries serve as
controls. Their medians varied within roughly 2.2%.

A local prototype applied the equality predicate in the Parquet scanner
for the same Cypher point lookup. It rotated four variants against one
read session, with four warmups and 21 timed queries per variant. All four
returned the same row. The original projection took 7.03 ms unfiltered and
6.93 ms with pushdown. With pushdown and 4,096-row groups sorted by
`external_id`, Snappy took 5.95 ms and Zstandard level 1 took 5.91 ms.
This prototype used copies of the property projection and left publication
costs out of the query measurements. The existing layout yielded little
benefit from reader pushdown on this query. The production path keeps the
existing layout and predicate handling.

ANA `node_search` now filters the projection's node IDs directly. For a
single matching ID it checks the node's label using the session's position
index. When there are multiple matches, it intersects the IDs with the
label's node list in canonical order before applying the result limit.

| ANA native lookup | 5k nodes (ms) | 25k nodes (ms) |
| --- | ---: | ---: |
| One match | 0.451 → 0.202 | 1.103 → 0.260 |
| No match | 0.291 → 0.115 | 1.076 → 0.133 |
| Many matches, limited to 100 | 1.427 → 1.206 | 2.881 → 1.731 |

Each case uses four warmups and 21 timed calls on the same fixtures. Each
call opens a fresh session before the timer starts. The measurements exclude
session setup and fetch. Fetched payloads match the baseline at both sizes.

[Supporting measurements](read-path-performance.json)
