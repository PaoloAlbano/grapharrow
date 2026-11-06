# ANA Session Resource Benchmark

Measured on 2026-11-06. This report compares the pre-quota adapter with the
bounded adapter using the same deterministic graph and tool workload. The
upstream contract is Agent-Native Graph v0.1.2. These are local development
measurements; the raw results identify each adapter source tree by SHA-256.

## Method

- Python 3.12.11; macOS-27.2-arm64-arm-64bit; arm64.
- 5,000 Device nodes with a name, integer and 160-character description; 15,000 LINK edges.
- One and four simultaneous workflows, three fresh subprocesses per case.
- Workflows stay open together; creation and tool calls execute sequentially.
- Database creation and process startup excluded from operation latency.
- Each workflow performs node_scan (1,000 rows), exact node_search, expand (three neighbors),
  project, a 100-row fetch, count_handle and a scalar fetch.
- Current RSS sampled outside the timed sections using ps on macOS, or /proc on Linux.
- Results below are medians; each create/tool sample is retained in the raw files.
- Snapshot and handle accounting is approximate retained object/buffer size. It is separate from RSS.

## Session opening and resident memory

| Workflows | Open median before (ms) | Open median bounded (ms) | Used RSS delta before (MiB) | Used RSS delta bounded (MiB) | Accounted state per workflow (MiB) |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 45.40 | 47.97 | 19.55 | 20.50 | 4.79 |
| 4 | 44.87 | 48.48 | 45.67 | 46.59 | 4.79 |

## Tool latency

The following uses the four-workflow case. Fetch combines the 100-row page and
scalar page samples, as recorded in the raw runs.

| Tool | Before median (ms) | Bounded median (ms) |
| --- | ---: | ---: |
| `node_scan` | 7.613 | 13.782 |
| `node_search` | 0.381 | 0.416 |
| `expand` | 0.388 | 0.414 |
| `project` | 1.090 | 1.814 |
| `fetch` | 0.014 | 0.067 |
| `count_handle` | 0.035 | 0.153 |

## Interpretation and limits

Accounting adds a traversal of each newly stored row graph. The implementation
caches that charge and only rechecks metadata/lineage and the latest fetched page
at finalization. It does not rescan all retained rows on each tool call.
Creation includes counting the snapshot tables and Python lookup structures.
The benchmark exposes this overhead; it does not establish a throughput target.

Four workflows retain more memory than one because each pins its own Arrow
snapshot and property projections. The current implementation does not share a
snapshot cache between workflows. RSS includes imports, allocator arenas and
Arrow allocations; closing a workflow releases its accounted budget, while the
allocators may keep resident pages for reuse. The raw files include the RSS
samples after close and garbage collection.

Quotas bound retained state, not allocations made while loading a snapshot or
constructing an intermediate result. They are not hard RSS limits, execution
deadlines or protection against every hostile query. Three repetitions on one
workstation do not establish tail latency, multi-user scaling or statistical
significance. No cloud service or model endpoint was used.

## Reproduction and raw results

```bash
uv run python scripts/benchmark_ana_sessions.py --nodes 5000 --sessions 1 4 --repeats 3 \
  --output /tmp/ana-sessions.json
```

- [Runner](../../scripts/benchmark_ana_sessions.py)
- [Pre-quota raw results](ana-session-resources-baseline.json)
- [Bounded raw results](ana-session-resources-bounded.json)

The pre-quota results are preserved from the earlier local adapter revision.
Rerunning the current runner measures the bounded adapter. Platform, allocator
and background load differences can change both timings and RSS.
