# ANA Seed Filtering and Ranked Discovery

Measured on November 8, 2026, using Python 3.12.11 and PyArrow 26.0.1 on
macOS arm64. The baseline includes the shared adjacency engine and the earlier
ANA read/state optimizations. Both implementations use the same dependencies,
runner and immutable snapshot. Optional ANA result caching is disabled.

## Fixture and measurement

The [runner](../../scripts/benchmark_ana_seed_discovery.py) creates 25,000
`Device` nodes and 50,000 `LINK` edges. Nodes have a name, integer value,
160-character description, a one-element alias list and a one-element tag list.
Every thousandth node has a Unicode name and alias, giving 25 exceptional rows
among otherwise ASCII data. A declared name equality index is available.
Each node has two outgoing edges to successive nodes modulo the graph size.

Each case opens one independent workflow before timing. Calls repeat within
that workflow, with two warmups and 21 measured samples. The first warmup is
reported separately. Timers cover the tool call, including normal admission and
lineage work; fetch and correctness checks are outside the timer. Native
allocation probes are one additional untimed call under `tracemalloc` after
collection. HTTP has no allocation probe. All handles remain until case cleanup.

HTTP uses the actual authenticated Uvicorn service over loopback, retains the
session header and consumes the JSON response. Timings include client/network
processing. Native and HTTP configurations share a fixture, while each has its
own application and cache instances. Workflow setup is outside the timer.

The indexed equality case selects one seed; the range case selects five; list
membership selects 25 seeds and returns ten rows. The two-hop case selects one
seed, returns one row and retains full branch membership. The unfiltered case
is a control. Entity queries cover shared name tokens, context, aliases,
Unicode lowercasing and a miss. Each implementation/configuration has one run;
these measurements do not establish statistical significance or production
latency bounds.

## Tool-call medians

Times are milliseconds, before → optimized. Full samples, first-call times,
allocation probes and retained-resource observations are preserved in the
[supporting results](ana-seed-discovery.json).

| Workload | Native (ms) | HTTP (ms) |
| --- | ---: | ---: |
| seed equal indexed | 110.690 → 0.440 | 114.550 → 1.182 |
| seed range | 111.109 → 0.494 | 115.640 → 1.177 |
| seed list | 115.932 → 39.951 | 120.776 → 41.883 |
| seed unfiltered control | 146.758 → 151.998 | 157.625 → 169.501 |
| seed two hop | 110.652 → 0.754 | 115.879 → 1.475 |
| entity name | 62.262 → 17.597 | 65.075 → 18.569 |
| entity context | 68.485 → 18.226 | 71.638 → 19.077 |
| entity alias | 62.254 → 20.456 | 66.033 → 21.590 |
| entity unicode | 43.194 → 14.216 | 44.742 → 15.092 |
| entity miss | 47.872 → 19.118 | 49.320 → 18.261 |

Selective native seed equality drops from 110.690 to 0.440 ms, and the native
range from 111.109 to 0.494 ms. The list predicate retains Python semantics
while avoiding full entity construction, decreasing from 115.932 to 39.951 ms.
Native entity resolution by name decreases from 62.262 to 17.597 ms. Generic
shared tokens still match many rows, so bounded ranking matters in addition
to simple substring preselection.

The unfiltered native control increases from 146.758 to 151.998 ms. It keeps
the previous full seed-materialization path. This control, run ordering, cache
state and local measurement variability limit conclusions about small changes.
There is no claim of a uniform speedup for unfiltered, small, Unicode-heavy or
otherwise unsupported workloads.

## Python allocation probes

These probes count Python allocator peaks during a separate call. They exclude
native Arrow buffer allocations, resident snapshot state and process RSS.
Gathering/indexing/ranking can still allocate buffers proportional to label
size. The probes are not total-memory or peak-RSS bounds.

| Native workload | Python peak before | Python peak optimized |
| --- | ---: | ---: |
| Indexed seed equality | 59,902,201 B | 11,561 B |
| Seed range | 59,902,201 B | 19,263 B |
| List membership | 59,902,201 B | 8,277,304 B |
| Filtered two-hop pattern | 59,902,433 B | 4,728,384 B |
| Entity name | 17,536,103 B | 31,470 B |
| Entity Unicode | 14,732,743 B | 37,325 B |

The probe constructs 25,000 seed payloads before optimization, versus one for
equality/two-hop, five for range and 25 for list membership afterward. The list
case still decodes 25,000 narrow property rows. The unfiltered control still
constructs all 25,000 payloads and has essentially the same Python allocation
peak, about 59.9 MB.

Entity name/context/alias cases previously scored 25,000 property rows in
Python; the optimized probes score 33. Unicode/miss cases score 25. These
include exceptional rows retained for exact Python semantics. Arrow still
examines column data to select the safe finalists; this is not an O(1) search
or a persistent text index.

Retained handle/row counts and snapshot charges match. The native Unicode-query
case accounts for 264 additional bytes across 24 handles, matching an observed
11-byte cached UTF-8 conversion of its query string. Retained-byte estimates
therefore need not be bit-for-bit identical after different marshalling paths.
Every workflow releases its admitted byte budget to zero on cleanup.

## Execution and correctness

Validated start-variable filters run before constructing workflow-owned seeds.
Eligible native scalar comparisons use Arrow and declared equality indexes.
Unsupported predicates stop the pushdown prefix; remaining predicates evaluate
in order over narrow properties, with canonical fallbacks for incomplete,
legacy, lossy, reserved-name or non-reconstructable projections. Filters remain
residual checks. Input-handle scope and canonical order are preserved, and the
final result limit is not applied to seed selection prematurely. Evaluation
stays lazy relative to eager hop validation, preserving error ordering.

Entity resolution computes exact vector scores for eligible ASCII rows and
retains enough safe finalists from both the hit and alternative rankings.
Canonical order is an explicit last key. Exceptional Unicode, escaped or
nonstandard aliases and nullable-description rows remain available to the
original Python scorer. That scorer processes the selected union and produces
the final best-match class, context scores and alternatives. Invalid scalar or
projection JSON errors are not hidden by excluding an unrelated name.

Small labels (fewer than 64 nodes), search strings longer than 64 lowercased
characters, excessive search-token sets, unsupported schemas/kernels and complex
alias/query cases retain conservative pruning or original scanning. This avoids
unbounded substring enumeration and preserves existing behavior. These are ANA
execution improvements available through native and HTTP calls; no tool/RPC
schema, storage layout or new cache reservation is introduced.

All 20 before/after API digests match. Each native case also matches its HTTP
result. The 68 added regression cases compare predicates and scoring with the
materialized/Python paths and cover canonical identity and ties, Unicode,
reverse matches, aliases, context, mixed numeric/boolean values, NaN, large
integers, null/missing fields, input scope, reordered/incomplete projections,
scalar subclasses, conversion/error ordering and limited full-branch counts.
The [regression suite](../../tests/unit/test_ana_seed_discovery.py) contains
the executable predicate, ranking and error-order comparisons. The
[interface comparison](../interface-comparison.md) describes cross-interface checks.

## Reproduction

The recorded comparison used two local source snapshots identified by hashes
in the supporting JSON. The command below recreates the fixture and measures
the checked-out implementation; it does not reconstruct the historical baseline.
Omit `--build-fixture` for subsequent runs on that fixture. The result cache
remains disabled by the runner.

```sh
uv run --group dev python scripts/benchmark_ana_seed_discovery.py \
  --database .benchmarks/ana-seed-discovery \
  --build-fixture --nodes 25000 --repeats 21 --http \
  --output .benchmarks/ana-seed-discovery.json
```

Real HTTP measurement requires permission to bind a loopback port. Omit
`--http` for native-only runs. Source hashes identify the compared engine
implementations; native probe counters and raw allocation values accompany
the timing samples.
