# Streaming ANA Pattern Bindings

Measured on November 7, 2026, with Python 3.12.11 and PyArrow 23.0.1 on
macOS arm64. The baseline includes the
[limited-expand optimizations](ana-traversal-stress.md).

## Execution changes

Pattern hops now consume batches of at most 256 input bindings and yield
matching rows progressively. Filters apply as their variables become bound.
The finalizer projects and deduplicates rows as they arrive. For unordered,
ungrouped output with a limit, it retains the requested result rows.

An additional path handles limited entity output when there is no projection,
grouping, aggregation, ordering, or pending filter at the final hop. It
materializes the requested entities and collects the complete matching node-ID
set separately. Entity deduplication still happens before the limit when
requested. The executor continues scanning candidates to preserve full branch
membership for compatible union counts.

Ordered results retain the original bindings needed to evaluate sort keys.
Aggregations consume the complete matching input, and the current aggregator
retains group members. Initial seed rows, distinct keys, complete branch IDs,
and Arrow candidate tables also consume memory. Streaming the hops therefore
reduces intermediate retention without imposing a hard peak-RSS limit.

## Method

The [traversal runner](../../scripts/benchmark_ana_traversals.py) creates
independent native workflows in a fresh worker process for each configuration.
The workflows share a byte budget and synchronize each round at a barrier.
After two warmups, each workflow performs 21 timed calls. The p95 uses the
nearest rank of 21 or 84 samples. Session setup, fetch, verification, and
barrier waiting are outside the timer.

The parent samples RSS approximately every 16–17 ms. Reported growth is the
highest sampled RSS minus RSS after sessions and seed handles are prepared.
Warmup calls and all returned handles remain in the measured worker until
workflow shutdown. Sampling can miss shorter peaks. These are measurements
of native thread concurrency on a local machine.

The one-hop fixture contains eight hubs with 20,000 outgoing relationships
each, 10,000 targets, and 250 low-degree seeds. Each hub has two parallel edges
to each target. The initial experiment included a low-degree expand control.
A separate repeat of the hub case checks whether the main result persists.

The two-hop fixture uses a hub degree of 1,000 and 500 targets. Its pattern
starts from one hub, follows `LINK` to a target, then follows incoming `LINK`
back to a hub under a different variable. Each initial edge has 15 eligible
return edges after relationship reuse is excluded, producing 15,000 bindings
and eight distinct final hubs. The result limit is 10.

## Results

The table uses the repeated one-hop run and the two-hop experiment. Cells
show baseline and streaming values. RSS growth is measured for the entire
worker process.

| Pattern | Workflows | Median (ms) | p95 (ms) | Sampled RSS growth (MiB) |
| --- | ---: | ---: | ---: | ---: |
| One hop, 20k degree, limit 10 | 1 | 237.80 → 29.29 | 258.83 → 30.95 | 53.8 → 14.1 |
| One hop, 20k degree, limit 10 | 4 | 962.59 → 86.87 | 1121.52 → 120.90 | 204.8 → 93.4 |
| Two hops, 1k degree, limit 10 | 1 | 168.96 → 40.21 | 179.62 → 49.71 | 34.9 → 3.6 |
| Two hops, 1k degree, limit 10 | 4 | 746.71 → 217.21 | 819.99 → 243.71 | 130.9 → 11.9 |

In the initial one-hop comparison, four workflows measured p95 values of
1543.24 and 121.22 ms, with RSS growth of 237.4 and 95.2 MiB. The unchanged
low-degree expand control also had a slower baseline run, indicating variation
between measurements. Its median with four workflows was 77.92 and 48.81 ms.
The repeated hub measurement continues to show the latency and memory
reduction. The sample counts and local environment do not establish a
production latency bound.

## Correctness

Fetched result digests match in every compared configuration. Each call
checks row count, ownership of entity references, and complete branch
membership. For the one-hop result, a 10-row handle still carries 10,000
distinct matching IDs for compatible union counts. The shared byte budget
returns to zero when all workflows close.

Regression cases cross binding-batch boundaries and exercise parallel edges,
entity deduplication before limits, late filters, filters on intermediate
nodes, sort keys outside the projection, full grouped aggregates, rebound
node variables, relationship uniqueness, and invalid later hops with empty
input. A limited entity case also verifies that only the requested target
payloads are materialized while its union count covers the entire branch.

## Reproduction

Run from the repository root with development dependencies available.
Use a fresh database path when creating each fixture. Reuse the same database
for comparisons between implementations.

```sh
uv run --group dev python scripts/benchmark_ana_traversals.py \
  --database .benchmarks/ana-pattern-hubs \
  --build-fixture --degree 20000 \
  --cases hub_pattern_limit_10 low_degree_250 \
  --sessions 1 4 --warmups 2 --repeats 21 \
  --output .benchmarks/ana-pattern-hubs.json

uv run --group dev python scripts/benchmark_ana_traversals.py \
  --database .benchmarks/ana-pattern-two-hop \
  --build-fixture --degree 1000 \
  --cases hub_two_hop_limit_10 \
  --sessions 1 4 --warmups 2 --repeats 21 \
  --output .benchmarks/ana-pattern-two-hop.json
```

Omit `--build-fixture` when reusing a fixture. The sampler supports macOS and
Linux and needs permission to inspect worker RSS.

[Supporting measurements](ana-pattern-streaming.json)
