# Bolt Engine Comparison Benchmark

This benchmark measures end-to-end Bolt response time for the shared Cypher workload.

## Configuration

- Nodes: `20000`
- Edges: `120000`
- Repeats: `5`
- Warmups: `2`
- Seed: `42`
- Seed mode: `disabled`
- Lookup indexes during seed: `no`
- Engines: `grapharrow, neo4j, memgraph`

## Results

| Engine | Query | Status | Median (s) | Best (s) | Worst (s) | Rows | Checksum | Semantic |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| grapharrow | relationship_type_alternatives_projection | ok | 0.107237 | 0.105362 | 0.108250 | 500 | `dabcd70a5ec5` | `dabcd70a5ec5` |
| neo4j | relationship_type_alternatives_projection | ok | 0.022264 | 0.014087 | 0.048517 | 500 | `dabcd70a5ec5` | `dabcd70a5ec5` |
| memgraph | relationship_type_alternatives_projection | ok | 0.009762 | 0.009031 | 0.016891 | 500 | `dabcd70a5ec5` | `dabcd70a5ec5` |

## Notes

- Memory is not measured for external engines in this benchmark.
- Timings include Bolt round trip, result pull, and Python driver materialization.
- Seed time is not included in query timings.
- Graph/value checksums are engine-local and are not expected to match across engines.
- Semantic checksums ignore engine-local graph IDs and compare a canonicalized unordered result multiset.
