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
| grapharrow | incoming_typed_projection_order_limit | ok | 0.099141 | 0.098424 | 0.125461 | 500 | `48675a352b12` | `4ad80f4a70ce` |
| grapharrow | recommendation_common_neighbors_collect_count | ok | 0.183745 | 0.171588 | 0.187623 | 100 | `cbf7abeafb57` | `bbca70a4c911` |
| grapharrow | multi_match_where_graph_return | ok | 0.318166 | 0.316120 | 0.322591 | 100 | `706d8d405238` | `d135fe0f5b0b` |
| neo4j | incoming_typed_projection_order_limit | ok | 0.011883 | 0.010853 | 0.012554 | 500 | `48675a352b12` | `4ad80f4a70ce` |
| neo4j | recommendation_common_neighbors_collect_count | ok | 0.017149 | 0.015449 | 0.043414 | 100 | `cbf7abeafb57` | `bbca70a4c911` |
| neo4j | multi_match_where_graph_return | ok | 0.032646 | 0.030598 | 0.091628 | 100 | `b7ec9c0af165` | `d135fe0f5b0b` |
| memgraph | incoming_typed_projection_order_limit | ok | 0.012696 | 0.008325 | 0.020088 | 500 | `48675a352b12` | `4ad80f4a70ce` |
| memgraph | recommendation_common_neighbors_collect_count | ok | 0.049143 | 0.025379 | 0.056930 | 100 | `cbf7abeafb57` | `bbca70a4c911` |
| memgraph | multi_match_where_graph_return | ok | 0.008763 | 0.007908 | 0.009535 | 100 | `207db6aef9cc` | `d135fe0f5b0b` |

## Notes

- Memory is not measured for external engines in this benchmark.
- Timings include Bolt round trip, result pull, and Python driver materialization.
- Seed time is not included in query timings.
- Graph/value checksums are engine-local and are not expected to match across engines.
- Semantic checksums ignore engine-local graph IDs and compare a canonicalized unordered result multiset.
