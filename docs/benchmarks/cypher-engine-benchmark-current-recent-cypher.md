# Cypher Engine Benchmark

This benchmark measures representative GraphArrow query paths on a synthetic embedded database.

## Configuration

- Nodes: `20000`
- Edges: `120000`
- Repeats: `5`
- Warmups: `1`
- Memory probe repeats: `3`
- Seed: `42`
- Planner enabled: `yes`

## Results

| Query | Median (s) | Peak RSS Δ (MB) | Arrow peak (MB) | Rows | Checksum |
| --- | ---: | ---: | ---: | ---: | --- |
| scalar_filter_sort_limit | 0.051257 | 48.42 | 22.26 | 100 | `d4dc7e6d50ab` |
| relationship_property_filter_sort_limit | 0.110290 | 97.47 | 54.31 | 500 | `9bdfe05eee31` |
| incoming_typed_projection_order_limit | 0.144643 | 58.92 | 25.09 | 500 | `4bf2f3ee3221` |
| grouped_with_count | 0.069378 | 42.22 | 17.44 | 6 | `aabcea513d55` |
| grouped_collect_neighbors | 0.144359 | 56.98 | 21.87 | 100 | `a47023e097d8` |
| with_collect_size | 0.125693 | 65.28 | 27.10 | 100 | `f979fe2c105b` |
| not_exists_candidate_filter | 0.352687 | 91.34 | 35.55 | 500 | `5aea3e8c1b9a` |
| relationship_type_alternatives_projection | 0.279888 | 69.75 | 25.66 | 500 | `e7eb7b11a64a` |
| abs_arithmetic_similarity_filter | 0.218529 | 91.83 | 33.86 | 500 | `08b42b785ae8` |
| return_distinct_projection_order_limit | 0.053107 | 46.42 | 19.28 | 1000 | `4fcce5b373ef` |
| optional_match_projection | 0.105849 | 70.02 | 30.95 | 1000 | `df5f5e712a4c` |
| optional_match_return_distinct_projection | 0.127703 | 102.97 | 45.37 | 1000 | `f9b761a9ccd4` |
| three_hop_traversal | 0.263508 | 126.59 | 74.45 | 1 | `5044ac5edd52` |
| three_hop_projection_order_limit | 0.269293 | 144.02 | 83.80 | 500 | `13b336220104` |
| three_hop_traversal_dense_where | 0.211109 | 98.64 | 45.47 | 1 | `4af33d2d4579` |
| three_hop_traversal_mixed_where | 0.209029 | 102.94 | 46.84 | 1 | `08374871e9a3` |
| multi_match_with_where | 0.205144 | 103.33 | 45.47 | 1 | `4af33d2d4579` |
| three_hop_wildcard_projection_order_limit | 0.297488 | 131.70 | 78.83 | 500 | `ab32f4a0932e` |
| range_hop_wildcard_projection_order_limit | 0.291397 | 143.08 | 84.03 | 500 | `007f0d24faa5` |
| unwind_parameter_pipeline | 0.000831 | 3.36 | 0.03 | 500 | `dc79e2f7ed68` |

## Traversal Diagnostics

These counts measure representative intermediate traversal cardinalities so traversal latency and memory can be explained by row expansion.

| Diagnostic | Count | Seconds |
| --- | ---: | ---: |
| hop_1_anchor_knows | 3835 | 0.170685 |
| incoming_hop_1_anchor_knows | 3894 | 0.142950 |
| hop_2_anchor_knows | 22600 | 0.179700 |
| hop_3_anchor_knows | 122063 | 0.275324 |
| hop_3_dense_filters | 23769 | 0.215903 |
| hop_3_mixed_filters | 4409 | 0.202925 |
| wildcard_3_anchor | 196950 | 0.305449 |
| range_1_3_anchor | 231986 | 0.291298 |

## Notes

- The benchmark includes parse, compile, execution, and public result shaping.
- The database is generated once per configuration and reused across measured runs.
- Results should be compared on the same machine and similar thermal/load conditions.
