# Cypher Engine Benchmark

This benchmark measures representative GraphArrow query paths on a synthetic embedded database.

## Configuration

- Nodes: `20000`
- Edges: `120000`
- Repeats: `2`
- Warmups: `1`
- Memory probe repeats: `1`
- Seed: `42`
- Planner enabled: `yes`

## Results

| Query | Median (s) | Peak RSS Δ (MB) | Arrow peak (MB) | Rows | Checksum |
| --- | ---: | ---: | ---: | ---: | --- |
| scalar_filter_sort_limit | 0.049551 | 44.16 | 19.19 | 100 | `d4dc7e6d50ab` |
| node_property_inline_point_lookup | 0.048853 | 44.59 | 18.65 | 1 | `7fee6e38e67d` |
| node_property_where_point_lookup | 0.051088 | 43.27 | 18.65 | 1 | `7fee6e38e67d` |
| text_prefix_filter_sort_limit | 0.050084 | 44.80 | 18.79 | 500 | `2e260ae3d911` |
| relationship_property_filter_sort_limit | 0.102611 | 67.62 | 32.46 | 500 | `9bdfe05eee31` |
| incoming_typed_projection_order_limit | 0.144942 | 61.30 | 25.15 | 500 | `4bf2f3ee3221` |
| grouped_with_count | 0.073456 | 42.44 | 17.44 | 6 | `aabcea513d55` |
| grouped_collect_neighbors | 0.142250 | 60.19 | 21.94 | 100 | `a47023e097d8` |
| with_collect_size | 0.121649 | 65.25 | 27.16 | 100 | `f979fe2c105b` |
| not_exists_candidate_filter | 0.350697 | 93.42 | 37.80 | 500 | `5aea3e8c1b9a` |
| relationship_type_alternatives_projection | 0.279546 | 73.08 | 25.67 | 500 | `e7eb7b11a64a` |
| relationship_type_text_filter_count | 0.303646 | 86.19 | 39.38 | 1 | `de464f53229a` |
| abs_arithmetic_similarity_filter | 0.219460 | 94.12 | 32.57 | 500 | `b04af9ff0ee3` |
| return_distinct_projection_order_limit | 0.051295 | 45.25 | 19.28 | 1000 | `4fcce5b373ef` |
| optional_match_projection | 0.101542 | 72.77 | 31.83 | 1000 | `df5f5e712a4c` |
| optional_match_return_distinct_projection | 0.115247 | 103.56 | 45.39 | 1000 | `f9b761a9ccd4` |
| three_hop_traversal | 0.269972 | 140.23 | 84.14 | 1 | `5044ac5edd52` |
| three_hop_projection_order_limit | 0.257825 | 158.02 | 92.18 | 500 | `13b336220104` |
| three_hop_traversal_dense_where | 0.198260 | 97.12 | 49.56 | 1 | `4af33d2d4579` |
| three_hop_traversal_mixed_where | 0.202998 | 100.69 | 50.56 | 1 | `08374871e9a3` |
| multi_match_with_where | 0.199593 | 96.47 | 49.56 | 1 | `4af33d2d4579` |
| recommendation_common_neighbors_collect_count | 0.653769 | 538.16 | 379.01 | 100 | `13e32011cf4d` |
| multi_match_where_graph_return | 0.324231 | 75.52 | 30.73 | 100 | `c68d3c5e2c7f` |
| three_hop_wildcard_projection_order_limit | 0.283973 | 162.88 | 103.94 | 500 | `ab32f4a0932e` |
| range_hop_typed_distinct_projection_order_limit | 0.143158 | 61.47 | 20.78 | 500 | `43d4fb81d079` |
| range_hop_wildcard_projection_order_limit | 0.285440 | 170.11 | 104.58 | 500 | `007f0d24faa5` |
| unwind_parameter_pipeline | 0.000771 | 2.75 | 0.03 | 500 | `dc79e2f7ed68` |

## Traversal Diagnostics

These counts measure representative intermediate traversal cardinalities so traversal latency and memory can be explained by row expansion.

| Diagnostic | Count | Seconds |
| --- | ---: | ---: |
| hop_1_anchor_knows | 3835 | 0.168835 |
| incoming_hop_1_anchor_knows | 3894 | 0.149218 |
| hop_2_anchor_knows | 22600 | 0.187171 |
| hop_3_anchor_knows | 122063 | 0.262398 |
| hop_3_dense_filters | 23769 | 0.210788 |
| hop_3_mixed_filters | 4409 | 0.212970 |
| wildcard_3_anchor | 196949 | 0.300227 |
| range_1_3_anchor | 231985 | 0.297929 |

## Notes

- The benchmark includes parse, compile, execution, and public result shaping.
- The database is generated once per configuration and reused across measured runs.
- Results should be compared on the same machine and similar thermal/load conditions.
