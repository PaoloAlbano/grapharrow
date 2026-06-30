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
| scalar_filter_sort_limit | 0.053714 | 47.05 | 22.26 | 100 | `d4dc7e6d50ab` |
| text_prefix_filter_sort_limit | 0.052920 | 46.39 | 20.76 | 500 | `2e260ae3d911` |
| relationship_property_filter_sort_limit | 0.108759 | 66.42 | 32.46 | 500 | `9bdfe05eee31` |
| incoming_typed_projection_order_limit | 0.152563 | 60.95 | 25.15 | 500 | `4bf2f3ee3221` |
| grouped_with_count | 0.079031 | 38.34 | 17.44 | 6 | `aabcea513d55` |
| grouped_collect_neighbors | 0.144698 | 56.67 | 21.94 | 100 | `a47023e097d8` |
| with_collect_size | 0.129143 | 62.94 | 27.16 | 100 | `f979fe2c105b` |
| not_exists_candidate_filter | 0.372740 | 98.53 | 37.80 | 500 | `5aea3e8c1b9a` |
| relationship_type_alternatives_projection | 0.289190 | 69.48 | 25.67 | 500 | `e7eb7b11a64a` |
| relationship_type_text_filter_count | 0.322742 | 83.44 | 39.38 | 1 | `de464f53229a` |
| abs_arithmetic_similarity_filter | 0.232007 | 90.55 | 32.57 | 500 | `b04af9ff0ee3` |
| return_distinct_projection_order_limit | 0.056152 | 45.34 | 19.28 | 1000 | `4fcce5b373ef` |
| optional_match_projection | 0.107891 | 70.69 | 31.83 | 1000 | `df5f5e712a4c` |
| optional_match_return_distinct_projection | 0.122762 | 101.97 | 45.35 | 1000 | `f9b761a9ccd4` |
| three_hop_traversal | 0.288038 | 135.72 | 83.89 | 1 | `5044ac5edd52` |
| three_hop_projection_order_limit | 0.277620 | 153.42 | 92.99 | 500 | `13b336220104` |
| three_hop_traversal_dense_where | 0.211279 | 96.81 | 49.56 | 1 | `4af33d2d4579` |
| three_hop_traversal_mixed_where | 0.212059 | 97.94 | 50.93 | 1 | `08374871e9a3` |
| multi_match_with_where | 0.207209 | 94.77 | 49.56 | 1 | `4af33d2d4579` |
| recommendation_common_neighbors_collect_count | 0.693022 | 534.30 | 379.24 | 100 | `13e32011cf4d` |
| multi_match_where_graph_return | 0.334072 | 74.39 | 30.73 | 100 | `c68d3c5e2c7f` |
| three_hop_wildcard_projection_order_limit | 0.300526 | 156.78 | 103.19 | 500 | `ab32f4a0932e` |
| range_hop_typed_distinct_projection_order_limit | 0.150632 | 61.16 | 20.78 | 500 | `43d4fb81d079` |
| range_hop_wildcard_projection_order_limit | 0.300123 | 164.56 | 107.51 | 500 | `007f0d24faa5` |
| unwind_parameter_pipeline | 0.000930 | 2.72 | 0.03 | 500 | `dc79e2f7ed68` |

## Traversal Diagnostics

These counts measure representative intermediate traversal cardinalities so traversal latency and memory can be explained by row expansion.

| Diagnostic | Count | Seconds |
| --- | ---: | ---: |
| hop_1_anchor_knows | 3835 | 0.182389 |
| incoming_hop_1_anchor_knows | 3894 | 0.150683 |
| hop_2_anchor_knows | 22600 | 0.201032 |
| hop_3_anchor_knows | 122063 | 0.281121 |
| hop_3_dense_filters | 23769 | 0.222326 |
| hop_3_mixed_filters | 4409 | 0.211686 |
| wildcard_3_anchor | 196949 | 0.331145 |
| range_1_3_anchor | 231985 | 0.314607 |

## Notes

- The benchmark includes parse, compile, execution, and public result shaping.
- The database is generated once per configuration and reused across measured runs.
- Results should be compared on the same machine and similar thermal/load conditions.
