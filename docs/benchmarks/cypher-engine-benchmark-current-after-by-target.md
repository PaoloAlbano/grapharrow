# Cypher Engine Benchmark

This benchmark measures representative GraphArrow query paths on a synthetic embedded database.

## Configuration

- Nodes: `20000`
- Edges: `120000`
- Repeats: `5`
- Warmups: `1`
- Memory probe repeats: `0`
- Seed: `42`
- Planner enabled: `yes`

## Results

| Query | Median (s) | Peak RSS Δ (MB) | Arrow peak (MB) | Rows | Checksum |
| --- | ---: | ---: | ---: | ---: | --- |
| scalar_filter_sort_limit | 0.055107 | 0.00 | 0.00 | 100 | `d4dc7e6d50ab` |
| node_property_inline_point_lookup | 0.051480 | 0.00 | 0.00 | 1 | `7fee6e38e67d` |
| node_property_where_point_lookup | 0.052749 | 0.00 | 0.00 | 1 | `7fee6e38e67d` |
| text_prefix_filter_sort_limit | 0.051705 | 0.00 | 0.00 | 500 | `2e260ae3d911` |
| relationship_property_filter_sort_limit | 0.105992 | 0.00 | 0.00 | 500 | `9bdfe05eee31` |
| incoming_typed_projection_order_limit | 0.102536 | 0.00 | 0.00 | 500 | `4bf2f3ee3221` |
| grouped_with_count | 0.074087 | 0.00 | 0.00 | 6 | `aabcea513d55` |
| grouped_collect_neighbors | 0.154162 | 0.00 | 0.00 | 100 | `a47023e097d8` |
| with_collect_size | 0.131063 | 0.00 | 0.00 | 100 | `f979fe2c105b` |
| not_exists_candidate_filter | 0.376123 | 0.00 | 0.00 | 500 | `5aea3e8c1b9a` |
| relationship_type_alternatives_projection | 0.104264 | 0.00 | 0.00 | 500 | `e7eb7b11a64a` |
| relationship_type_text_filter_count | 0.311435 | 0.00 | 0.00 | 1 | `de464f53229a` |
| abs_arithmetic_similarity_filter | 0.183574 | 0.00 | 0.00 | 500 | `b04af9ff0ee3` |
| return_distinct_projection_order_limit | 0.053367 | 0.00 | 0.00 | 1000 | `4fcce5b373ef` |
| optional_match_projection | 0.104329 | 0.00 | 0.00 | 1000 | `df5f5e712a4c` |
| optional_match_return_distinct_projection | 0.120404 | 0.00 | 0.00 | 1000 | `f9b761a9ccd4` |
| three_hop_traversal | 0.275615 | 0.00 | 0.00 | 1 | `f3bb83e86d8d` |
| three_hop_projection_order_limit | 0.270974 | 0.00 | 0.00 | 500 | `13b336220104` |
| three_hop_traversal_dense_where | 0.203443 | 0.00 | 0.00 | 1 | `4af33d2d4579` |
| three_hop_traversal_mixed_where | 0.209819 | 0.00 | 0.00 | 1 | `08374871e9a3` |
| multi_match_with_where | 0.204083 | 0.00 | 0.00 | 1 | `4af33d2d4579` |
| recommendation_common_neighbors_collect_count | 0.192054 | 0.00 | 0.00 | 100 | `13e32011cf4d` |
| multi_match_where_graph_return | 0.334083 | 0.00 | 0.00 | 100 | `c95ef1e7e57b` |
| three_hop_wildcard_projection_order_limit | 0.299708 | 0.00 | 0.00 | 500 | `ab32f4a0932e` |
| range_hop_typed_distinct_projection_order_limit | 0.150017 | 0.00 | 0.00 | 500 | `43d4fb81d079` |
| range_hop_wildcard_projection_order_limit | 0.303974 | 0.00 | 0.00 | 500 | `007f0d24faa5` |
| unwind_parameter_pipeline | 0.000772 | 0.00 | 0.00 | 500 | `dc79e2f7ed68` |

## Traversal Diagnostics

These counts measure representative intermediate traversal cardinalities so traversal latency and memory can be explained by row expansion.

| Diagnostic | Count | Seconds |
| --- | ---: | ---: |
| hop_1_anchor_knows | 3835 | 0.179976 |
| incoming_hop_1_anchor_knows | 3894 | 0.122937 |
| hop_2_anchor_knows | 22600 | 0.214046 |
| hop_3_anchor_knows | 122062 | 0.311348 |
| hop_3_dense_filters | 23769 | 0.223961 |
| hop_3_mixed_filters | 4409 | 0.218227 |
| wildcard_3_anchor | 196949 | 0.344408 |
| range_1_3_anchor | 231985 | 0.334810 |

## Notes

- The benchmark includes parse, compile, execution, and public result shaping.
- The database is generated once per configuration and reused across measured runs.
- Results should be compared on the same machine and similar thermal/load conditions.
