# Cypher Engine Benchmark

This benchmark measures representative GraphArrow query paths on a synthetic embedded database.

## Configuration

- Nodes: `60000`
- Edges: `360000`
- Repeats: `3`
- Warmups: `1`
- Memory probe repeats: `0`
- Seed: `42`
- Planner enabled: `yes`

## Results

| Query | Median (s) | Peak RSS Δ (MB) | Arrow peak (MB) | Rows | Checksum |
| --- | ---: | ---: | ---: | ---: | --- |
| scalar_filter_sort_limit | 0.150160 | 0.00 | 0.00 | 100 | `cec953a63817` |
| node_property_inline_point_lookup | 0.144127 | 0.00 | 0.00 | 1 | `f856cda5d861` |
| node_property_where_point_lookup | 0.147706 | 0.00 | 0.00 | 1 | `f856cda5d861` |
| text_prefix_filter_sort_limit | 0.148001 | 0.00 | 0.00 | 500 | `2e260ae3d911` |
| relationship_property_filter_sort_limit | 0.307268 | 0.00 | 0.00 | 500 | `f7b7feb06424` |
| incoming_typed_projection_order_limit | 0.297668 | 0.00 | 0.00 | 500 | `3b728d3f1633` |
| grouped_with_count | 0.230945 | 0.00 | 0.00 | 6 | `9dbd4c8f032d` |
| grouped_collect_neighbors | 0.439861 | 0.00 | 0.00 | 100 | `ef11598622b6` |
| with_collect_size | 0.365146 | 0.00 | 0.00 | 100 | `748b83b6b4b6` |
| not_exists_candidate_filter | 0.733760 | 0.00 | 0.00 | 500 | `86566863d209` |
| relationship_type_alternatives_projection | 0.305168 | 0.00 | 0.00 | 500 | `ce8dbf279f1c` |
| relationship_type_text_filter_count | 0.700155 | 0.00 | 0.00 | 1 | `8ba173363375` |
| abs_arithmetic_similarity_filter | 0.411150 | 0.00 | 0.00 | 500 | `4751757d1494` |
| return_distinct_projection_order_limit | 0.157211 | 0.00 | 0.00 | 1000 | `f5b6d3be1210` |
| optional_match_projection | 0.293359 | 0.00 | 0.00 | 1000 | `6454be44c098` |
| optional_match_return_distinct_projection | 0.344583 | 0.00 | 0.00 | 1000 | `fc11517cbed2` |
| three_hop_traversal | 0.440880 | 0.00 | 0.00 | 1 | `76b3f5b354b1` |
| three_hop_projection_order_limit | 0.495162 | 0.00 | 0.00 | 500 | `5c325018ce0d` |
| three_hop_traversal_dense_where | 0.570350 | 0.00 | 0.00 | 1 | `b529a6a668ad` |
| three_hop_traversal_mixed_where | 0.571560 | 0.00 | 0.00 | 1 | `7ff56474fc65` |
| multi_match_with_where | 0.576490 | 0.00 | 0.00 | 1 | `b529a6a668ad` |
| recommendation_common_neighbors_collect_count | 0.510035 | 0.00 | 0.00 | 100 | `4cad99d3fe3a` |
| multi_match_where_graph_return | 0.923120 | 0.00 | 0.00 | 100 | `fdee08278456` |
| three_hop_wildcard_projection_order_limit | 0.436243 | 0.00 | 0.00 | 500 | `3a005e1f02e6` |
| range_hop_typed_distinct_projection_order_limit | 0.453015 | 0.00 | 0.00 | 500 | `43d4fb81d079` |
| range_hop_wildcard_projection_order_limit | 0.438435 | 0.00 | 0.00 | 500 | `6c03dfd1c63d` |
| unwind_parameter_pipeline | 0.000740 | 0.00 | 0.00 | 500 | `dc79e2f7ed68` |

## Traversal Diagnostics

These counts measure representative intermediate traversal cardinalities so traversal latency and memory can be explained by row expansion.

| Diagnostic | Count | Seconds |
| --- | ---: | ---: |
| hop_1_anchor_knows | 3817 | 0.301971 |
| incoming_hop_1_anchor_knows | 3825 | 0.235280 |
| hop_2_anchor_knows | 22593 | 0.353045 |
| hop_3_anchor_knows | 121089 | 0.445329 |
| hop_3_dense_filters | 22203 | 0.592631 |
| hop_3_mixed_filters | 2876 | 0.571301 |
| wildcard_3_anchor | 201720 | 0.379436 |
| range_1_3_anchor | 237546 | 0.360692 |

## Notes

- The benchmark includes parse, compile, execution, and public result shaping.
- The database is generated once per configuration and reused across measured runs.
- Results should be compared on the same machine and similar thermal/load conditions.
