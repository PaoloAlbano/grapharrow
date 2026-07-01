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
| scalar_filter_sort_limit | 0.147152 | 0.00 | 0.00 | 100 | `cec953a63817` |
| node_property_inline_point_lookup | 0.141440 | 0.00 | 0.00 | 1 | `f856cda5d861` |
| node_property_where_point_lookup | 0.142346 | 0.00 | 0.00 | 1 | `f856cda5d861` |
| text_prefix_filter_sort_limit | 0.142237 | 0.00 | 0.00 | 500 | `2e260ae3d911` |
| relationship_property_filter_sort_limit | 0.302387 | 0.00 | 0.00 | 500 | `f7b7feb06424` |
| incoming_typed_projection_order_limit | 0.291615 | 0.00 | 0.00 | 500 | `3b728d3f1633` |
| grouped_with_count | 0.224273 | 0.00 | 0.00 | 6 | `9dbd4c8f032d` |
| grouped_collect_neighbors | 0.464530 | 0.00 | 0.00 | 100 | `ef11598622b6` |
| with_collect_size | 0.368988 | 0.00 | 0.00 | 100 | `748b83b6b4b6` |
| not_exists_candidate_filter | 0.740749 | 0.00 | 0.00 | 500 | `86566863d209` |
| relationship_type_alternatives_projection | 0.301793 | 0.00 | 0.00 | 500 | `ce8dbf279f1c` |
| relationship_type_text_filter_count | 0.705033 | 0.00 | 0.00 | 1 | `8ba173363375` |
| abs_arithmetic_similarity_filter | 0.393802 | 0.00 | 0.00 | 500 | `4751757d1494` |
| return_distinct_projection_order_limit | 0.152414 | 0.00 | 0.00 | 1000 | `f5b6d3be1210` |
| optional_match_projection | 0.298742 | 0.00 | 0.00 | 1000 | `6454be44c098` |
| optional_match_return_distinct_projection | 0.336663 | 0.00 | 0.00 | 1000 | `fc11517cbed2` |
| three_hop_traversal | 0.444034 | 0.00 | 0.00 | 1 | `76b3f5b354b1` |
| three_hop_projection_order_limit | 0.513274 | 0.00 | 0.00 | 500 | `5c325018ce0d` |
| three_hop_traversal_dense_where | 0.581570 | 0.00 | 0.00 | 1 | `b529a6a668ad` |
| three_hop_traversal_mixed_where | 0.581029 | 0.00 | 0.00 | 1 | `7ff56474fc65` |
| multi_match_with_where | 0.580580 | 0.00 | 0.00 | 1 | `b529a6a668ad` |
| recommendation_common_neighbors_collect_count | 0.511913 | 0.00 | 0.00 | 100 | `4cad99d3fe3a` |
| multi_match_where_graph_return | 0.918545 | 0.00 | 0.00 | 100 | `fdee08278456` |
| three_hop_wildcard_projection_order_limit | 0.426267 | 0.00 | 0.00 | 500 | `3a005e1f02e6` |
| range_hop_typed_distinct_projection_order_limit | 0.450066 | 0.00 | 0.00 | 500 | `43d4fb81d079` |
| range_hop_wildcard_projection_order_limit | 0.431368 | 0.00 | 0.00 | 500 | `6c03dfd1c63d` |
| unwind_parameter_pipeline | 0.000758 | 0.00 | 0.00 | 500 | `dc79e2f7ed68` |

## Traversal Diagnostics

These counts measure representative intermediate traversal cardinalities so traversal latency and memory can be explained by row expansion.

| Diagnostic | Count | Seconds |
| --- | ---: | ---: |
| hop_1_anchor_knows | 3817 | 0.298940 |
| incoming_hop_1_anchor_knows | 3825 | 0.246390 |
| hop_2_anchor_knows | 22593 | 0.344455 |
| hop_3_anchor_knows | 121089 | 0.431918 |
| hop_3_dense_filters | 22203 | 0.594881 |
| hop_3_mixed_filters | 2876 | 0.586685 |
| wildcard_3_anchor | 201720 | 0.374480 |
| range_1_3_anchor | 237546 | 0.354488 |

## Notes

- The benchmark includes parse, compile, execution, and public result shaping.
- The database is generated once per configuration and reused across measured runs.
- Results should be compared on the same machine and similar thermal/load conditions.
