# Bolt Engine Comparison Benchmark

This benchmark measures end-to-end Bolt response time for the shared Cypher workload.

## Configuration

- Nodes: `20000`
- Edges: `120000`
- Repeats: `3`
- Warmups: `1`
- Seed: `42`
- Seed mode: `disabled`
- Lookup indexes during seed: `no`
- Engines: `grapharrow, neo4j, memgraph`

## Results

| Engine | Query | Status | Median (s) | Best (s) | Worst (s) | Rows | Checksum | Semantic |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| grapharrow | scalar_filter_sort_limit | ok | 0.051995 | 0.050602 | 0.052423 | 100 | `331f990adb55` | `79c3141a1fde` |
| grapharrow | node_property_inline_point_lookup | ok | 0.051667 | 0.050996 | 0.052467 | 1 | `f4d33513fb3c` | `f4d33513fb3c` |
| grapharrow | node_property_where_point_lookup | ok | 0.048764 | 0.048710 | 0.048969 | 1 | `f4d33513fb3c` | `f4d33513fb3c` |
| grapharrow | text_prefix_filter_sort_limit | ok | 0.051643 | 0.051603 | 0.064702 | 500 | `42e12559de9c` | `42e12559de9c` |
| grapharrow | relationship_property_filter_sort_limit | ok | 0.104935 | 0.104934 | 0.107800 | 500 | `33d6713235fa` | `f081f30fde79` |
| grapharrow | incoming_typed_projection_order_limit | ok | 0.106546 | 0.099695 | 0.109119 | 500 | `48675a352b12` | `4ad80f4a70ce` |
| grapharrow | grouped_with_count | ok | 0.070671 | 0.070376 | 0.079316 | 6 | `364d68bb6517` | `14e14c1448da` |
| grapharrow | grouped_collect_neighbors | ok | 0.146370 | 0.146245 | 0.149508 | 100 | `8b8104596b6c` | `85d11094341d` |
| grapharrow | with_collect_size | ok | 0.125962 | 0.118705 | 0.126518 | 100 | `00b28edfdc75` | `5989a8bd81d7` |
| grapharrow | not_exists_candidate_filter | ok | 0.361400 | 0.358995 | 0.362761 | 500 | `be929f9548ae` | `be929f9548ae` |
| grapharrow | relationship_type_alternatives_projection | ok | 0.285928 | 0.277375 | 0.302065 | 500 | `dabcd70a5ec5` | `dabcd70a5ec5` |
| grapharrow | relationship_type_text_filter_count | ok | 0.310719 | 0.302564 | 0.333341 | 1 | `8b2023455675` | `8b2023455675` |
| grapharrow | abs_arithmetic_similarity_filter | ok | 0.182827 | 0.174496 | 0.187191 | 500 | `2fd6734abcc7` | `42964aabcb10` |
| grapharrow | return_distinct_projection_order_limit | ok | 0.056791 | 0.056576 | 0.057045 | 1000 | `44b18c2ca180` | `a1df05fefd6c` |
| grapharrow | optional_match_projection | ok | 0.108505 | 0.108174 | 0.108837 | 1000 | `bff2e16e0f7c` | `7933ae5b98bf` |
| grapharrow | optional_match_return_distinct_projection | ok | 0.121154 | 0.119547 | 0.128398 | 1000 | `7980e545eee1` | `7980e545eee1` |
| grapharrow | three_hop_traversal | ok | 0.281210 | 0.274954 | 0.284018 | 1 | `e180931f8c60` | `e180931f8c60` |
| grapharrow | three_hop_projection_order_limit | ok | 0.272680 | 0.271274 | 0.288766 | 500 | `e5812f65381a` | `e5812f65381a` |
| grapharrow | three_hop_traversal_dense_where | ok | 0.199539 | 0.195506 | 0.218276 | 1 | `ea3ebbff2f82` | `ea3ebbff2f82` |
| grapharrow | three_hop_traversal_mixed_where | ok | 0.196023 | 0.195520 | 0.196477 | 1 | `e4a06f2e6963` | `e4a06f2e6963` |
| grapharrow | multi_match_with_where | ok | 0.196203 | 0.195951 | 0.204842 | 1 | `ea3ebbff2f82` | `ea3ebbff2f82` |
| grapharrow | recommendation_common_neighbors_collect_count | ok | 0.188659 | 0.176600 | 0.191623 | 100 | `cbf7abeafb57` | `bbca70a4c911` |
| grapharrow | multi_match_where_graph_return | ok | 0.317821 | 0.315343 | 0.326720 | 100 | `706d8d405238` | `d135fe0f5b0b` |
| grapharrow | three_hop_wildcard_projection_order_limit | ok | 0.284769 | 0.284268 | 0.291722 | 500 | `3f5859ef87a4` | `3f5859ef87a4` |
| grapharrow | range_hop_typed_distinct_projection_order_limit | ok | 0.145899 | 0.145074 | 0.154525 | 500 | `54accf374166` | `54accf374166` |
| grapharrow | range_hop_wildcard_projection_order_limit | ok | 0.289135 | 0.287253 | 0.289406 | 500 | `df2dc7b52f0e` | `df2dc7b52f0e` |
| grapharrow | unwind_parameter_pipeline | ok | 0.004453 | 0.004452 | 0.004546 | 500 | `0d3d17a70401` | `0d3d17a70401` |
| neo4j | scalar_filter_sort_limit | ok | 0.011118 | 0.010343 | 0.011419 | 100 | `331f990adb55` | `79c3141a1fde` |
| neo4j | node_property_inline_point_lookup | ok | 0.006300 | 0.005612 | 0.007094 | 1 | `f4d33513fb3c` | `f4d33513fb3c` |
| neo4j | node_property_where_point_lookup | ok | 0.006998 | 0.006354 | 0.007728 | 1 | `f4d33513fb3c` | `f4d33513fb3c` |
| neo4j | text_prefix_filter_sort_limit | ok | 0.012473 | 0.011751 | 0.012616 | 500 | `42e12559de9c` | `42e12559de9c` |
| neo4j | relationship_property_filter_sort_limit | ok | 0.049031 | 0.044747 | 0.070125 | 500 | `33d6713235fa` | `f081f30fde79` |
| neo4j | incoming_typed_projection_order_limit | ok | 0.011820 | 0.011556 | 0.011913 | 500 | `48675a352b12` | `4ad80f4a70ce` |
| neo4j | grouped_with_count | ok | 0.004992 | 0.004086 | 0.005859 | 6 | `364d68bb6517` | `14e14c1448da` |
| neo4j | grouped_collect_neighbors | ok | 0.006272 | 0.005901 | 0.006334 | 100 | `051eaab67a5c` | `85d11094341d` |
| neo4j | with_collect_size | ok | 0.005528 | 0.005399 | 0.005607 | 100 | `00b28edfdc75` | `5989a8bd81d7` |
| neo4j | not_exists_candidate_filter | ok | 0.039208 | 0.027852 | 0.039791 | 500 | `be929f9548ae` | `be929f9548ae` |
| neo4j | relationship_type_alternatives_projection | ok | 0.011584 | 0.010728 | 0.011702 | 500 | `dabcd70a5ec5` | `dabcd70a5ec5` |
| neo4j | relationship_type_text_filter_count | ok | 0.024071 | 0.023872 | 0.024139 | 1 | `8b2023455675` | `8b2023455675` |
| neo4j | abs_arithmetic_similarity_filter | ok | 0.018180 | 0.017963 | 0.018744 | 500 | `5cf372eb9a25` | `42964aabcb10` |
| neo4j | return_distinct_projection_order_limit | ok | 0.014255 | 0.013685 | 0.015414 | 1000 | `44b18c2ca180` | `a1df05fefd6c` |
| neo4j | optional_match_projection | ok | 0.017157 | 0.017142 | 0.018421 | 1000 | `80c63ec86e21` | `7933ae5b98bf` |
| neo4j | optional_match_return_distinct_projection | ok | 0.020954 | 0.014338 | 0.035746 | 1000 | `7980e545eee1` | `7980e545eee1` |
| neo4j | three_hop_traversal | ok | 0.029865 | 0.028966 | 0.029879 | 1 | `e180931f8c60` | `e180931f8c60` |
| neo4j | three_hop_projection_order_limit | ok | 0.072044 | 0.068750 | 0.074205 | 500 | `e5812f65381a` | `e5812f65381a` |
| neo4j | three_hop_traversal_dense_where | ok | 0.041469 | 0.041399 | 0.052137 | 1 | `ea3ebbff2f82` | `ea3ebbff2f82` |
| neo4j | three_hop_traversal_mixed_where | ok | 0.044442 | 0.044210 | 0.048606 | 1 | `e4a06f2e6963` | `e4a06f2e6963` |
| neo4j | multi_match_with_where | ok | 0.042260 | 0.040975 | 0.054808 | 1 | `ea3ebbff2f82` | `ea3ebbff2f82` |
| neo4j | recommendation_common_neighbors_collect_count | ok | 0.014765 | 0.014433 | 0.014915 | 100 | `cbf7abeafb57` | `bbca70a4c911` |
| neo4j | multi_match_where_graph_return | ok | 0.029364 | 0.029145 | 0.032531 | 100 | `b7ec9c0af165` | `d135fe0f5b0b` |
| neo4j | three_hop_wildcard_projection_order_limit | ok | 0.100828 | 0.098723 | 0.115001 | 500 | `3f5859ef87a4` | `3f5859ef87a4` |
| neo4j | range_hop_typed_distinct_projection_order_limit | ok | 0.569700 | 0.566089 | 0.584483 | 500 | `54accf374166` | `54accf374166` |
| neo4j | range_hop_wildcard_projection_order_limit | ok | 0.116933 | 0.116492 | 0.132460 | 500 | `df2dc7b52f0e` | `df2dc7b52f0e` |
| neo4j | unwind_parameter_pipeline | ok | 0.006221 | 0.005595 | 0.007533 | 500 | `0d3d17a70401` | `0d3d17a70401` |
| memgraph | scalar_filter_sort_limit | ok | 0.006054 | 0.006047 | 0.006144 | 100 | `331f990adb55` | `79c3141a1fde` |
| memgraph | node_property_inline_point_lookup | ok | 0.002025 | 0.002005 | 0.002036 | 1 | `f4d33513fb3c` | `f4d33513fb3c` |
| memgraph | node_property_where_point_lookup | ok | 0.002053 | 0.002049 | 0.002061 | 1 | `f4d33513fb3c` | `f4d33513fb3c` |
| memgraph | text_prefix_filter_sort_limit | ok | 0.006633 | 0.006148 | 0.006733 | 500 | `42e12559de9c` | `42e12559de9c` |
| memgraph | relationship_property_filter_sort_limit | ok | 0.031682 | 0.029369 | 0.031847 | 500 | `33d6713235fa` | `f081f30fde79` |
| memgraph | incoming_typed_projection_order_limit | ok | 0.008659 | 0.007182 | 0.008987 | 500 | `48675a352b12` | `4ad80f4a70ce` |
| memgraph | grouped_with_count | ok | 0.002694 | 0.002668 | 0.002864 | 6 | `364d68bb6517` | `14e14c1448da` |
| memgraph | grouped_collect_neighbors | ok | 0.004709 | 0.004526 | 0.004711 | 100 | `c1f58099b26b` | `85d11094341d` |
| memgraph | with_collect_size | ok | 0.003941 | 0.003901 | 0.004120 | 100 | `00b28edfdc75` | `5989a8bd81d7` |
| memgraph | not_exists_candidate_filter | ok | 0.019587 | 0.018804 | 0.021243 | 500 | `be929f9548ae` | `be929f9548ae` |
| memgraph | relationship_type_alternatives_projection | ok | 0.008355 | 0.007708 | 0.009296 | 500 | `dabcd70a5ec5` | `dabcd70a5ec5` |
| memgraph | relationship_type_text_filter_count | ok | 0.018565 | 0.018415 | 0.018697 | 1 | `8b2023455675` | `8b2023455675` |
| memgraph | abs_arithmetic_similarity_filter | ok | 0.013780 | 0.013293 | 0.014785 | 500 | `419510743dc4` | `42964aabcb10` |
| memgraph | return_distinct_projection_order_limit | ok | 1.112260 | 1.107721 | 1.126931 | 1000 | `44b18c2ca180` | `a1df05fefd6c` |
| memgraph | optional_match_projection | ok | 0.072641 | 0.072440 | 0.074694 | 1000 | `df0db3697240` | `7933ae5b98bf` |
| memgraph | optional_match_return_distinct_projection | ok | 0.127596 | 0.111939 | 0.145009 | 1000 | `7980e545eee1` | `7980e545eee1` |
| memgraph | three_hop_traversal | ok | 0.012361 | 0.012317 | 0.012908 | 1 | `e180931f8c60` | `e180931f8c60` |
| memgraph | three_hop_projection_order_limit | ok | 0.091290 | 0.082198 | 0.095666 | 500 | `e5812f65381a` | `e5812f65381a` |
| memgraph | three_hop_traversal_dense_where | ok | 0.011416 | 0.011214 | 0.011670 | 1 | `ea3ebbff2f82` | `ea3ebbff2f82` |
| memgraph | three_hop_traversal_mixed_where | ok | 0.013455 | 0.013448 | 0.013487 | 1 | `e4a06f2e6963` | `e4a06f2e6963` |
| memgraph | multi_match_with_where | ok | 0.011194 | 0.011100 | 0.011290 | 1 | `ea3ebbff2f82` | `ea3ebbff2f82` |
| memgraph | recommendation_common_neighbors_collect_count | ok | 0.018758 | 0.018750 | 0.018918 | 100 | `cbf7abeafb57` | `bbca70a4c911` |
| memgraph | multi_match_where_graph_return | ok | 0.008646 | 0.008119 | 0.008804 | 100 | `207db6aef9cc` | `d135fe0f5b0b` |
| memgraph | three_hop_wildcard_projection_order_limit | ok | 0.141025 | 0.126848 | 0.142763 | 500 | `3f5859ef87a4` | `3f5859ef87a4` |
| memgraph | range_hop_typed_distinct_projection_order_limit | ok | 0.889021 | 0.880137 | 0.916637 | 500 | `54accf374166` | `54accf374166` |
| memgraph | range_hop_wildcard_projection_order_limit | ok | 0.152460 | 0.149451 | 0.156590 | 500 | `df2dc7b52f0e` | `df2dc7b52f0e` |
| memgraph | unwind_parameter_pipeline | ok | 0.005306 | 0.004462 | 0.005447 | 500 | `0d3d17a70401` | `0d3d17a70401` |

## Notes

- Memory is not measured for external engines in this benchmark.
- Timings include Bolt round trip, result pull, and Python driver materialization.
- Seed time is not included in query timings.
- Graph/value checksums are engine-local and are not expected to match across engines.
- Semantic checksums ignore engine-local graph IDs and compare a canonicalized unordered result multiset.
