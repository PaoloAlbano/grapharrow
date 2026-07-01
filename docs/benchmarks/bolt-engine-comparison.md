# Bolt Engine Comparison Benchmark

This benchmark measures end-to-end Bolt response time for the shared Cypher workload.

## Configuration

- Nodes: `20000`
- Edges: `120000`
- Repeats: `3`
- Warmups: `1`
- Seed: `42`
- Lookup indexes during seed: `no`
- Engines: `grapharrow, neo4j, memgraph`

## Results

| Engine | Query | Status | Median (s) | Best (s) | Worst (s) | Rows | Checksum/Error |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| grapharrow | scalar_filter_sort_limit | ok | 0.054578 | 0.054253 | 0.055555 | 100 | `331f990adb55` |
| grapharrow | node_property_inline_point_lookup | ok | 0.050680 | 0.050664 | 0.050935 | 1 | `f4d33513fb3c` |
| grapharrow | node_property_where_point_lookup | ok | 0.051092 | 0.050365 | 0.052551 | 1 | `f4d33513fb3c` |
| grapharrow | text_prefix_filter_sort_limit | ok | 0.059383 | 0.059036 | 0.059586 | 500 | `42e12559de9c` |
| grapharrow | relationship_property_filter_sort_limit | ok | 0.113781 | 0.112489 | 0.113891 | 500 | `33d6713235fa` |
| grapharrow | incoming_typed_projection_order_limit | ok | 0.162914 | 0.152048 | 0.164915 | 500 | `48675a352b12` |
| grapharrow | grouped_with_count | ok | 0.074241 | 0.073731 | 0.086640 | 6 | `364d68bb6517` |
| grapharrow | grouped_collect_neighbors | ok | 0.156213 | 0.144735 | 0.159779 | 100 | `8b8104596b6c` |
| grapharrow | with_collect_size | ok | 0.126895 | 0.123193 | 0.136764 | 100 | `00b28edfdc75` |
| grapharrow | not_exists_candidate_filter | ok | 0.398987 | 0.382557 | 0.399616 | 500 | `be929f9548ae` |
| grapharrow | relationship_type_alternatives_projection | ok | 0.310036 | 0.305851 | 0.314303 | 500 | `dabcd70a5ec5` |
| grapharrow | relationship_type_text_filter_count | ok | 0.327346 | 0.325214 | 0.327680 | 1 | `8b2023455675` |
| grapharrow | abs_arithmetic_similarity_filter | ok | 0.241871 | 0.241036 | 0.248539 | 500 | `2fd6734abcc7` |
| grapharrow | return_distinct_projection_order_limit | ok | 0.067632 | 0.067432 | 0.067672 | 1000 | `44b18c2ca180` |
| grapharrow | optional_match_projection | ok | 0.120301 | 0.120256 | 0.120376 | 1000 | `bff2e16e0f7c` |
| grapharrow | optional_match_return_distinct_projection | ok | 0.134922 | 0.133490 | 0.147292 | 1000 | `7980e545eee1` |
| grapharrow | three_hop_traversal | ok | 0.276951 | 0.273957 | 0.294801 | 1 | `34799a497b7d` |
| grapharrow | three_hop_projection_order_limit | ok | 0.275557 | 0.267485 | 0.282629 | 500 | `e5812f65381a` |
| grapharrow | three_hop_traversal_dense_where | ok | 0.201604 | 0.200824 | 0.203113 | 1 | `ea3ebbff2f82` |
| grapharrow | three_hop_traversal_mixed_where | ok | 0.213434 | 0.212343 | 0.215596 | 1 | `e4a06f2e6963` |
| grapharrow | multi_match_with_where | ok | 0.201581 | 0.201468 | 0.203582 | 1 | `ea3ebbff2f82` |
| grapharrow | recommendation_common_neighbors_collect_count | ok | 0.691474 | 0.690089 | 0.738058 | 100 | `cbf7abeafb57` |
| grapharrow | multi_match_where_graph_return | ok | 0.342539 | 0.332894 | 0.351932 | 100 | `cd41f103ad98` |
| grapharrow | three_hop_wildcard_projection_order_limit | ok | 0.298157 | 0.298108 | 0.299838 | 500 | `3f5859ef87a4` |
| grapharrow | range_hop_typed_distinct_projection_order_limit | ok | 0.154674 | 0.154039 | 0.169201 | 500 | `54accf374166` |
| grapharrow | range_hop_wildcard_projection_order_limit | ok | 0.296369 | 0.295302 | 0.311138 | 500 | `df2dc7b52f0e` |
| grapharrow | unwind_parameter_pipeline | error |  |  |  |  | {neo4j_code: GraphArrow.QueryError} {message: Missing parameter: $values} {gql_status: 50N42} {gql_status_description: e |
| neo4j | scalar_filter_sort_limit | ok | 0.014315 | 0.010147 | 0.033688 | 100 | `331f990adb55` |
| neo4j | node_property_inline_point_lookup | ok | 0.005385 | 0.005207 | 0.007187 | 1 | `f4d33513fb3c` |
| neo4j | node_property_where_point_lookup | ok | 0.005046 | 0.004892 | 0.005250 | 1 | `f4d33513fb3c` |
| neo4j | text_prefix_filter_sort_limit | ok | 0.009227 | 0.009092 | 0.009549 | 500 | `42e12559de9c` |
| neo4j | relationship_property_filter_sort_limit | ok | 0.049481 | 0.048008 | 0.051364 | 500 | `33d6713235fa` |
| neo4j | incoming_typed_projection_order_limit | ok | 0.011091 | 0.010648 | 0.012786 | 500 | `48675a352b12` |
| neo4j | grouped_with_count | ok | 0.004880 | 0.004495 | 0.006552 | 6 | `364d68bb6517` |
| neo4j | grouped_collect_neighbors | ok | 0.006855 | 0.006721 | 0.006938 | 100 | `051eaab67a5c` |
| neo4j | with_collect_size | ok | 0.006152 | 0.005880 | 0.006840 | 100 | `00b28edfdc75` |
| neo4j | not_exists_candidate_filter | ok | 0.029461 | 0.029312 | 0.031298 | 500 | `be929f9548ae` |
| neo4j | relationship_type_alternatives_projection | ok | 0.011389 | 0.010938 | 0.012253 | 500 | `dabcd70a5ec5` |
| neo4j | relationship_type_text_filter_count | ok | 0.025692 | 0.024830 | 0.025916 | 1 | `8b2023455675` |
| neo4j | abs_arithmetic_similarity_filter | ok | 0.019812 | 0.018673 | 0.020877 | 500 | `5cf372eb9a25` |
| neo4j | return_distinct_projection_order_limit | ok | 0.015213 | 0.014420 | 0.015493 | 1000 | `44b18c2ca180` |
| neo4j | optional_match_projection | ok | 0.015223 | 0.014870 | 0.015247 | 1000 | `80c63ec86e21` |
| neo4j | optional_match_return_distinct_projection | ok | 0.015467 | 0.014863 | 0.019347 | 1000 | `7980e545eee1` |
| neo4j | three_hop_traversal | ok | 0.030327 | 0.029493 | 0.039125 | 1 | `e180931f8c60` |
| neo4j | three_hop_projection_order_limit | ok | 0.071411 | 0.066926 | 0.075432 | 500 | `e5812f65381a` |
| neo4j | three_hop_traversal_dense_where | ok | 0.042936 | 0.042644 | 0.043197 | 1 | `ea3ebbff2f82` |
| neo4j | three_hop_traversal_mixed_where | ok | 0.046130 | 0.045718 | 0.049255 | 1 | `e4a06f2e6963` |
| neo4j | multi_match_with_where | ok | 0.045978 | 0.045557 | 0.050653 | 1 | `ea3ebbff2f82` |
| neo4j | recommendation_common_neighbors_collect_count | ok | 0.019412 | 0.018897 | 0.020219 | 100 | `cbf7abeafb57` |
| neo4j | multi_match_where_graph_return | ok | 0.005425 | 0.004673 | 0.006094 | 100 | `21b82aef80dd` |
| neo4j | three_hop_wildcard_projection_order_limit | ok | 0.103316 | 0.097003 | 0.104935 | 500 | `3f5859ef87a4` |
| neo4j | range_hop_typed_distinct_projection_order_limit | ok | 0.594259 | 0.588888 | 0.623094 | 500 | `54accf374166` |
| neo4j | range_hop_wildcard_projection_order_limit | ok | 0.132634 | 0.118043 | 0.135664 | 500 | `df2dc7b52f0e` |
| neo4j | unwind_parameter_pipeline | error |  |  |  |  | {neo4j_code: Neo.ClientError.Statement.ParameterMissing} {message: Expected parameter(s): values} {gql_status: 42001} {g |
| memgraph | scalar_filter_sort_limit | ok | 0.006837 | 0.006303 | 0.007065 | 100 | `331f990adb55` |
| memgraph | node_property_inline_point_lookup | ok | 0.002687 | 0.002353 | 0.003132 | 1 | `f4d33513fb3c` |
| memgraph | node_property_where_point_lookup | ok | 0.002176 | 0.002138 | 0.003122 | 1 | `f4d33513fb3c` |
| memgraph | text_prefix_filter_sort_limit | ok | 0.006971 | 0.006718 | 0.007561 | 500 | `42e12559de9c` |
| memgraph | relationship_property_filter_sort_limit | ok | 0.033971 | 0.033664 | 0.034429 | 500 | `33d6713235fa` |
| memgraph | incoming_typed_projection_order_limit | ok | 0.008793 | 0.008560 | 0.009106 | 500 | `48675a352b12` |
| memgraph | grouped_with_count | ok | 0.002863 | 0.002691 | 0.003110 | 6 | `364d68bb6517` |
| memgraph | grouped_collect_neighbors | ok | 0.005469 | 0.005121 | 0.007546 | 100 | `c1f58099b26b` |
| memgraph | with_collect_size | ok | 0.004682 | 0.004229 | 0.004684 | 100 | `00b28edfdc75` |
| memgraph | not_exists_candidate_filter | ok | 0.022011 | 0.021468 | 0.023617 | 500 | `be929f9548ae` |
| memgraph | relationship_type_alternatives_projection | ok | 0.008700 | 0.007786 | 0.009244 | 500 | `dabcd70a5ec5` |
| memgraph | relationship_type_text_filter_count | ok | 0.019349 | 0.019203 | 0.020685 | 1 | `8b2023455675` |
| memgraph | abs_arithmetic_similarity_filter | ok | 0.014177 | 0.013797 | 0.014563 | 500 | `419510743dc4` |
| memgraph | return_distinct_projection_order_limit | ok | 1.134825 | 1.123441 | 1.135209 | 1000 | `44b18c2ca180` |
| memgraph | optional_match_projection | ok | 0.078424 | 0.075551 | 0.105830 | 1000 | `df0db3697240` |
| memgraph | optional_match_return_distinct_projection | ok | 0.292154 | 0.249413 | 0.330469 | 1000 | `7980e545eee1` |
| memgraph | three_hop_traversal | ok | 0.012998 | 0.012843 | 0.013631 | 1 | `e180931f8c60` |
| memgraph | three_hop_projection_order_limit | ok | 0.193526 | 0.089211 | 0.197075 | 500 | `e5812f65381a` |
| memgraph | three_hop_traversal_dense_where | ok | 0.012198 | 0.011923 | 0.012223 | 1 | `ea3ebbff2f82` |
| memgraph | three_hop_traversal_mixed_where | ok | 0.013626 | 0.013516 | 0.014977 | 1 | `e4a06f2e6963` |
| memgraph | multi_match_with_where | ok | 0.012375 | 0.012108 | 0.015744 | 1 | `ea3ebbff2f82` |
| memgraph | recommendation_common_neighbors_collect_count | ok | 0.022017 | 0.021501 | 0.023436 | 100 | `cbf7abeafb57` |
| memgraph | multi_match_where_graph_return | ok | 0.005370 | 0.005135 | 0.006556 | 100 | `13535f52e42b` |
| memgraph | three_hop_wildcard_projection_order_limit | ok | 0.193272 | 0.135611 | 0.195244 | 500 | `3f5859ef87a4` |
| memgraph | range_hop_typed_distinct_projection_order_limit | ok | 0.921562 | 0.920295 | 0.956459 | 500 | `54accf374166` |
| memgraph | range_hop_wildcard_projection_order_limit | ok | 0.157638 | 0.156031 | 0.176726 | 500 | `df2dc7b52f0e` |
| memgraph | unwind_parameter_pipeline | error |  |  |  |  | {neo4j_code: Memgraph.ClientError.MemgraphError.MemgraphError} {message: Parameter $values not provided.} {gql_status: 5 |

## Notes

- Memory is not measured for external engines in this benchmark.
- Timings include Bolt round trip, result pull, and Python driver materialization.
- Seed time is not included in query timings.
- Graph/value checksums are engine-local and are not expected to match across engines.
