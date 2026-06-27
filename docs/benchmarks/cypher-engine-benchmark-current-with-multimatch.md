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
| scalar_filter_sort_limit | 0.052785 | 47.67 | 22.21 | 100 | `d4dc7e6d50ab` |
| relationship_property_filter_sort_limit | 0.114355 | 107.75 | 62.77 | 500 | `273782367980` |
| grouped_with_count | 0.070628 | 42.75 | 17.38 | 6 | `aabcea513d55` |
| return_distinct_projection_order_limit | 0.053237 | 45.56 | 19.23 | 1000 | `4fcce5b373ef` |
| optional_match_projection | 0.105636 | 73.52 | 31.68 | 1000 | `47f952d1f251` |
| optional_match_return_distinct_projection | 0.118821 | 99.09 | 53.03 | 774 | `45cbbdcdc710` |
| three_hop_traversal | 0.270542 | 168.00 | 117.11 | 1 | `4f18d6945f14` |
| three_hop_projection_order_limit | 0.278540 | 191.89 | 130.33 | 500 | `ab32f4a0932e` |
| three_hop_traversal_dense_where | 0.210709 | 152.81 | 92.34 | 1 | `53bc4cc2976e` |
| three_hop_traversal_mixed_where | 0.212348 | 149.78 | 93.71 | 1 | `53bc4cc2976e` |
| multi_match_with_where | 0.209103 | 150.55 | 92.34 | 1 | `53bc4cc2976e` |
| three_hop_wildcard_projection_order_limit | 0.290732 | 150.66 | 81.51 | 500 | `ab32f4a0932e` |
| range_hop_wildcard_projection_order_limit | 0.299757 | 143.61 | 85.41 | 313 | `b143e19b29e2` |
| unwind_parameter_pipeline | 0.000821 | 3.17 | 0.03 | 500 | `dc79e2f7ed68` |

## Notes

- The benchmark includes parse, compile, execution, and public result shaping.
- The database is generated once per configuration and reused across measured runs.
- Results should be compared on the same machine and similar thermal/load conditions.
