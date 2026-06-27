# Cypher Engine Benchmark

This benchmark measures representative GraphArrow query paths on a synthetic embedded database.

## Configuration

- Nodes: `100000`
- Edges: `600000`
- Repeats: `5`
- Warmups: `1`
- Memory probe repeats: `3`
- Seed: `42`
- Planner enabled: `yes`

## Results

| Query | Median (s) | Peak RSS Δ (MB) | Arrow peak (MB) | Rows | Checksum |
| --- | ---: | ---: | ---: | ---: | --- |
| scalar_filter_sort_limit | 0.190441 | 163.22 | 104.61 | 100 | `4350ec27d694` |
| relationship_property_filter_sort_limit | 0.421350 | 375.88 | 265.84 | 500 | `d992a4ad96af` |
| grouped_with_count | 0.190136 | 152.73 | 91.48 | 6 | `2b7e7f197f4b` |
| optional_match_projection | 0.377068 | 219.91 | 141.92 | 1000 | `46431f508ff9` |
| three_hop_traversal | 1.188024 | 235.05 | 153.49 | 1 | `b8820b037c37` |
| three_hop_projection_order_limit | 1.189400 | 262.53 | 171.14 | 500 | `98894437349a` |
| three_hop_traversal_dense_where | 0.726828 | 218.66 | 138.94 | 1 | `a5813890d3ba` |
| three_hop_traversal_mixed_where | 0.717241 | 241.50 | 145.85 | 1 | `a5813890d3ba` |
| three_hop_wildcard_projection_order_limit | 1.033119 | 225.48 | 125.83 | 500 | `98894437349a` |
| range_hop_wildcard_projection_order_limit | 1.040797 | 230.92 | 129.22 | 318 | `448f336964f3` |
| unwind_parameter_pipeline | 0.000863 | 2.78 | 0.03 | 500 | `dc79e2f7ed68` |

## Notes

- The benchmark includes parse, compile, execution, and public result shaping.
- The database is generated once per configuration and reused across measured runs.
- Results should be compared on the same machine and similar thermal/load conditions.
