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
| scalar_filter_sort_limit | 0.190657 | 160.81 | 104.61 | 100 | `4350ec27d694` |
| relationship_property_filter_sort_limit | 0.419727 | 380.34 | 265.84 | 500 | `d992a4ad96af` |
| grouped_with_count | 0.365257 | 129.62 | 88.84 | 6 | `2b7e7f197f4b` |
| optional_match_projection | 0.448536 | 387.25 | 326.02 | 1000 | `46431f508ff9` |
| three_hop_traversal | 1.155372 | 235.55 | 153.49 | 1 | `b8820b037c37` |
| three_hop_projection_order_limit | 1.032985 | 271.98 | 172.39 | 500 | `98894437349a` |
| three_hop_traversal_dense_where | 0.708512 | 228.72 | 139.68 | 1 | `a5813890d3ba` |
| three_hop_traversal_mixed_where | 0.710472 | 243.19 | 144.76 | 1 | `a5813890d3ba` |
| three_hop_wildcard_projection_order_limit | 1.020577 | 235.41 | 125.83 | 500 | `98894437349a` |
| range_hop_wildcard_projection_order_limit | 1.034174 | 232.75 | 129.22 | 318 | `448f336964f3` |
| unwind_parameter_pipeline | 0.000863 | 2.83 | 0.03 | 500 | `dc79e2f7ed68` |

## Notes

- The benchmark includes parse, compile, execution, and public result shaping.
- The database is generated once per configuration and reused across measured runs.
- Results should be compared on the same machine and similar thermal/load conditions.
