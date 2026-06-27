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
| scalar_filter_sort_limit | 0.189228 | 162.75 | 104.61 | 100 | `4350ec27d694` |
| relationship_property_filter_sort_limit | 0.417817 | 377.73 | 265.84 | 500 | `d992a4ad96af` |
| grouped_with_count | 0.189133 | 153.12 | 91.48 | 6 | `2b7e7f197f4b` |
| optional_match_projection | 0.369228 | 220.48 | 141.92 | 1000 | `46431f508ff9` |
| three_hop_traversal | 1.150443 | 232.73 | 153.49 | 1 | `b8820b037c37` |
| three_hop_projection_order_limit | 1.032501 | 274.56 | 173.14 | 500 | `98894437349a` |
| three_hop_traversal_dense_where | 0.707982 | 234.31 | 139.93 | 1 | `a5813890d3ba` |
| three_hop_traversal_mixed_where | 0.706606 | 239.38 | 144.85 | 1 | `a5813890d3ba` |
| three_hop_wildcard_projection_order_limit | 1.033743 | 227.56 | 125.83 | 500 | `98894437349a` |
| range_hop_wildcard_projection_order_limit | 1.033396 | 234.55 | 129.22 | 318 | `448f336964f3` |
| unwind_parameter_pipeline | 0.000886 | 2.97 | 0.03 | 500 | `dc79e2f7ed68` |

## Notes

- The benchmark includes parse, compile, execution, and public result shaping.
- The database is generated once per configuration and reused across measured runs.
- Results should be compared on the same machine and similar thermal/load conditions.
