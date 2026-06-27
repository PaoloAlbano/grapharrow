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
| scalar_filter_sort_limit | 0.189514 | 161.62 | 104.61 | 100 | `4350ec27d694` |
| relationship_property_filter_sort_limit | 0.421228 | 374.69 | 265.84 | 500 | `d992a4ad96af` |
| grouped_with_count | 0.365361 | 124.09 | 88.84 | 6 | `2b7e7f197f4b` |
| optional_match_projection | 0.375061 | 218.73 | 141.92 | 1000 | `46431f508ff9` |
| three_hop_traversal | 1.158215 | 239.75 | 152.99 | 1 | `b8820b037c37` |
| three_hop_projection_order_limit | 1.034124 | 273.59 | 173.39 | 500 | `98894437349a` |
| three_hop_traversal_dense_where | 0.705240 | 231.50 | 139.93 | 1 | `a5813890d3ba` |
| three_hop_traversal_mixed_where | 0.718013 | 246.50 | 145.85 | 1 | `a5813890d3ba` |
| three_hop_wildcard_projection_order_limit | 1.031612 | 224.66 | 125.83 | 500 | `98894437349a` |
| range_hop_wildcard_projection_order_limit | 1.038844 | 231.38 | 129.22 | 318 | `448f336964f3` |
| unwind_parameter_pipeline | 0.000893 | 3.05 | 0.03 | 500 | `dc79e2f7ed68` |

## Notes

- The benchmark includes parse, compile, execution, and public result shaping.
- The database is generated once per configuration and reused across measured runs.
- Results should be compared on the same machine and similar thermal/load conditions.
