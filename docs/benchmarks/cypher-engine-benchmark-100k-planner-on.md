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
| scalar_filter_sort_limit | 0.191023 | 162.19 | 104.61 | 100 | `4350ec27d694` |
| relationship_property_filter_sort_limit | 0.421943 | 380.97 | 265.84 | 500 | `d992a4ad96af` |
| grouped_with_count | 0.377150 | 138.84 | 88.84 | 6 | `2b7e7f197f4b` |
| optional_match_projection | 0.423971 | 359.89 | 244.74 | 1000 | `46431f508ff9` |
| three_hop_traversal | 1.151226 | 232.34 | 151.49 | 1 | `b8820b037c37` |
| three_hop_projection_order_limit | 1.042527 | 270.17 | 172.89 | 500 | `98894437349a` |
| three_hop_traversal_dense_where | 0.708537 | 241.14 | 139.43 | 1 | `a5813890d3ba` |
| three_hop_traversal_mixed_where | 0.706229 | 247.25 | 145.85 | 1 | `a5813890d3ba` |
| three_hop_wildcard_projection_order_limit | 1.013793 | 225.83 | 125.83 | 500 | `98894437349a` |
| range_hop_wildcard_projection_order_limit | 1.041094 | 233.39 | 129.22 | 318 | `448f336964f3` |
| unwind_parameter_pipeline | 0.000944 | 3.09 | 0.03 | 500 | `dc79e2f7ed68` |

## Notes

- The benchmark includes parse, compile, execution, and public result shaping.
- The database is generated once per configuration and reused across measured runs.
- Results should be compared on the same machine and similar thermal/load conditions.
