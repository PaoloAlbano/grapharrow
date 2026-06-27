# Cypher Engine Benchmark

This benchmark measures representative GraphArrow query paths on a synthetic embedded database.

## Configuration

- Nodes: `100000`
- Edges: `600000`
- Repeats: `5`
- Warmups: `1`
- Memory probe repeats: `3`
- Seed: `42`
- Planner enabled: `no`

## Results

| Query | Median (s) | Peak RSS Δ (MB) | Arrow peak (MB) | Rows | Checksum |
| --- | ---: | ---: | ---: | ---: | --- |
| scalar_filter_sort_limit | 0.190638 | 159.47 | 104.61 | 100 | `4350ec27d694` |
| relationship_property_filter_sort_limit | 0.738714 | 379.94 | 265.84 | 500 | `d992a4ad96af` |
| grouped_with_count | 0.363714 | 151.70 | 91.48 | 6 | `2b7e7f197f4b` |
| return_distinct_projection_order_limit | 0.197626 | 150.55 | 88.84 | 1000 | `f3bcd4d478ba` |
| optional_match_projection | 0.424795 | 212.86 | 141.92 | 1000 | `46431f508ff9` |
| optional_match_return_distinct_projection | 0.449979 | 358.69 | 221.63 | 771 | `0d8de3005ce1` |
| three_hop_traversal | 1.350630 | 236.86 | 152.24 | 1 | `b8820b037c37` |
| three_hop_projection_order_limit | 1.348774 | 271.61 | 173.39 | 500 | `98894437349a` |
| three_hop_traversal_dense_where | 1.318345 | 233.75 | 139.93 | 1 | `a5813890d3ba` |
| three_hop_traversal_mixed_where | 1.315319 | 238.19 | 145.85 | 1 | `a5813890d3ba` |
| three_hop_wildcard_projection_order_limit | 1.029989 | 227.06 | 125.83 | 500 | `98894437349a` |
| range_hop_wildcard_projection_order_limit | 1.047228 | 229.16 | 129.22 | 318 | `448f336964f3` |
| unwind_parameter_pipeline | 0.000937 | 2.80 | 0.03 | 500 | `dc79e2f7ed68` |

## Notes

- The benchmark includes parse, compile, execution, and public result shaping.
- The database is generated once per configuration and reused across measured runs.
- Results should be compared on the same machine and similar thermal/load conditions.
