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
| scalar_filter_sort_limit | 0.188833 | 161.12 | 104.61 | 100 | `4350ec27d694` |
| relationship_property_filter_sort_limit | 0.417086 | 375.98 | 265.84 | 500 | `d992a4ad96af` |
| grouped_with_count | 0.190687 | 151.70 | 91.48 | 6 | `2b7e7f197f4b` |
| return_distinct_projection_order_limit | 0.196699 | 149.44 | 88.84 | 1000 | `f3bcd4d478ba` |
| optional_match_projection | 0.370226 | 214.53 | 141.92 | 1000 | `46431f508ff9` |
| optional_match_return_distinct_projection | 0.499532 | 362.88 | 221.63 | 1000 | `5d29b4ea5f79` |
| three_hop_traversal | 1.185149 | 234.16 | 153.49 | 1 | `b8820b037c37` |
| three_hop_projection_order_limit | 1.042277 | 273.58 | 173.64 | 500 | `98894437349a` |
| three_hop_traversal_dense_where | 0.716983 | 230.91 | 139.93 | 1 | `a5813890d3ba` |
| three_hop_traversal_mixed_where | 0.711847 | 242.41 | 145.85 | 1 | `a5813890d3ba` |
| three_hop_wildcard_projection_order_limit | 1.027689 | 223.69 | 125.83 | 500 | `98894437349a` |
| range_hop_wildcard_projection_order_limit | 1.048798 | 235.47 | 129.22 | 318 | `448f336964f3` |
| unwind_parameter_pipeline | 0.000828 | 2.64 | 0.03 | 500 | `dc79e2f7ed68` |

## Notes

- The benchmark includes parse, compile, execution, and public result shaping.
- The database is generated once per configuration and reused across measured runs.
- Results should be compared on the same machine and similar thermal/load conditions.
