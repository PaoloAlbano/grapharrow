# Cypher Engine Benchmark

This benchmark measures representative GraphArrow query paths on a synthetic embedded database.

## Configuration

- Nodes: `100000`
- Edges: `600000`
- Repeats: `5`
- Warmups: `1`
- Memory probe repeats: `3`
- Seed: `42`

## Results

| Query | Median (s) | Peak RSS Δ (MB) | Arrow peak (MB) | Rows | Checksum |
| --- | ---: | ---: | ---: | ---: | --- |
| scalar_filter_sort_limit | 0.198199 | 163.05 | 104.61 | 100 | `4350ec27d694` |
| relationship_property_filter_sort_limit | 0.425858 | 372.16 | 265.84 | 500 | `d992a4ad96af` |
| grouped_with_count | 0.366448 | 129.12 | 88.84 | 6 | `2b7e7f197f4b` |
| optional_match_projection | 0.430925 | 360.47 | 244.74 | 1000 | `46431f508ff9` |
| three_hop_traversal | 1.316414 | 244.36 | 151.92 | 1 | `b8820b037c37` |
| three_hop_projection_order_limit | 1.074917 | 272.34 | 170.89 | 500 | `98894437349a` |
| three_hop_traversal_dense_where | 1.319572 | 230.11 | 136.50 | 1 | `a5813890d3ba` |
| three_hop_traversal_mixed_where | 1.336079 | 243.91 | 142.37 | 1 | `a5813890d3ba` |
| three_hop_wildcard_projection_order_limit | 1.028920 | 225.91 | 125.83 | 500 | `98894437349a` |
| range_hop_wildcard_projection_order_limit | 1.023901 | 234.89 | 129.22 | 318 | `448f336964f3` |
| unwind_parameter_pipeline | 0.000874 | 2.94 | 0.03 | 500 | `dc79e2f7ed68` |

## Notes

- The benchmark includes parse, compile, execution, and public result shaping.
- The database is generated once per configuration and reused across measured runs.
- Results should be compared on the same machine and similar thermal/load conditions.
