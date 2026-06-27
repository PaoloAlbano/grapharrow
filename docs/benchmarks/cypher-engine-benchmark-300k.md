# Cypher Engine Benchmark

This benchmark measures representative GraphArrow query paths on a synthetic embedded database.

## Configuration

- Nodes: `300000`
- Edges: `1800000`
- Repeats: `1`
- Warmups: `0`
- Memory probe repeats: `1`
- Seed: `42`

## Results

| Query | Median (s) | Peak RSS Δ (MB) | Arrow peak (MB) | Rows | Checksum |
| --- | ---: | ---: | ---: | ---: | --- |
| scalar_filter_sort_limit | 0.646011 | 314.95 | 290.18 | 100 | `5c7d22240ce5` |
| relationship_property_filter_sort_limit | 2.173346 | 816.84 | 694.70 | 500 | `59876888157e` |
| grouped_with_count | 1.087166 | 276.06 | 236.81 | 6 | `1d82529979e6` |
| optional_match_projection | 1.319780 | 872.97 | 728.96 | 1000 | `83b9b6769b04` |
| three_hop_traversal | 3.817054 | 377.39 | 318.42 | 1 | `dd41391174b0` |
| three_hop_projection_order_limit | 3.810862 | 404.23 | 330.38 | 500 | `be5d783c7c8e` |
| three_hop_traversal_dense_where | 3.862925 | 170.16 | 310.04 | 1 | `e7696b63b9a1` |
| three_hop_traversal_mixed_where | 3.954352 | 387.69 | 346.91 | 1 | `6ba2754f298e` |
| three_hop_wildcard_projection_order_limit | 3.162959 | 364.09 | 286.85 | 500 | `be5d783c7c8e` |
| range_hop_wildcard_projection_order_limit | 3.062921 | 360.30 | 290.24 | 282 | `61fca5016697` |
| unwind_parameter_pipeline | 0.009037 | 3.31 | 0.03 | 500 | `dc79e2f7ed68` |

## Notes

- The benchmark includes parse, compile, execution, and public result shaping.
- The database is generated once per configuration and reused across measured runs.
- Results should be compared on the same machine and similar thermal/load conditions.
