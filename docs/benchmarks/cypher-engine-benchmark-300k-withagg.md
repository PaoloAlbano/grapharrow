# Cypher Engine Benchmark

This benchmark measures representative GraphArrow query paths on a synthetic embedded database.

## Configuration

- Nodes: `300000`
- Edges: `1800000`
- Repeats: `5`
- Warmups: `1`
- Memory probe repeats: `3`
- Seed: `42`
- Planner enabled: `yes`

## Results

| Query | Median (s) | Peak RSS Δ (MB) | Arrow peak (MB) | Rows | Checksum |
| --- | ---: | ---: | ---: | ---: | --- |
| scalar_filter_sort_limit | 0.553726 | 286.03 | 290.18 | 100 | `5c7d22240ce5` |
| relationship_property_filter_sort_limit | 1.270902 | 815.38 | 788.91 | 500 | `59876888157e` |
| grouped_with_count | 0.554783 | 297.41 | 273.18 | 6 | `1d82529979e6` |
| optional_match_projection | 1.140430 | 426.47 | 432.09 | 1000 | `83b9b6769b04` |
| three_hop_traversal | 3.477817 | 370.95 | 330.73 | 1 | `dd41391174b0` |
| three_hop_projection_order_limit | 3.043168 | 355.81 | 350.93 | 500 | `be5d783c7c8e` |
| three_hop_traversal_dense_where | 2.127880 | 405.30 | 330.41 | 1 | `e7696b63b9a1` |
| three_hop_traversal_mixed_where | 2.141131 | 410.72 | 368.19 | 1 | `6ba2754f298e` |
| three_hop_wildcard_projection_order_limit | 3.011295 | 367.34 | 286.85 | 500 | `be5d783c7c8e` |
| range_hop_wildcard_projection_order_limit | 3.005739 | 302.78 | 290.24 | 282 | `61fca5016697` |
| unwind_parameter_pipeline | 0.001044 | 2.95 | 0.03 | 500 | `dc79e2f7ed68` |

## Notes

- The benchmark includes parse, compile, execution, and public result shaping.
- The database is generated once per configuration and reused across measured runs.
- Results should be compared on the same machine and similar thermal/load conditions.
