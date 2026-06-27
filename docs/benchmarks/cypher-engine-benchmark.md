# Cypher Engine Benchmark

This benchmark measures representative GraphArrow query paths on a synthetic embedded database.

## Configuration

- Nodes: `20000`
- Edges: `120000`
- Repeats: `5`
- Warmups: `1`
- Memory probe repeats: `3`
- Seed: `42`

## Results

| Query | Median (s) | Peak RSS Δ (MB) | Arrow peak (MB) | Rows | Checksum |
| --- | ---: | ---: | ---: | ---: | --- |
| scalar_filter_sort_limit | 0.042025 | 47.38 | 22.21 | 100 | `d4dc7e6d50ab` |
| relationship_property_filter_sort_limit | 0.092695 | 101.84 | 56.00 | 500 | `273782367980` |
| grouped_with_count | 0.074734 | 38.23 | 17.38 | 6 | `aabcea513d55` |
| optional_match_projection | 0.091061 | 94.78 | 48.96 | 1000 | `47f952d1f251` |
| three_hop_traversal | 0.274378 | 140.50 | 93.60 | 1 | `4f18d6945f14` |
| three_hop_projection_order_limit | 0.239197 | 166.06 | 98.23 | 500 | `ab32f4a0932e` |
| three_hop_traversal_dense_where | 0.270693 | 125.66 | 74.50 | 1 | `53bc4cc2976e` |
| three_hop_traversal_mixed_where | 0.275213 | 128.67 | 75.33 | 1 | `53bc4cc2976e` |
| three_hop_wildcard_projection_order_limit | 0.242445 | 124.84 | 73.00 | 500 | `ab32f4a0932e` |
| range_hop_wildcard_projection_order_limit | 0.242466 | 130.45 | 76.36 | 313 | `b143e19b29e2` |
| unwind_parameter_pipeline | 0.000890 | 2.86 | 0.03 | 500 | `dc79e2f7ed68` |

## Notes

- The benchmark includes parse, compile, execution, and public result shaping.
- The database is generated once per configuration and reused across measured runs.
- Results should be compared on the same machine and similar thermal/load conditions.
