# GraphArrow Interface Comparison on GOT

Compares in-process GraphArrow interfaces against the previous GraphArrow Bolt median from `bolt-engine-comparison-got-preloaded.json`.

| Query | Interface | Median (s) | Best (s) | Rows | Bolt median (s) | vs Bolt |
|---|---|---:|---:|---:|---:|---:|
| `got_character_name_projection` | `embedded` | 0.005319 | 0.005110 | 100 | 0.006839 | 0.78x |
| `got_character_name_projection` | `rest_testclient` | 0.006272 | 0.005963 | 100 | 0.006839 | 0.92x |
| `got_character_name_projection` | `mcp_cypher_direct` | 0.004863 | 0.004739 | 100 | 0.006839 | 0.71x |
| `got_character_name_tolower_contains` | `embedded` | 0.004866 | 0.004815 | 1 | 0.005297 | 0.92x |
| `got_character_name_tolower_contains` | `rest_testclient` | 0.006077 | 0.005812 | 1 | 0.005297 | 1.15x |
| `got_character_name_tolower_contains` | `mcp_cypher_direct` | 0.005629 | 0.004811 | 1 | 0.005297 | 1.06x |
| `got_location_deaths` | `embedded` | 0.013072 | 0.012925 | 41 | 0.013913 | 0.94x |
| `got_location_deaths` | `rest_testclient` | 0.014291 | 0.013981 | 41 | 0.013913 | 1.03x |
| `got_location_deaths` | `mcp_cypher_direct` | 0.012979 | 0.012811 | 41 | 0.013913 | 0.93x |
| `got_castle_location_text_filter` | `embedded` | 0.018189 | 0.013424 | 1 | 0.013844 | 1.31x |
| `got_castle_location_text_filter` | `rest_testclient` | 0.014818 | 0.014659 | 1 | 0.013844 | 1.07x |
| `got_castle_location_text_filter` | `mcp_cypher_direct` | 0.013358 | 0.013216 | 1 | 0.013844 | 0.96x |
| `got_killed_method_text_filter_count` | `embedded` | 0.023207 | 0.022993 | 1 | 0.023815 | 0.97x |
| `got_killed_method_text_filter_count` | `rest_testclient` | 0.023842 | 0.023614 | 1 | 0.023815 | 1.00x |
| `got_killed_method_text_filter_count` | `mcp_cypher_direct` | 0.023003 | 0.022880 | 1 | 0.023815 | 0.97x |
| `got_victim_location_two_hop_projection` | `embedded` | 0.015318 | 0.014850 | 100 | 0.017484 | 0.88x |
| `got_victim_location_two_hop_projection` | `rest_testclient` | 0.016262 | 0.016109 | 100 | 0.017484 | 0.93x |
| `got_victim_location_two_hop_projection` | `mcp_cypher_direct` | 0.015242 | 0.015167 | 100 | 0.017484 | 0.87x |
| `got_unwind_parameter_names` | `embedded` | 0.000480 | 0.000473 | 5 | 0.000933 | 0.51x |
| `got_unwind_parameter_names` | `rest_testclient` | 0.001603 | 0.001517 | 5 | 0.000933 | 1.72x |
| `got_unwind_parameter_names` | `mcp_cypher_direct` | 0.000484 | 0.000442 | 5 | 0.000933 | 0.52x |
| `got_visual_death_location` | `embedded` | 0.029520 | 0.029329 | 50 | 0.031575 | 0.93x |
| `got_visual_death_location` | `rest_testclient` | 0.030833 | 0.030192 | 50 | 0.031575 | 0.98x |
| `got_visual_death_location` | `mcp_cypher_direct` | 0.029551 | 0.029101 | 50 | 0.031575 | 0.94x |
| `got_jon_snow_death_locations` | `embedded` | 0.083007 | 0.080330 | 51 | 0.084578 | 0.98x |
| `got_jon_snow_death_locations` | `rest_testclient` | 0.082637 | 0.082059 | 51 | 0.084578 | 0.98x |
| `got_jon_snow_death_locations` | `mcp_cypher_direct` | 0.081529 | 0.081076 | 51 | 0.084578 | 0.96x |
