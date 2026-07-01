# Bolt Engine Comparison Benchmark

This benchmark measures end-to-end Bolt response time for the shared Cypher workload.

## Configuration

- Query set: `got`
- Nodes: `20000`
- Edges: `120000`
- Repeats: `3`
- Warmups: `1`
- Seed: `42`
- Seed mode: `per-engine`
- Lookup indexes during seed: `no`
- Engines: `grapharrow, neo4j, memgraph`

## Results

| Engine | Query | Status | Median (s) | Best (s) | Worst (s) | Rows | Checksum | Semantic |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| grapharrow | got_schema_labels | ok | 0.008730 | 0.008432 | 0.009406 | 6 | `3ae3acba4603` | `9b29565fc381` |
| grapharrow | got_schema_relationship_types | ok | 0.024704 | 0.024217 | 0.038675 | 6 | `3d01257bd978` | `ce0aee594d7e` |
| grapharrow | got_character_name_projection | ok | 0.007057 | 0.006991 | 0.007262 | 100 | `ab30d42ed4d9` | `9aa44acc2ac6` |
| grapharrow | got_character_name_starts_with | ok | 0.005943 | 0.005554 | 0.006212 | 2 | `15761ab4e64d` | `15761ab4e64d` |
| grapharrow | got_character_name_tolower_contains | ok | 0.005430 | 0.005385 | 0.005500 | 1 | `04b449ae9488` | `04b449ae9488` |
| grapharrow | got_character_keys_sample | ok | 0.008509 | 0.008490 | 0.008531 | 20 | `03472999aeae` | `03472999aeae` |
| grapharrow | got_episode_deaths | ok | 0.016531 | 0.015743 | 0.026072 | 69 | `779a72897c67` | `f1293e0340a3` |
| grapharrow | got_season_deaths | ok | 0.014467 | 0.014457 | 0.014789 | 8 | `02c525d5b017` | `fa14ad2744bf` |
| grapharrow | got_location_deaths | ok | 0.014507 | 0.014285 | 0.014567 | 41 | `d5f1b8198acb` | `b69fac2b3124` |
| grapharrow | got_castle_location_text_filter | ok | 0.014250 | 0.014161 | 0.026610 | 1 | `61c30d5b35e9` | `61c30d5b35e9` |
| grapharrow | got_relationship_type_text_filter_count | ok | 0.022932 | 0.022758 | 0.023049 | 1 | `02d38acf3875` | `02d38acf3875` |
| grapharrow | got_killed_method_text_filter_count | ok | 0.023827 | 0.023726 | 0.036506 | 1 | `5a306cbe375a` | `5a306cbe375a` |
| grapharrow | got_episode_avg_rating | ok | 0.016366 | 0.016327 | 0.016505 | 8 | `060aaad72845` | `a045957e5180` |
| grapharrow | got_episode_collect_death_orders | ok | 0.018868 | 0.018280 | 0.030063 | 20 | `753433733ff1` | `f31983c63754` |
| grapharrow | got_victim_location_two_hop_projection | ok | 0.018769 | 0.017975 | 0.020234 | 100 | `c34a578f3769` | `c34a578f3769` |
| grapharrow | got_unwind_parameter_names | ok | 0.000939 | 0.000925 | 0.000970 | 5 | `a07e5c5c01da` | `a07e5c5c01da` |
| grapharrow | got_visual_death_location | ok | 0.031671 | 0.031511 | 0.049494 | 50 | `5db1ac565533` | `175a31910bfb` |
| grapharrow | got_jon_snow_death_locations | ok | 0.068014 | 0.066044 | 0.081415 | 51 | `9501e59f8795` | `3fe6918a5631` |
| neo4j | got_schema_labels | ok | 0.001179 | 0.001114 | 0.001246 | 6 | `3ae3acba4603` | `9b29565fc381` |
| neo4j | got_schema_relationship_types | ok | 0.002452 | 0.002369 | 0.002722 | 6 | `00d8c8a19424` | `ce0aee594d7e` |
| neo4j | got_character_name_projection | ok | 0.001789 | 0.001642 | 0.001815 | 100 | `ab30d42ed4d9` | `9aa44acc2ac6` |
| neo4j | got_character_name_starts_with | ok | 0.000585 | 0.000500 | 0.000601 | 2 | `15761ab4e64d` | `15761ab4e64d` |
| neo4j | got_character_name_tolower_contains | ok | 0.000579 | 0.000519 | 0.000632 | 1 | `04b449ae9488` | `04b449ae9488` |
| neo4j | got_character_keys_sample | ok | 0.000772 | 0.000724 | 0.000788 | 20 | `0d0b276d9fc7` | `03472999aeae` |
| neo4j | got_episode_deaths | ok | 0.001600 | 0.001521 | 0.001759 | 69 | `779a72897c67` | `f1293e0340a3` |
| neo4j | got_season_deaths | ok | 0.000807 | 0.000802 | 0.000931 | 8 | `02c525d5b017` | `fa14ad2744bf` |
| neo4j | got_location_deaths | ok | 0.001212 | 0.001145 | 0.001459 | 41 | `d5f1b8198acb` | `b69fac2b3124` |
| neo4j | got_castle_location_text_filter | ok | 0.000535 | 0.000487 | 0.000547 | 1 | `61c30d5b35e9` | `61c30d5b35e9` |
| neo4j | got_relationship_type_text_filter_count | ok | 0.002072 | 0.002070 | 0.002084 | 1 | `02d38acf3875` | `02d38acf3875` |
| neo4j | got_killed_method_text_filter_count | ok | 0.000746 | 0.000743 | 0.000811 | 1 | `5a306cbe375a` | `5a306cbe375a` |
| neo4j | got_episode_avg_rating | ok | 0.000725 | 0.000722 | 0.001039 | 8 | `060aaad72845` | `a045957e5180` |
| neo4j | got_episode_collect_death_orders | ok | 0.001213 | 0.001145 | 0.001265 | 20 | `3b571ad5853e` | `f31983c63754` |
| neo4j | got_victim_location_two_hop_projection | ok | 0.002498 | 0.002435 | 0.002924 | 100 | `c34a578f3769` | `c34a578f3769` |
| neo4j | got_unwind_parameter_names | ok | 0.000477 | 0.000474 | 0.000516 | 5 | `a07e5c5c01da` | `a07e5c5c01da` |
| neo4j | got_visual_death_location | ok | 0.002195 | 0.002156 | 0.002236 | 50 | `6369113c35ca` | `175a31910bfb` |
| neo4j | got_jon_snow_death_locations | ok | 0.002754 | 0.002602 | 0.002948 | 51 | `68fed69244ae` | `3fe6918a5631` |
| memgraph | got_schema_labels | ok | 0.037634 | 0.025664 | 0.265788 | 6 | `3ae3acba4603` | `9b29565fc381` |
| memgraph | got_schema_relationship_types | ok | 0.161047 | 0.074204 | 0.204168 | 6 | `3d01257bd978` | `ce0aee594d7e` |
| memgraph | got_character_name_projection | ok | 0.047399 | 0.034798 | 0.055420 | 100 | `ab30d42ed4d9` | `9aa44acc2ac6` |
| memgraph | got_character_name_starts_with | ok | 0.012371 | 0.001152 | 0.035283 | 2 | `15761ab4e64d` | `15761ab4e64d` |
| memgraph | got_character_name_tolower_contains | ok | 0.001051 | 0.000956 | 0.001113 | 1 | `04b449ae9488` | `04b449ae9488` |
| memgraph | got_character_keys_sample | ok | 0.001928 | 0.001151 | 0.002452 | 20 | `b24945348265` | `03472999aeae` |
| memgraph | got_episode_deaths | ok | 0.006487 | 0.003123 | 0.006541 | 69 | `59a633ac2613` | `f1293e0340a3` |
| memgraph | got_season_deaths | ok | 0.001775 | 0.001741 | 0.001798 | 8 | `02c525d5b017` | `fa14ad2744bf` |
| memgraph | got_location_deaths | ok | 0.001202 | 0.001137 | 0.001231 | 41 | `faaf7cde69a1` | `b69fac2b3124` |
| memgraph | got_castle_location_text_filter | ok | 0.000627 | 0.000569 | 0.000867 | 1 | `61c30d5b35e9` | `61c30d5b35e9` |
| memgraph | got_relationship_type_text_filter_count | ok | 0.002708 | 0.002558 | 0.002851 | 1 | `02d38acf3875` | `02d38acf3875` |
| memgraph | got_killed_method_text_filter_count | ok | 0.000757 | 0.000716 | 0.000762 | 1 | `5a306cbe375a` | `5a306cbe375a` |
| memgraph | got_episode_avg_rating | ok | 0.000745 | 0.000668 | 0.004040 | 8 | `060aaad72845` | `a045957e5180` |
| memgraph | got_episode_collect_death_orders | ok | 0.002293 | 0.002034 | 0.002534 | 20 | `753433733ff1` | `f31983c63754` |
| memgraph | got_victim_location_two_hop_projection | ok | 0.003382 | 0.003052 | 0.004737 | 100 | `c34a578f3769` | `c34a578f3769` |
| memgraph | got_unwind_parameter_names | ok | 0.000396 | 0.000362 | 0.000511 | 5 | `a07e5c5c01da` | `a07e5c5c01da` |
| memgraph | got_visual_death_location | ok | 0.002845 | 0.002589 | 0.002929 | 50 | `95d96814bd45` | `175a31910bfb` |
| memgraph | got_jon_snow_death_locations | ok | 0.003341 | 0.003201 | 0.003483 | 51 | `184aacaa83dc` | `3fe6918a5631` |

## Notes

- Memory is not measured for external engines in this benchmark.
- Timings include Bolt round trip, result pull, and Python driver materialization.
- Seed time is not included in query timings.
- Graph/value checksums are engine-local and are not expected to match across engines.
- Semantic checksums ignore engine-local graph IDs and compare a canonicalized unordered result multiset.
