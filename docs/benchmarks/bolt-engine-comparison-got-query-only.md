# Bolt Engine Comparison Benchmark

This benchmark measures end-to-end Bolt response time for the shared Cypher workload.

## Configuration

- Query set: `got`
- Nodes: `20000`
- Edges: `120000`
- Repeats: `3`
- Warmups: `1`
- Seed: `42`
- Seed mode: `disabled`
- Lookup indexes during seed: `no`
- Engines: `grapharrow, neo4j, memgraph`

## Results

| Engine | Query | Status | Median (s) | Best (s) | Worst (s) | Rows | Checksum | Semantic |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| grapharrow | got_schema_labels | ok | 0.011050 | 0.010805 | 0.012647 | 6 | `3ae3acba4603` | `9b29565fc381` |
| grapharrow | got_schema_relationship_types | ok | 0.042212 | 0.042004 | 0.054035 | 6 | `3d01257bd978` | `ce0aee594d7e` |
| grapharrow | got_episode_deaths | ok | 0.049492 | 0.039966 | 0.061971 | 69 | `779a72897c67` | `f1293e0340a3` |
| grapharrow | got_season_deaths | ok | 0.044043 | 0.034018 | 0.046400 | 8 | `02c525d5b017` | `fa14ad2744bf` |
| grapharrow | got_location_deaths | ok | 0.059678 | 0.049199 | 0.065285 | 41 | `d5f1b8198acb` | `b69fac2b3124` |
| grapharrow | got_castle_location_text_filter | ok | 0.044546 | 0.041021 | 0.058604 | 1 | `61c30d5b35e9` | `61c30d5b35e9` |
| grapharrow | got_episode_avg_rating | ok | 0.028223 | 0.022656 | 0.043236 | 8 | `060aaad72845` | `a045957e5180` |
| grapharrow | got_visual_death_location | ok | 0.034375 | 0.034156 | 0.037764 | 50 | `44e4bdb72a0e` | `422ec6d5708f` |
| grapharrow | got_jon_snow_death_locations | ok | 0.091971 | 0.090462 | 0.101411 | 51 | `fe7225d81b3e` | `3fe6918a5631` |
| neo4j | got_schema_labels | ok | 0.021855 | 0.020459 | 0.279439 | 1 | `19309b08464d` | `19309b08464d` |
| neo4j | got_schema_relationship_types | ok | 0.231713 | 0.093907 | 0.328828 | 2 | `4ef318870d41` | `f1b3448e4949` |
| neo4j | got_episode_deaths | ok | 0.002544 | 0.001699 | 0.064848 | 0 | `4f53cda18c2b` | `4f53cda18c2b` |
| neo4j | got_season_deaths | ok | 0.002949 | 0.002196 | 0.018692 | 0 | `4f53cda18c2b` | `4f53cda18c2b` |
| neo4j | got_location_deaths | ok | 0.001625 | 0.001601 | 0.004727 | 0 | `4f53cda18c2b` | `4f53cda18c2b` |
| neo4j | got_castle_location_text_filter | ok | 0.003082 | 0.002837 | 0.012051 | 0 | `4f53cda18c2b` | `4f53cda18c2b` |
| neo4j | got_episode_avg_rating | ok | 0.002161 | 0.001986 | 0.009344 | 0 | `4f53cda18c2b` | `4f53cda18c2b` |
| neo4j | got_visual_death_location | ok | 0.001746 | 0.001618 | 0.004631 | 0 | `4f53cda18c2b` | `4f53cda18c2b` |
| neo4j | got_jon_snow_death_locations | ok | 0.001682 | 0.001389 | 0.008774 | 0 | `4f53cda18c2b` | `4f53cda18c2b` |
| memgraph | got_schema_labels | ok | 0.004075 | 0.003693 | 0.004152 | 1 | `19309b08464d` | `19309b08464d` |
| memgraph | got_schema_relationship_types | ok | 0.017717 | 0.017329 | 0.017723 | 2 | `4ef318870d41` | `f1b3448e4949` |
| memgraph | got_episode_deaths | ok | 0.001623 | 0.001528 | 0.002462 | 0 | `4f53cda18c2b` | `4f53cda18c2b` |
| memgraph | got_season_deaths | ok | 0.001942 | 0.001468 | 0.002081 | 0 | `4f53cda18c2b` | `4f53cda18c2b` |
| memgraph | got_location_deaths | ok | 0.001935 | 0.001217 | 0.002289 | 0 | `4f53cda18c2b` | `4f53cda18c2b` |
| memgraph | got_castle_location_text_filter | ok | 0.001305 | 0.001214 | 0.001594 | 0 | `4f53cda18c2b` | `4f53cda18c2b` |
| memgraph | got_episode_avg_rating | ok | 0.001287 | 0.001201 | 0.002235 | 0 | `4f53cda18c2b` | `4f53cda18c2b` |
| memgraph | got_visual_death_location | ok | 0.001652 | 0.001645 | 0.002361 | 0 | `4f53cda18c2b` | `4f53cda18c2b` |
| memgraph | got_jon_snow_death_locations | ok | 0.001464 | 0.001342 | 0.001560 | 0 | `4f53cda18c2b` | `4f53cda18c2b` |

## Notes

- Memory is not measured for external engines in this benchmark.
- Timings include Bolt round trip, result pull, and Python driver materialization.
- Seed time is not included in query timings.
- Graph/value checksums are engine-local and are not expected to match across engines.
- Semantic checksums ignore engine-local graph IDs and compare a canonicalized unordered result multiset.
