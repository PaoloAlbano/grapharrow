# GOT Embedded GraphArrow Benchmark

- Engine: `grapharrow-embedded`
- Query set: `got`
- Database path: `/tmp/grapharrow-db-D`
- Repeats: `5`
- Warmups: `2`

| Query | Status | Median (s) | Best (s) | Worst (s) | Rows | Checksum |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| got_schema_labels | ok | 0.008717 | 0.008607 | 0.020636 | 6 | `3ae3acba4603` |
| got_schema_relationship_types | ok | 0.030134 | 0.029405 | 0.041464 | 6 | `3d01257bd978` |
| got_episode_deaths | ok | 0.017602 | 0.016947 | 0.018584 | 69 | `779a72897c67` |
| got_season_deaths | ok | 0.019392 | 0.018077 | 0.019734 | 8 | `02c525d5b017` |
| got_location_deaths | ok | 0.032768 | 0.031352 | 0.033123 | 41 | `d5f1b8198acb` |
| got_castle_location_text_filter | ok | 0.033036 | 0.031820 | 0.035077 | 1 | `61c30d5b35e9` |
| got_episode_avg_rating | ok | 0.018693 | 0.017668 | 0.021254 | 8 | `060aaad72845` |
| got_visual_death_location | ok | 0.032805 | 0.032010 | 0.033263 | 50 | `dedbe20f3f3f` |
| got_jon_snow_death_locations | ok | 0.094649 | 0.087189 | 0.100593 | 51 | `820ebe1d8c85` |
