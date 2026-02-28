# PostgreSQL 18 Compatibility — Statement-Level Scores

**Overall: 10,816 / 12,791 (84.6%)**

Last updated: 2026-02-28 (after PR #68 — plpgsql 100%)

| File | Pass | Total | % |
|------|------|-------|---|
| aggregates | 470 | 568 | 82.7% |
| arrays | 452 | 515 | 87.8% |
| boolean | 85 | 98 | 86.7% |
| case | 58 | 64 | 90.6% |
| create_index | 559 | 671 | 83.3% |
| create_table | 213 | 326 | 65.3% |
| create_view | 230 | 290 | 79.3% |
| date | 203 | 271 | 74.9% |
| delete | 8 | 10 | 80.0% |
| explain | 60 | 61 | 98.4% |
| float4 | 57 | 98 | 58.2% |
| float8 | 127 | 181 | 70.2% |
| groupingsets | 193 | 216 | 89.4% |
| insert | 276 | 375 | 73.6% |
| insert_conflict | 194 | 258 | 75.2% |
| int2 | 62 | 76 | 81.6% |
| int4 | 63 | 94 | 67.0% |
| int8 | 121 | 174 | 69.5% |
| interval | 356 | 446 | 79.8% |
| join | 783 | 915 | 85.6% |
| json | 389 | 464 | 83.8% |
| jsonb | 979 | 1089 | 89.9% |
| limit | 7 | 8 | 87.5% |
| matview | 137 | 178 | 77.0% |
| merge | 480 | 560 | 85.7% |
| numeric | 945 | 1057 | 89.4% |
| **plpgsql** | **931** | **931** | **100.0%** |
| prepare | 11 | 11 | 100.0% |
| select | 82 | 86 | 95.3% |
| select_distinct | 100 | 101 | 99.0% |
| select_having | 21 | 23 | 91.3% |
| select_implicit | 29 | 44 | 65.9% |
| strings | 472 | 548 | 86.1% |
| subselect | 322 | 358 | 89.9% |
| test_setup | 37 | 58 | 63.8% |
| test_setup_modified | 31 | 58 | 53.4% |
| text | 43 | 73 | 58.9% |
| time | 37 | 44 | 84.1% |
| timestamp | 145 | 177 | 81.9% |
| truncate | 11 | 11 | 100.0% |
| union | 172 | 204 | 84.3% |
| update | 233 | 271 | 86.0% |
| window | 384 | 427 | 89.9% |
| with | 248 | 303 | 81.8% |
