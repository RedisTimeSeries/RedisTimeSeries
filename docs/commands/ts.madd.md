### TS.MADD

Append new samples to one or more time series.

```sql
TS.MADD {key timestamp value}...
```

- _key_ - Key name for time series
- _timestamp_ - (integer) UNIX sample timestamp **in milliseconds**. `*` can be used for an automatic timestamp from the system clock.
- _value_ - numeric data value of the sample (double). We expect the double number to follow [RFC 7159](https://tools.ietf.org/html/rfc7159) (JSON standard). In particular, the parser will reject overly large values that would not fit in binary64. It will not accept NaN or infinite values.

#### Examples
```sql
127.0.0.1:6379>TS.MADD temperature:2:32 1548149180000 26 cpu:2:32 1548149183000 54
1) (integer) 1548149180000
2) (integer) 1548149183000
127.0.0.1:6379>TS.MADD temperature:2:32 1548149181000 45 cpu:2:32 1548149180000 30
1) (integer) 1548149181000
2) (integer) 1548149180000
```

#### Complexity

If a compaction rule exits on a time series, `TS.MADD` performance might be reduced.
The complexity of `TS.MADD` is always O(N*M) when N is the amount of series updated and M is the amount of compaction rules or O(N) with no compaction.
