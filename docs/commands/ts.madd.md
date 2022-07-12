---
syntax: 
---

Append new samples to one or more time series

{{< highlight bash >}}
TS.MADD {key timestamp value}...
{{< / highlight >}}

[**Examples**](#examples)

## Required arguments

`key` is the key name for the time series.

`timestamp` is (integer) UNIX sample timestamp in milliseconds or `*` to set the timestamp to the server clock.

`value` is numeric data value of the sample (double). The double number should follow [RFC 7159](https://tools.ietf.org/html/rfc7159) (JSON standard). The parser rejects overly large values that would not fit in binary64. It does not accept NaN or infinite values.

## Complexity

If a compaction rule exits on a time series, `TS.MADD` performance might be reduced.
The complexity of `TS.MADD` is always O(N*M), where N is the amount of series updated and M is the amount of compaction rules or O(N) with no compaction.

## Examples

### Add stock prices at different timestamps

Create two stocks and add their prices at three different timestamps.

{{< highlight bash >}}
127.0.0.1:6379> TS.CREATE stock:A LABELS type stock name A
OK
127.0.0.1:6379> TS.CREATE stock:B LABELS type stock name B
OK
127.0.0.1:6379> TS.MADD stock:A 1000 100 stock:A 1010 110 stock:A 1020 120
1) (integer) 1000
2) (integer) 1010
3) (integer) 1020
127.0.0.1:6379> TS.MADD stock:B 1000 120 stock:B 1010 110 stock:B 1020 100
1) (integer) 1000
2) (integer) 1010
3) (integer) 1020
{{< / highlight >}}

## See also

`TS.MRANGE` | `TS.RANGE` | `TS.MREVRANGE` | `TS.REVRANGE`

## Related topics

[RedisTimeSeries](/docs/stack/timeseries)
