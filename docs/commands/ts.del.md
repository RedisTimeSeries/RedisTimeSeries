---
syntax: 
---

Delete all samples between two timestamps for a given time series

## Syntax

{{< highlight bash >}}
TS.DEL key fromTimestamp toTimestamp
{{< / highlight >}}

[Examples](#examples)

## Required arguments

<details open><summary><code>key</code></summary> 

is key name for the time series.
</details>

<details open><summary><code>fromTimestamp</code></summary> 

is start timestamp for the range deletion.
</details>

<details open><summary><code>toTimestamp</code></summary>

is end timestamp for the range deletion.

The given timestamp interval is closed (inclusive), meaning that samples whose timestamp eqauls the `fromTimestamp` or `toTimestamp` are also deleted.

## Return value

Integer reply: The number of samples that were removed.

## Examples 

<details open><summary><b>Delete range of data points</b></summary>

Create time series for temperature in Tel Aviv and Jerusalem, then add different temperature samples.

{{< highlight bash >}}
127.0.0.1:6379> TS.CREATE temp:TLV LABELS type temp location TLV
OK
127.0.0.1:6379> TS.CREATE temp:JLM LABELS type temp location JLM
OK
127.0.0.1:6379> TS.MADD temp:TLV 1000 30 temp:TLV 1010 35 temp:TLV 1020 9999 temp:TLV 1030 40
1) (integer) 1000
2) (integer) 1010
3) (integer) 1020
4) (integer) 1030
127.0.0.1:6379> TS.MADD temp:JLM 1005 30 temp:JLM 1015 35 temp:JLM 1025 9999 temp:JLM 1035 40
1) (integer) 1005
2) (integer) 1015
3) (integer) 1025
4) (integer) 1035
{{< / highlight >}}

Delete the range of data points for temperature in Tel Aviv.

{{< highlight bash >}}
127.0.0.1:6379> TS.DEL temp:TLV 1000 1030
(integer) 4
{{< / highlight >}}
</details>

## See also

`TS.ADD` 

## Related topics

[RedisTimeSeries](/docs/stack/timeseries)