---
syntax: 
---

Get the last sample

## Syntax

{{< highlight bash >}}
TS.GET key 
  [LATEST]
{{< / highlight >}}

[Examples](#examples)

## Required arguments

<details open><summary><code>key</code></summary> 

is key name for the time series.
</details>

## Optional arguments

<details open><summary><code>LATEST</code> (since RedisTimeSeries v1.8)</summary> 

is used when a time series is a compaction. With `LATEST`, TS.MRANGE also reports the compacted value of the latest possibly partial bucket, given that this bucket's start time falls within `[fromTimestamp, toTimestamp]`. Without `LATEST`, TS.MRANGE does not report the latest possibly partial bucket. When a time series is not a compaction, `LATEST` is ignored.
  
The data in the latest bucket of a compaction is possibly partial. A bucket is _closed_ and compacted only upon arrival of a new sample that _opens_ a new _latest_ bucket. There are cases, however, when the compacted value of the latest possibly partial bucket is also required. In such a case, use `LATEST`.
</details>

## Return value

The returned array contains:
- The last sample timestamp, followed by the last sample value, when the time series contains data. 
- An empty array, when the time series is empty.

## Examples

<details open>
<summary><b>Get last temperature sample for a city</b></summary>

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

Get the last timestamp and temperature value for Jerusalem.

{{< highlight bash >}}
127.0.0.1:6379> TS.GET temp:JLM
1) (integer) 1035
2) 40
127.0.0.1:6379>
{{< / highlight >}}
</details>

## See also

`TS.MGET`  

## Related topics

[RedisTimeSeries](/docs/stack/timeseries)