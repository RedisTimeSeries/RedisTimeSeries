---
syntax: 
---

Increase the value of the sample with the maximum existing timestamp, or create a new sample with a value equal to the value of the sample with the maximum existing timestamp with a given increment

## Syntax

{{< highlight bash >}}
TS.INCRBY key value [TIMESTAMP timestamp] [RETENTION retentionPeriod] [UNCOMPRESSED] [CHUNK_SIZE size] [LABELS {label value}...]
{{< / highlight >}}

[Examples](#examples)

## Required arguments

<details open><summary><code>key</code></summary> 

is key name for the time series.
</details>

<details open><summary><code>value</code></summary> 

is numeric data value of the sample (double)
</details>

<note><b>Notes</b>

 - If the time series does not exist, it is automatically created.
 - You can use this command as a counter or gauge that automatically gets history as a time series.
</note>

## Optional arguments

<details open><summary><code>TIMESTAMP timestamp</code></summary> 

is (integer) UNIX sample timestamp in milliseconds or `*` to set the timestamp according to the server clock.

`timestamp` must be equal to or higher than the maximum existing timestamp. When equal, the value of the sample with the maximum existing timestamp is increased. If it is higher, a new sample with a timestamp set to `timestamp` is created, and its value is set to the value of the sample with the maximum existing timestamp plus `value`. 

If the time series is empty, the value is set to `value`. When not specified, set the timestamp to the server clock.
</details>

<details open><summary><code>RETENTION retentionPeriod</code></summmary> 

is maximum retention period, compared to the maximum existing timestamp, in milliseconds. Use it only if you are creating a new time series. It is ignored if you are adding samples to an existing time series. See `RETENTION` in `TS.CREATE`.
</details>

 
<details open><summary><code>UNCOMPRESSED</code></summary>

changes data storage from compressed (default) to uncompressed. Use it only if you are creating a new time series. It is ignored if you are adding samples to an existing time series. See `ENCODING` in `TS.CREATE`.
</details>

<details open><summary><code>CHUNK_SIZE size</code></summary> 

is memory size, in bytes, allocated for each data chunk. Use it only if you are creating a new time series. It is ignored if you are adding samples to an existing time series. See `CHUNK_SIZE` in `TS.CREATE`.
</details>

<details open><summary><code>LABELS [{label value}...]</code></summary> 

is set of label-value pairs that represent metadata labels of the key and serve as a secondary index. Use it only if you are creating a new time series. It is ignored if you are adding samples to an existing time series. See `LABELS` in `TS.CREATE`.
</details>

<note><b>Notes</b>

 - You can use this command to add data to a nonexisting time series in a single command.
  This is why `RETENTION`, `UNCOMPRESSED`,  `CHUNK_SIZE`, and `LABELS` are optional arguments.
 - When specified and the key doesn't exist, a new time series is created.
  Setting the `RETENTION` and `LABELS` introduces additional time complexity.
</note>

## Examples

<details open><summary><b>Store sum of data from several sources</b></summary> 

Suppose you are getting number of orders or total income per minute from several points of sale, and you want to store only the combined value. Call TS.INCRBY for each point-of-sale report.

{{< highlight bash >}}
127.0.0.1:6379> TS.INCRBY a 232 TIMESTAMP 1657811829000		// point-of-sale #1
(integer) 1657811829000
127.0.0.1:6379> TS.INCRBY a 157 TIMESTAMP 1657811829000		// point-of-sale #2
(integer) 1657811829000
127.0.0.1:6379> TS.INCRBY a 432 TIMESTAMP 1657811829000		// point-of-sale #3
(integer) 1657811829000
{{< / highlight >}}

Note that the timestamps must arrive in non-decreasing order.

{{< highlight bash >}}
127.0.0.1:6379> ts.incrby a 100 TIMESTAMP 50
(error) TSDB: for incrby/decrby, timestamp should be newer than the lastest one
{{< / highlight >}}

You can achieve similar results without such protection using `TS.ADD key timestamp value ON_DUPLICATE sum`.
</details>

<details open><summary><b>Count sensor captures</b></summary>

Supose a sensor ticks whenever a car is passed on a road, and you want to count occurrences. Whenever you get a tick from the sensor you can simply call:

{{< highlight bash >}}
127.0.0.1:6379> TS.INCRBY a 1
(integer) 1658431553109
{{< / highlight >}}

The timestamp is filled automatically. 
</details>

## See also

`TS.DECRBY` | `TS.CREATE` 

## Related topics

[RedisTimeSeries](/docs/stack/timeseries)
