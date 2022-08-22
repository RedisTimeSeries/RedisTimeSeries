---
syntax: 
---

Update the retention, chunk size, duplicate policy, and labels of an existing time series

## Syntax

{{< highlight bash >}}
TS.ALTER key [RETENTION retentionPeriod] [CHUNK_SIZE size] [DUPLICATE_POLICY policy] [LABELS [{label value}...]]
{{< / highlight >}}

[Examples](#examples)

## Required arguments

<details open><summary><code>key</code></summary> 

is key name for the time series.
</details>

<note><b>Note:</b> This command alters only the specified element. For example, if you specify only `RETENTION` and `LABELS`, the chunk size and the duplicate policy are not altered. </note>

## Optional arguments

<details open><summary><code>RETENTION retentionPeriod</code></summary>

is maximum retention period, compared to the maximum existing timestamp, in milliseconds. See `RETENTION` in `TS.CREATE`.
</details>

<details open><summary><code>CHUNK_SIZE size</code></summary> 

The initial allocation size, in bytes, for the data part of each new chunk. Actual chunks may consume more memory. See `CHUNK_SIZE` in `TS.CREATE`. Changing this value does not affect existing chunks.
</details>

<details open><summary><code>DUPLICATE_POLICY policy</code></summary> 

is policy for handling multiple samples with identical timestamps. See `DUPLICATE_POLICY` in `TS.CREATE`.
</details>

<details open><summary><code>LABELS [{label value}...]</code></summary> 

is set of label-value pairs that represent metadata labels of the key and serve as a secondary index.

If `LABELS` is specified, the given label list is applied. Labels that are not present in the given list are removed implicitly. Specifying `LABELS` with no label-value pairs removes all existing labels. See `LABELS` in `TS.CREATE`.
</details>

## Examples

<details open><summary><b>Alter a temperature time series</b></summary>

Create a temperature time series.

{{< highlight bash >}}
127.0.0.1:6379> TS.CREATE temperature:2:32 RETENTION 60000 DUPLICATE_POLICY MAX LABELS sensor_id 2 area_id 32
OK
{{< / highlight >}}

Alter the labels in the time series.

{{< highlight bash >}}
127.0.0.1:6379> TS.ALTER temperature:2:32 LABELS sensor_id 2 area_id 32 sub_area_id 15
OK
{{< / highlight >}}
</details>

## See also

`TS.CREATE` 

## Related topics

[RedisTimeSeries](/docs/stack/timeseries)
