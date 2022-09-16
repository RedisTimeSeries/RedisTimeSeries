---
syntax: 
---

Return information and statistics for a time series.

## Syntax

{{< highlight bash >}}
TS.INFO key 
  [DEBUG]
{{< / highlight >}}

[Examples](#examples)

## Required arguments

<details open>
<summary><code>key</code></summary> 
is key name of the time series.
</details>

## Optional arguments

<details open>
<summary><code>[DEBUG]</code></summary>

is an optional flag to get a more detailed information about the chunks.
</details>

## Return value

TS.INFO returns an array-reply with these elements:

| Name | Description
| ---- | -
| `totalSamples`    | Total number of samples in this time series
| `memoryUsage`     | Total number of bytes allocated for this time series, which is the sum of <br> - The memory used for storing the series' configuration parameters (retention period, duplication policy, etc.)<br>- The memory used for storing the series' compaction rules<br>- The memory used for storing the series' labels (key-value pairs)<br>- The memory used for storing the chunks (chunk header + compressed/uncompressed data)
| `firstTimestamp`  | First timestamp present in this time series
| `lastTimestamp`   | Last timestamp present in this time series
| `retentionTime`   | The retention period, in milliseconds, for this time series
| `chunkCount`      | Number of Memory Chunks used for this time series
| `chunkSize`       | The initial allocation size, in bytes, for the data part of each new chunk.<br>Actual chunks may consume more memory. Changing chunkSize (using `TS.ALTER`) does not affect existing chunks.
| `chunkType`       | The chunk type: `compressed` or `uncompressed`
| `duplicatePolicy` | The [duplicate policy](/docs/stack/timeseries/configuration/#duplicate_policy) of this time series
| `labels`          | A nested array of label-value pairs that represent the metadata labels of this time series
| `sourceKey`       | Key name for source time series in case the current series is a target of a [compaction rule](/commands/ts.createrule/)
| `rules`           | A nested array of the [compaction rules](/commands/ts.createrule/) defined in this time series, with these elements  for each rule:<br>- The compaction key<br>- The bucket duration<br>- The aggregator<br>- The alignment (since RedisTimeSeries v1.8)

When `DEBUG` is specified, the response contains an additional array field called `Chunks` with these elements:

| Name | Description
| ---- | -
| `startTimestamp`  | First timestamp present in the chunk
| `endTimestamp`    | Last timestamp present in the chunk
| `samples`         | Total number of samples in the chunk
| `size`            | The chunk data size in bytes. This is the exact size that used for data only inside the chunk. It does not include other overheads.
| `bytesPerSample`  | Ratio of `size` and `samples`

## Examples

<details open>
<summary><b>Find information about a temperature/humidity time series by location and sensor type</b></summary>

Create a set of sensors to measure temperature and humidity in your study and kitchen.

{{< highlight bash >}}
127.0.0.1:6379> TS.CREATE telemetry:study:temperature LABELS room study type temperature
OK
127.0.0.1:6379> TS.CREATE telemetry:study:humidity LABELS room study type humidity
OK
127.0.0.1:6379> TS.CREATE telemetry:kitchen:temperature LABELS room kitchen type temperature
OK
127.0.0.1:6379> TS.CREATE telemetry:kitchen:humidity LABELS room kitchen type humidity
OK
{{< / highlight >}}

Find information about the time series for temperature in the kitchen.

{{< highlight bash >}}
127.0.0.1:6379> TS.INFO telemetry:kitchen:temperature
 1) totalSamples
 2) (integer) 0
 3) memoryUsage
 4) (integer) 4246
 5) firstTimestamp
 6) (integer) 0
 7) lastTimestamp
 8) (integer) 0
 9) retentionTime
10) (integer) 0
11) chunkCount
12) (integer) 1
13) chunkSize
14) (integer) 4096
15) chunkType
16) compressed
17) duplicatePolicy
18) (nil)
19) labels
20) 1) 1) "room"
       2) "kitchen"
    2) 1) "type"
       2) "temperature"
21) sourceKey
22) (nil)
23) rules
24) (empty array)
{{< / highlight >}}

Query the time series using DEBUG to get more information about the chunks.

{{< highlight bash >}}
127.0.0.1:6379> TS.INFO telemetry:kitchen:temperature DEBUG
 1) totalSamples
 2) (integer) 0
 3) memoryUsage
 4) (integer) 4246
 5) firstTimestamp
 6) (integer) 0
 7) lastTimestamp
 8) (integer) 0
 9) retentionTime
10) (integer) 0
11) chunkCount
12) (integer) 1
13) chunkSize
14) (integer) 4096
15) chunkType
16) compressed
17) duplicatePolicy
18) (nil)
19) labels
20) 1) 1) "room"
       2) "kitchen"
    2) 1) "type"
       2) "temperature"
21) sourceKey
22) (nil)
23) rules
24) (empty array)
25) keySelfName
26) "telemetry:kitchen:temperature"
27) Chunks
28) 1)  1) startTimestamp
        2) (integer) 0
        3) endTimestamp
        4) (integer) 0
        5) samples
        6) (integer) 0
        7) size
        8) (integer) 4096
        9) bytesPerSample
       10) "inf"
{{< / highlight >}}

</details>

## See also

`TS.RANGE` | `TS.QUERYINDEX` | `TS.GET`

## Related topics

[RedisTimeSeries](/docs/stack/timeseries)
