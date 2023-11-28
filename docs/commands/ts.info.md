---
syntax: |
  TS.INFO key 
    [DEBUG]
---

Return information and statistics for a time series.

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

@array-reply with information about the time series (name-value pairs):

| Name<br>@simple-string-reply | Description
| ---------------------------- | -
| `totalSamples`    | @integer-reply<br> Total number of samples in this time series
| `memoryUsage`     | @integer-reply<br> Total number of bytes allocated for this time series, which is the sum of <br> - The memory used for storing the series' configuration parameters (retention period, duplication policy, etc.)<br>- The memory used for storing the series' compaction rules<br>- The memory used for storing the series' labels (key-value pairs)<br>- The memory used for storing the chunks (chunk header + compressed/uncompressed data)
| `firstTimestamp`  | @integer-reply<br> First timestamp present in this time series (Unix timestamp in milliseconds)
| `lastTimestamp`   | @integer-reply<br> Last timestamp present in this time series  (Unix timestamp in milliseconds)
| `retentionTime`   | @integer-reply<br> The retention period, in milliseconds, for this time series
| `chunkCount`      | @integer-reply<br> Number of chunks used for this time series
| `chunkSize`       | @integer-reply<br> The initial allocation size, in bytes, for the data part of each new chunk.<br>Actual chunks may consume more memory. Changing the chunk size (using `TS.ALTER`) does not affect existing chunks.
| `chunkType`       | @simple-string-reply<br> The chunks type: `compressed` or `uncompressed`
| `duplicatePolicy` | @simple-string-reply or @nil-reply<br> The [duplicate policy](/docs/stack/timeseries/configuration/#duplicate_policy) of this time series
| `labels`          | @array-reply or @nil-reply<br> Metadata labels of this time series<br> Each element is a 2-elements @array-reply of (@bulk-string-reply, @bulk-string-reply) representing (label, value)
| `sourceKey`       | @bulk-string-reply or @nil-reply<br>Key name for source time series in case the current series is a target of a [compaction rule](/commands/ts.createrule/)
| `rules`           | @array-reply<br> [Compaction rules](/commands/ts.createrule/) defined in this time series<br> Each rule is an @array-reply with 4 elements:<br>- @bulk-string-reply: The compaction key<br>- @integer-reply: The bucket duration<br>- @simple-string-reply: The aggregator<br>- @integer-reply: The alignment (since RedisTimeSeries v1.8)

When `DEBUG` is specified, the response also contains:

| Name<br>@simple-string-reply | Description
| ---------------------------- | -
| `keySelfName`     | @bulk-string-reply<br> Name of the key
| `Chunks`          | @array-reply with information about the chunks<br>Each element is an @array-reply of information about a single chunk in a name(@simple-string-reply)-value pairs:<br>- `startTimestamp` - @integer-reply - First timestamp present in the chunk<br>- `endTimestamp` - @integer-reply - Last timestamp present in the chunk<br>- `samples` - @integer-reply - Total number of samples in the chunk<br>- `size` - @integer-reply - the chunk's internal data size (without overheads) in bytes<br>- `bytesPerSample` - @bulk-string-reply (double) - Ratio of `size` and `samples`

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
