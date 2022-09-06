---
syntax: 
---

Create a compaction rule

## Syntax

{{< highlight bash >}}
TS.CREATERULE sourceKey destKey 
  AGGREGATION aggregator bucketDuration 
  [alignTimestamp]
{{< / highlight >}}

[Examples](#examples)

## Required arguments

<details open><summary><code>sourceKey</code></summary>

is key name for the source time series.
</details>

<details open><summary><code>destKey</code></summary> 

is key name for destination (compacted) time series.
</details>

<details open><summary><code>AGGREGATION aggregator bucketDuration</code></summary> 

aggregates results into time buckets.

  - `aggregator` takes one of the following aggregation types:

    | `aggregator` &nbsp; &nbsp; &nbsp;  | Description                                                      |
    | ------------ | ---------------------------------------------------------------- |
    | `avg`        | Arithmetic mean of all values                                    |
    | `sum`        | Sum of all values                                                |
    | `min`        | Minimum value                                                    |
    | `max`        | Maximum value                                                    |
    | `range`      | Difference between the highest and the lowest value              |
    | `count`      | Number of values                                                 |
    | `first`      | Value with lowest timestamp in the bucket                        |
    | `last`       | Value with highest timestamp in the bucket                       |
    | `std.p`      | Population standard deviation of the values                      |
    | `std.s`      | Sample standard deviation of the values                          |
    | `var.p`      | Population variance of the values                                |
    | `var.s`      | Sample variance of the values                                    |
    | `twa`        | Time-weighted average of all values (since RedisTimeSeries v1.8) |

  - `bucketDuration` is duration of each bucket, in milliseconds.

## Optional arguments

<details open><summary><code>alignTimestamp</code> (since RedisTimeSeries v1.8)</summary> 

ensures that there is a bucket that starts exactly at `alignTimestamp` and aligns all other buckets accordingly. It is expressed in milliseconds. The default value is 0 aligned with the epoch. For example, if `bucketDuration` is 24 hours (`24 * 3600 * 1000`), setting `alignTimestamp` to 6 hours after the epoch (`6 * 3600 * 1000`) ensures that each bucket’s timeframe is `[06:00 .. 06:00)`.
</details>

<details open><summary><code>destKey</code></summary> 

is a `timeseries` type and is created before `TS.CREATERULE` is called. 
</details>

<note><b>Notes</b>

- Calling `TS.CREATERULE` with a nonempty `destKey` can result in an undefined behavior.
- Samples should not be explicitly added to `destKey`.
- Only new samples that are added into the source series after the creation of the rule will be aggregated
- If no samples are added to the source time series during a bucket period. no _compacted sample_ is added to the destination time series.
- The timestamp of a compacted sample added to the destination time series is set to the start timestamp the appropriate compaction bucket. For example, for a 10-minute compaction bucket with no alignment, the compacted samples timestamps are `x:00`, `x:10`, `x:20`, and so on.
</note>

## Examples

<details open>
<summary><b>Create a compaction rule</b></summary>

Create a time series to store the temperatures measured in Tel Aviv.

{{< highlight bash >}}
127.0.0.1:6379> TS.CREATE temp:TLV LABELS type temp location TLV
OK
{{< / highlight >}}

Next, create a compacted time series named _dailyAvgTemp_ containing one compacted sample per 24 hours: the time-weighted average of all measurements taken from midnight to next midnight.

{{< highlight bash >}}
127.0.0.1:6379> TS.CREATE dailyAvgTemp:TLV LABELS type temp location TLV
127.0.0.1:6379> TS.CREATERULE temp:TLV dailyAvgTemp:TLV AGGREGATION twa 86400000 
{{< / highlight >}}

Now, also create a compacted time series named _dailyDiffTemp_. This time series will contain one compacted sample per 24 hours: the difference between the minimum and the maximum temperature measured between 06:00 and 06:00 next day.
 Here, 86400000 is the number of milliseconds in 24 hours, 21600000 is the number of milliseconds in 6 hours.

{{< highlight bash >}}
127.0.0.1:6379> TS.CREATE dailyDiffTemp:TLV LABELS type temp location TLV
127.0.0.1:6379> TS.CREATERULE temp:TLV dailyDiffTemp:TLV AGGREGATION range 86400000 21600000
{{< / highlight >}}

## See also

`TS.DELETERULE` 

## Related topics

[RedisTimeSeries](/docs/stack/timeseries)