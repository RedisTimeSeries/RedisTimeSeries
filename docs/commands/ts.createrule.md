---
syntax: 
---

Create a compaction rule

## Syntax

{{< highlight bash >}}
TS.CREATERULE sourceKey destKey AGGREGATION aggregator bucketDuration [alignTimestamp]
{{< / highlight >}}

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

## See also

`TS.DELETERULE` 

## Related topics

[RedisTimeSeries](/docs/stack/timeseries)