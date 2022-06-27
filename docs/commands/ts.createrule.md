### TS.CREATERULE

Create a compaction rule.

```sql
TS.CREATERULE sourceKey destKey AGGREGATION aggregator bucketDuration [alignTimestamp]
```

- _sourceKey_ - Key name for source time series
- _destKey_ - Key name for destination (compacted) time series
- `AGGREGATION` _aggregator_ _bucketDuration_

   Aggregate results into time buckets.
  - _aggregator_ - Aggregation type: One of the following:
    | aggregator | description                                                      |
    | ---------- | ---------------------------------------------------------------- |
    | `avg`      | arithmetic mean of all values                                    |
    | `sum`      | sum of all values                                                |
    | `min`      | minimum value                                                    |
    | `max`      | maximum value                                                    |
    | `range`    | difference between the highest and the lowest value              |
    | `count`    | number of values                                                 |
    | `first`    | the value with the lowest timestamp in the bucket                |
    | `last`     | the value with the highest timestamp in the bucket               |
    | `std.p`    | population standard deviation of the values                      |
    | `std.s`    | sample standard deviation of the values                          |
    | `var.p`    | population variance of the values                                |
    | `var.s`    | sample variance of the values                                    |
    | `twa`      | time-weighted average of all values (since RedisTimeSeries v1.8) |
    
  - _bucketDuration_ - Duration of each bucket, in milliseconds

  - _alignTimestamp_ (since RedisTimeSeries v1.8)

    Assure that there is a bucket that starts at exactly _alignTimestamp_ and align all other buckets accordingly. Units: milliseconds. Default value: 0 (aligned with the epoch). Example: if _bucketDuration_ is 24 hours (24 * 3600 * 1000), setting _alignTimestamp_ to 6 hours after the epoch (6 * 3600 * 1000) will ensure that each bucket’s timeframe is [06:00 .. 06:00).

_destKey_ should be of a `timeseries` type, and should be created before `TS.CREATERULE` is called. 

Notes:

- Calling `TS.CREATERULE` with a nonempty _destKey_ can result in an undefined behavior
- Samples should not be explicitly added to _destKey_
- Only new samples that are added into the source series after the creation of the rule will be aggregated
- If no samples were added to the source time series during a bucket period - no 'compacted sample' would be added to the destination time series
- The timestamp of 'compacted samples' added to the  destination time series would be set to the start timestamp of each bucket (e.g., for 10-minutes compaction bucket with no alignment - the compacted samples timestamps would be x:00, x:10, x:20, ...). 

