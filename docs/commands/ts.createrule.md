### TS.CREATERULE

Create a compaction rule.

```sql
TS.CREATERULE sourceKey destKey AGGREGATION aggregator bucketDuration
```

- _sourceKey_ - Key name for source time series
- _destKey_ - Key name for destination (compacted) time series
- `AGGREGATION` _aggregator_ _bucketDuration_

   Aggregate results into time buckets.
  - _aggregator_ - Aggregation type: One of the following:
    | aggregator | description                                         |
    | ---------- | --------------------------------------------------- |
    | `avg`      | arithmetic mean of all values                       |
    | `sum`      | sum of all values                                   |
    | `min`      | minimum value                                       |
    | `max`      | maximum value                                       |
    | `range`    | difference between the highest and the lowest value |
    | `count`    | number of values                                    |
    | `first`    | the value with the lowest timestamp in the bucket   |
    | `last`     | the value with the highest timestamp in the bucket  |
    | `std.p`    | population standard deviation of the values         |
    | `std.s`    | sample standard deviation of the values             |
    | `var.p`    | population variance of the values                   |
    | `var.s`    | sample variance of the values                       |
    | `twa`      | time-weighted average of all values                 |
  - _bucketDuration_ - duration of each bucket, in milliseconds

  The alignment of time buckets is 0.

_destKey_ should be of a `timeseries` type, and should be created before `TS.CREATERULE` is called. 

Notes:

- Calling `TS.CREATERULE` with a nonempty _destKey_ can result in an undefined behavior
- Samples should not be explicitly added to _destKey_
- Only new samples that are added into the source series after the creation of the rule will be aggregated

