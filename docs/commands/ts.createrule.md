### TS.CREATERULE

Create a compaction rule.

```sql
TS.CREATERULE sourceKey destKey AGGREGATION aggregationType bucketDuration
```

- _sourceKey_ - Key name for source time series
- _destKey_ - Key name for destination (compacted) time series
- AGGREGATION _aggregationType_ _bucketDuration_
  - _aggregationType_ - Aggregation type: One of the following:
    | type    | description                                         |
    | ------- | --------------------------------------------------- |
    | `avg`   | arithmetic mean of all values                       |
    | `sum`   | sum of all values                                   |
    | `min`   | minimum value                                       |
    | `max`   | maximum value                                       |
    | `range` | difference between the highest and the lowest value |
    | `count` | number of values                                    |
    | `first` | the value with the lowest timestamp in the bucket   |
    | `last`  | the value with the highest timestamp in the bucket  |
    | `std.p` | population standard deviation of the values         |
    | `std.s` | sample standard deviation of the values             |
    | `var.p` | population variance of the values                   |
    | `var.s` | sample variance of the values                       |
  - _bucketDuration_ - Time bucket for aggregation in milliseconds

  The alignment of time buckets is 0.

DEST_KEY should be of a `timeseries` type, and should be created before TS.CREATERULE is called.

!!! info "Note on existing samples in the source time series"
        
        Currently, only new samples that are added into the source series after the creation of the rule will be aggregated.
