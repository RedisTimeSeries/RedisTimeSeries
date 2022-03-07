### TS.CREATERULE

Create a compaction rule.

```sql
TS.CREATERULE sourceKey destKey AGGREGATION aggregationType bucketDuration
```

* sourceKey - Key name for source time series
* destKey - Key name for destination time series
* aggregationType - Aggregation type: avg, sum, min, max, range, count, first, last, std.p, std.s, var.p, var.s
* bucketDuration - Time bucket for aggregation in milliseconds
* The alignment of the Time buckets is 0.

DEST_KEY should be of a `timeseries` type, and should be created before TS.CREATERULE is called.

!!! info "Note on existing samples in the source time series"
        
        Currently, only new samples that are added into the source series after creation of the rule will be aggregated.
