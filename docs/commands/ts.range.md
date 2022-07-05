---
syntax: 
---

Query a range in forward direction.

```
TS.RANGE key fromTimestamp toTimestamp
         [LATEST]
         [FILTER_BY_TS ts...]
         [FILTER_BY_VALUE min max]
         [COUNT count] 
         [[ALIGN value] AGGREGATION aggregator bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]
```

## Required arguments

`key` is the key name for the time series.

`fromTimestamp` is start timestamp for the range query. Use `-` to express the minimum possible timestamp (0).

`toTimestamp` is end timestamp for the range query. Use `+` to express the maximum possible timestamp.
    
    > **NOTE:** When the time series is a compaction, the last compacted value may aggregate raw values with timestamp beyond `toTimestamp`. That is because `toTimestamp` only limits the timestamp of the compacted value, which is the start time of the raw bucket that was compacted.

## Optional arguments

`LATEST` (since RedisTimeSeries v1.8), used when a time series is a compaction. With `LATEST`, TS.MRANGE also reports the compacted value of the latest possibly partial bucket, given that this bucket's start time falls within `[fromTimestamp, toTimestamp]`. Without `LATEST`, TS.MRANGE does not report the latest possibly partial bucket. When a time series is not a compaction, `LATEST` is ignored.
  
The data in the latest bucket of a compaction is possibly partial. A bucket is _closed_ and compacted only upon arrival of a new sample that _opens_ a new _latest_ bucket. There are cases, however, when the compacted value of the latest possibly partial bucket is also required. In such a case, use `LATEST`.

`FILTER_BY_TS ts...` (since RedisTimeSeries v1.6) followed by a list of timestamps filters results by specific timestamps.

`FILTER_BY_VALUE min max` (since RedisTimeSeries v1.6) filters results by minimum and maximum values.

`COUNT count` limits the number of returned samples.

`ALIGN value` (since RedisTimeSeries v1.6) is a time bucket alignment control for `AGGREGATION`. 
It controls the time bucket timestamps by changing the reference timestamp on which a bucket is defined. 
Values include:
   
 - `start` or `-`: The reference timestamp will be the query start interval time (`fromTimestamp`) which can't be `-`
 - `end` or `+`: The reference timestamp will be the query end interval time (`toTimestamp`) which can't be `+`
 - A specific timestamp: align the reference timestamp to a specific time
   
> **NOTE:** When not provided, alignment is set to `0`.

`AGGREGATION aggregator bucketDuration` aggregates results into time buckets, where:

  - `aggregator` takes one of the following aggregation types:

    | `aggregator` | Description                                                      |
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

`[BUCKETTIMESTAMP bt]` (since RedisTimeSeries v1.8) controls how bucket timestamps are reported.

| `bt`         | Description                                                |
| ------------ | ---------------------------------------------------------- |
| `-` or `low` | Timestamp is the start time (default)                      |
| `+` or `high`| Timestamp is the end time                                  |
| `~` or `mid` | Timestamp is the mid time (rounded down if not an integer) |

`[EMPTY]` (since RedisTimeSeries v1.8) is a flag, which, when specified, reports aggregations for empty buckets.

| `aggregator`         | Value reported for each empty bucket |
| -------------------- | ------------------------------------ |
| `sum`, `count`       | `0`                                  |
| `min`, `max`, `range`, `avg` | Based on linear interpolation of the last value before the bucket’s start time and the first value on or after the bucket’s end tim, calculates the min/max/range/avg within the bucket. Returns `NaN` if no values exist before or after the bucket.       |
| `first`              | Last value before the bucket’s start time. Returns `NaN` if no such value exists.     |
| `last`               | The first value on or after the bucket’s end time. Returns NaN if no such value exists. |
| `std.p`, `std.s`         | `NaN` |
| `twa` | Based on linear interpolation or extrapolation. Returns `NaN` when it cannot interpolate or extrapolate. |

Regardless of the values of `fromTimestamp` and `toTimestamp`, no data is reported for buckets that end before the oldest available raw sample, or begin after the newest available raw sample.

## Complexity

TS.RANGE complexity is `O(n/m+k)`, where:

  - `n` is number of data points.
  - `m` is chunk size (data points per chunk).
  - `k` is number of data points that are in the requested range

This can be improved in the future by using binary search to find the start of the range, which makes this `O(Log(n/m)+k*m)`.
But, because `m` is small, you can disregard it and look at the operation as O(Log(n)+k).

## Examples

### Aggregated query 

```
127.0.0.1:6379> TS.RANGE temperature:3:32 1548149180000 1548149210000 AGGREGATION avg 5000
1) 1) (integer) 1548149180000
   2) "26.199999999999999"
2) 1) (integer) 1548149185000
   2) "27.399999999999999"
3) 1) (integer) 1548149190000
   2) "24.800000000000001"
4) 1) (integer) 1548149195000
   2) "23.199999999999999"
5) 1) (integer) 1548149200000
   2) "25.199999999999999"
6) 1) (integer) 1548149205000
   2) "28"
7) 1) (integer) 1548149210000
   2) "20"
```

## See also

`TS.MRANGE` | `TS.REVRANGE` | `TS.MREVRANGE`

## Related topics

[RedisTimeSeries](/docs/stack/timeseries)