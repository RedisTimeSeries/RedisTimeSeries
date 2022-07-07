---
syntax: 
---

Query a range in forward direction.

{{< highlight bash >}}
TS.RANGE key fromTimestamp toTimestamp
         [LATEST]
         [FILTER_BY_TS ts...]
         [FILTER_BY_VALUE min max]
         [COUNT count] 
         [[ALIGN value] AGGREGATION aggregator bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]
{{< / highlight >}}

[**Examples**](#examples)

## Required arguments

`key` is the key name for the time series.

`fromTimestamp` is start timestamp for the range query. Use `-` to express the minimum possible timestamp (0).

`toTimestamp` is end timestamp for the range query. Use `+` to express the maximum possible timestamp.
    
    > **NOTE:** When the time series is a compaction, the last compacted value may aggregate raw values with timestamp beyond `toTimestamp`. That is because `toTimestamp` only limits the timestamp of the compacted value, which is the start time of the raw bucket that was compacted.

## Optional arguments

`LATEST` (since RedisTimeSeries v1.8), used when a time series is a compaction. With `LATEST`, TS.RANGE also reports the compacted value of the latest possibly partial bucket, given that this bucket's start time falls within `[fromTimestamp, toTimestamp]`. Without `LATEST`, TS.RANGE does not report the latest possibly partial bucket. When a time series is not a compaction, `LATEST` is ignored.
  
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

### Filter results by timestamp or sample value

Consider a metric where acceptable values are between -100 and 100, and the value 9999 is used as an indication of bad measurement.

{{< highlight bash >}}
127.0.0.1:6379> TS.CREATE temp:TLV LABELS type temp location TLV
OK
127.0.0.1:6379> TS.MADD temp:TLV 1000 30 temp:TLV 1010 35 temp:TLV 1020 9999 temp:TLV 1030 40
1) (integer) 1000
2) (integer) 1010
3) (integer) 1020
4) (integer) 1030
{{< / highlight >}}

Now, retrieve all values except out-of-range values.

{{< highlight bash >}}
TS.RANGE temp:TLV - + FILTER_BY_VALUE -100 100
1) 1) (integer) 1000
   2) 30
2) 1) (integer) 1010
   2) 35
3) 1) (integer) 1030
   2) 40
{{< / highlight >}}

Now, retrieve the average value, while ignoring out-of-range values.

{{< highlight bash >}}
TS.RANGE temp:TLV - + FILTER_BY_VALUE -100 100 AGGREGATION avg 1000
1) 1) (integer) 1000
   2) 35
{{< / highlight >}}

### Align aggregation buckets

To demonstrate alignment, let’s create a stock and add prices at three different timestamps.

{{< highlight bash >}}
127.0.0.1:6379> TS.CREATE stock:A LABELS type stock name A
OK
127.0.0.1:6379> TS.MADD stock:A 1000 100 stock:A 1010 110 stock:A 1020 120
1) (integer) 1000
2) (integer) 1010
3) (integer) 1020
127.0.0.1:6379> TS.MADD stock:A 2000 200 stock:A 2010 210 stock:A 2020 220
1) (integer) 2000
2) (integer) 2010
3) (integer) 2020
127.0.0.1:6379> TS.MADD stock:A 3000 300 stock:A 3010 310 stock:A 3020 320
1) (integer) 3000
2) (integer) 3010
3) (integer) 3020
{{< / highlight >}}

Next, aggregate without using `ALIGN`, defaulting to alignment 0.

{{< highlight bash >}}
127.0.0.1:6379> TS.RANGE stock:A - + AGGREGATION min 20
1) 1) (integer) 1000
   2) 100
2) 1) (integer) 1020
   2) 120
3) 1) (integer) 1040
   2) 210
4) 1) (integer) 1060
   2) 300
5) 1) (integer) 1080
   2) 320
{{< / highlight >}}

And now set `ALIGN` to 10 to have a bucket start at time 10, and align all the buckets with a 20 milliseconds duration.

{{< highlight bash >}}
127.0.0.1:6379> TS.RANGE stock:A - + ALIGN 10 AGGREGATION min 20
1) 1) (integer) 990
   2) 100
2) 1) (integer) 1010
   2) 110
3) 1) (integer) 1990
   2) 200
4) 1) (integer) 2010
   2) 210
5) 1) (integer) 2990
   2) 300
6) 1) (integer) 3010
   2) 310
{{< / highlight >}}

When the start timestamp for the range query is explicitly stated (not `-`), you can set ALIGN to that time by setting align to `-` or to `start`.

{{< highlight bash >}}
127.0.0.1:6379> TS.RANGE stock:A 5 + ALIGN - AGGREGATION min 20
1) 1) (integer) 985
   2) 100
2) 1) (integer) 1005
   2) 110
3) 1) (integer) 1985
   2) 200
4) 1) (integer) 2005
   2) 210
5) 1) (integer) 2985
   2) 300
6) 1) (integer) 3005
   2) 310
{{< / highlight >}}

Similarly, when the end timestamp for the range query is explicitly stated, you can set ALIGN to that time by setting align to `+` or to `end`.

## See also

`TS.MRANGE` | `TS.REVRANGE` | `TS.MREVRANGE`

## Related topics

[RedisTimeSeries](/docs/stack/timeseries)