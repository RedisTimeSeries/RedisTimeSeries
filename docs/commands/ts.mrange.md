---
syntax: 
---

Query a range across multiple time series by filters in forward direction

## Syntax

{{< highlight bash >}}
TS.MRANGE fromTimestamp toTimestamp
          [LATEST]
          [FILTER_BY_TS ts...]
          [FILTER_BY_VALUE min max]
          [WITHLABELS | SELECTED_LABELS label...]
          [COUNT count]
          [[ALIGN value] AGGREGATION aggregator bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]
          FILTER filter..
          [GROUPBY label REDUCE reducer]
{{< / highlight >}}

[**Examples**](#examples)

## Required arguments

`fromTimestamp` is start timestamp for the range query. Use `-` to express the minimum possible timestamp (0).

`toTimestamp` is end timestamp for range query. Use `+` to express the maximum possible timestamp.

`FILTER filter..` uses these filters:

  - `label = value`, where `label` equals `value`
  - `label != value`, where `label` does not equal `value`
  - `label = `, where `key` does not have label `label`
  - `label != `, where `key` has label `label`
  - `label = (_value1_,_value2_,...)`, where `key` with label `label` equals one of the values in the list
  - `label != (value1,value2,...)` is key with label `label` that does not equal any of the values in the list

  > **NOTE:** When using filters, apply a minimum of one `label = value` filter.

## Optional arguments

`LATEST` (since RedisTimeSeries v1.8), used when a time series is a compaction. With `LATEST`, TS.MRANGE also reports the compacted value of the latest possibly partial bucket, given that this bucket's start time falls within `[fromTimestamp, toTimestamp]`. Without `LATEST`, TS.MRANGE does not report the latest possibly partial bucket. When a time series is not a compaction, `LATEST` is ignored.
  
The data in the latest bucket of a compaction is possibly partial. A bucket is _closed_ and compacted only upon arrival of a new sample that _opens_ a new _latest_ bucket. There are cases, however, when the compacted value of the latest possibly partial bucket is also required. In such a case, use `LATEST`.

`FILTER_BY_TS ts...` (since RedisTimeSeries v1.6) followed by a list of timestamps filters results by specific timestamps.

`FILTER_BY_VALUE min max` (since RedisTimeSeries v1.6) filters results by minimum and maximum values.

`WITHLABELS` includes in the reply all label-value pairs representing metadata labels of the time series. 
If `WITHLABELS` or `SELECTED_LABELS` are not specified, by default, an empty list is reported as label-value pairs.

`SELECTED_LABELS label...` (since RedisTimeSeries v1.6) returns a subset of the label-value pairs that represent metadata labels of the time series. 
Use when a large number of labels exists per series, but only the values of some of the labels are required. 
If `WITHLABELS` or `SELECTED_LABELS` are not specified, by default, an empty list is reported as label-value pairs.

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
| `min`, `max`, `range`, `avg` | Based on linear interpolation of the last value before the bucket’s start time and the first value on or after the bucket’s end time, calculates the min/max/range/avg within the bucket. Returns `NaN` if no values exist before or after the bucket.       |
| `first`              | Last value before the bucket’s start time. Returns `NaN` if no such value exists.     |
| `last`               | The first value on or after the bucket’s end time. Returns NaN if no such value exists. |
| `std.p`, `std.s`         | `NaN` |
| `twa` | Based on linear interpolation or extrapolation. Returns `NaN` when it cannot interpolate or extrapolate. |

Regardless of the values of fromTimestamp and toTimestamp, no data is reported for buckets that end before the oldest available raw sample, or begin after the newest available raw sample.

`GROUPBY label REDUCE reducer` (since RedisTimeSeries v1.6) aggregates results across different time series, grouped by the provided label name. 
When combined with `AGGREGATION` the groupby/reduce is applied post aggregation stage.

  - `label` is label name to group a series by. A new series for each value is produced.

  - `reducer` is reducer type used to aggregate series that share the same label value.

    | `reducer` | Description                         |
    | --------- | ----------------------------------- |
    | `avg`     | per label value: arithmetic mean of all values (since RedisTimeSeries v1.8)  |
    | `sum`     | per label value: sum of all values  |
    | `min`     | per label value: minimum value      |
    | `max`     | per label value: maximum value      |
    | `range`   | per label value: difference between the highest and the lowest value (since RedisTimeSeries v1.8) |
    | `count`   | per label value: number of values (since RedisTimeSeries v1.8) |
    | `std.p`   | per label value: population standard deviation of the values (since RedisTimeSeries v1.8) |
    | `std.s`   | per label value: sample standard deviation of the values (since RedisTimeSeries v1.8) |
    | `var.p`   | per label value: population variance of the values (since RedisTimeSeries v1.8) |
    | `var.s`   | per label value: sample variance of the values (since RedisTimeSeries v1.8) |

> **NOTES:** 
  - The produced time series is named `<label>=<groupbyvalue>`
  - The produced time series contains two labels with these label array structures:
    - `reducer`, the reducer used
    - `source`, the time series keys used to compute the grouped series ("key1,key2,key3,...")

## Return value

For each time series matching the specified filters, the following is reported:
- The key name
- A list of label-value pairs
  - By default, an empty list is reported
  - If `WITHLABELS` is specified, all labels associated with this time series are reported
  - If `SELECTED_LABELS label...` is specified, the selected labels are reported
- Timestamp-value pairs for all samples/aggregations matching the range

> **NOTE:** The `MRANGE` command cannot be part of transaction when running on a Redis cluster.

## Examples

### Retrieve maximum stock price per timestamp

Create two stocks and add their prices at three different timestamps.

{{< highlight bash >}}
127.0.0.1:6379> TS.CREATE stock:A LABELS type stock name A
OK
127.0.0.1:6379> TS.CREATE stock:B LABELS type stock name B
OK
127.0.0.1:6379> TS.MADD stock:A 1000 100 stock:A 1010 110 stock:A 1020 120
1) (integer) 1000
2) (integer) 1010
3) (integer) 1020
127.0.0.1:6379> TS.MADD stock:B 1000 120 stock:B 1010 110 stock:B 1020 100
1) (integer) 1000
2) (integer) 1010
3) (integer) 1020
{{< / highlight >}}

You can now retrieve the maximum stock price per timestamp.

{{< highlight bash >}}
127.0.0.1:6379> TS.MRANGE - + WITHLABELS FILTER type=stock GROUPBY type REDUCE max
1) 1) "type=stock"
   2) 1) 1) "type"
         2) "stock"
      2) 1) "__reducer__"
         2) "max"
      3) 1) "__source__"
         2) "stock:A,stock:B"
   3) 1) 1) (integer) 1000
         2) 120
      2) 1) (integer) 1010
         2) 110
      3) 1) (integer) 1020
         2) 120
{{< / highlight >}}

The `FILTER type=stock` clause returns a single time series representing stock prices. The `GROUPBY type REDUCE max` clause splits the time series into groups with identical type values, and then, for each timestamp, aggregates all series that share the same type value using the max aggregator.

### Calculate average stock price and retrieve maximum average 

Create two stocks and add their prices at nine different timestamps.

{{< highlight bash >}}
127.0.0.1:6379> TS.CREATE stock:A LABELS type stock name A
OK
127.0.0.1:6379> TS.CREATE stock:B LABELS type stock name B
OK
127.0.0.1:6379> TS.MADD stock:A 1000 100 stock:A 1010 110 stock:A 1020 120
1) (integer) 1000
2) (integer) 1010
3) (integer) 1020
127.0.0.1:6379> TS.MADD stock:B 1000 120 stock:B 1010 110 stock:B 1020 100
1) (integer) 1000
2) (integer) 1010
3) (integer) 1020
127.0.0.1:6379> TS.MADD stock:A 2000 200 stock:A 2010 210 stock:A 2020 220
1) (integer) 2000
2) (integer) 2010
3) (integer) 2020
127.0.0.1:6379> TS.MADD stock:B 2000 220 stock:B 2010 210 stock:B 2020 200
1) (integer) 2000
2) (integer) 2010
3) (integer) 2020
127.0.0.1:6379> TS.MADD stock:A 3000 300 stock:A 3010 310 stock:A 3020 320
1) (integer) 3000
2) (integer) 3010
3) (integer) 3020
127.0.0.1:6379> TS.MADD stock:B 3000 320 stock:B 3010 310 stock:B 3020 300
1) (integer) 3000
2) (integer) 3010
3) (integer) 3020
{{< / highlight >}}

Now, for each stock, calculate the average stock price per a 1000-millisecond timeframe, and then retrieve the stock with the maximum average for that timeframe.

{{< highlight bash >}}
127.0.0.1:6379> TS.MRANGE - + WITHLABELS AGGREGATION avg 1000 FILTER type=stock GROUPBY type REDUCE max
1) 1) "type=stock"
   2) 1) 1) "type"
         2) "stock"
      2) 1) "__reducer__"
         2) "max"
      3) 1) "__source__"
         2) "stock:A,stock:B"
   3) 1) 1) (integer) 1000
         2) 110
      2) 1) (integer) 2000
         2) 210
      3) 1) (integer) 3000
         2) 310
{{< / highlight >}}

### Group query results

Query a time series using `metric=cpu`, then group results by `metric_name REDUCE max`.

{{< highlight bash >}}
127.0.0.1:6379> TS.ADD ts1 1548149180000 90 labels metric cpu metric_name system
(integer) 1548149180000
127.0.0.1:6379> TS.ADD ts1 1548149185000 45
(integer) 1548149185000
127.0.0.1:6379> TS.ADD ts2 1548149180000 99 labels metric cpu metric_name user
(integer) 1548149180000
127.0.0.1:6379> TS.MRANGE - + WITHLABELS FILTER metric=cpu GROUPBY metric_name REDUCE max
1) 1) "metric_name=system"
   2) 1) 1) "metric_name"
         2) "system"
      2) 1) "__reducer__"
         2) "max"
      3) 1) "__source__"
         2) "ts1"
   3) 1) 1) (integer) 1548149180000
         2) 90
      2) 1) (integer) 1548149185000
         2) 45
2) 1) "metric_name=user"
   2) 1) 1) "metric_name"
         2) "user"
      2) 1) "__reducer__"
         2) "max"
      3) 1) "__source__"
         2) "ts2"
   3) 1) 1) (integer) 1548149180000
         2) 99
{{< / highlight >}}

### Filter query by value

Query a time series using `metric=cpu`, then filter values larger or equal to 90.0 and smaller or equal to 100.0.

{{< highlight bash >}}
127.0.0.1:6379> TS.ADD ts1 1548149180000 90 labels metric cpu metric_name system
(integer) 1548149180000
127.0.0.1:6379> TS.ADD ts1 1548149185000 45
(integer) 1548149185000
127.0.0.1:6379> TS.ADD ts2 1548149180000 99 labels metric cpu metric_name user
(integer) 1548149180000
127.0.0.1:6379> TS.MRANGE - + FILTER_BY_VALUE 90 100 WITHLABELS FILTER metric=cpu
1) 1) "ts1"
   2) 1) 1) "metric"
         2) "cpu"
      2) 1) "metric_name"
         2) "system"
   3) 1) 1) (integer) 1548149180000
         2) 90
2) 1) "ts2"
   2) 1) 1) "metric"
         2) "cpu"
      2) 1) "metric_name"
         2) "user"
   3) 1) 1) (integer) 1548149180000
         2) 99
{{< / highlight >}}

### Query using a label

Query a time series using `metric=cpu`, but only return the team label.

{{< highlight bash >}}
127.0.0.1:6379> TS.ADD ts1 1548149180000 90 labels metric cpu metric_name system team NY
(integer) 1548149180000
127.0.0.1:6379> TS.ADD ts1 1548149185000 45
(integer) 1548149185000
127.0.0.1:6379> TS.ADD ts2 1548149180000 99 labels metric cpu metric_name user team SF
(integer) 1548149180000
127.0.0.1:6379> TS.MRANGE - + SELECTED_LABELS team FILTER metric=cpu
1) 1) "ts1"
   2) 1) 1) "team"
         2) "NY"
   3) 1) 1) (integer) 1548149180000
         2) 90
      2) 1) (integer) 1548149185000
         2) 45
2) 1) "ts2"
   2) 1) 1) "team"
         2) "SF"
   3) 1) 1) (integer) 1548149180000
         2) 99
{{< / highlight >}}

## See also

`TS.RANGE` | `TS.MREVRANGE` | `TS.REVRANGE`

## Related topics

[RedisTimeSeries](/docs/stack/timeseries)
