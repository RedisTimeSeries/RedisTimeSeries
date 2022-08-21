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

[Examples](#examples)

## Required arguments

<details open>
<summary><code>fromTimestamp</code></summary>
          
is start timestamp for the range query. Use `-` to denote the timestamp of the earliest sample in the time series.
          
</details>

<details open>
<summary><code>toTimestamp</code></summary>

is end timestamp for range query. Use `+` to denote the timestamp of the latest sample in the time series.

</details>

<details open>
<summary><code>FILTER filter..</code></summary>

filters time series based on their labels and label values, with these options:

  - `label=value`, where `label` equals `value`
  - `label!=value`, where `label` does not equal `value`
  - `label=`, where `key` does not have label `label`
  - `label!=`, where `key` has label `label`
  - `label=(_value1_,_value2_,...)`, where `key` with label `label` equals one of the values in the list
  - `label!=(value1,value2,...)`, where key with label `label` does not equal any of the values in the list

<note><b>Notes:</b> 
   - When using filters, apply a minimum of one `label=value` filter.
   - Filters are conjunctive. For example, the FILTER `type=temperature room=study` means the a time series is a temperature time series of a study room.
   </note>
</details>

## Optional arguments

<details open>
<summary><code>LATEST</code> (since RedisTimeSeries v1.8)</summary>

is used when a time series is a compaction. With `LATEST`, TS.MRANGE also reports the compacted value of the latest possibly partial bucket, given that this bucket's start time falls within `[fromTimestamp, toTimestamp]`. Without `LATEST`, TS.MRANGE does not report the latest possibly partial bucket. When a time series is not a compaction, `LATEST` is ignored.

The data in the latest bucket of a compaction is possibly partial. A bucket is _closed_ and compacted only upon arrival of a new sample that _opens_ a new _latest_ bucket. There are cases, however, when the compacted value of the latest possibly partial bucket is also required. In such a case, use `LATEST`.
</details>

<details open>
<summary><code>FILTER_BY_TS ts...</code> (since RedisTimeSeries v1.6)</summary>

filters results by a list of specific timestamps. For each specified timestamp, a result is reported if the timestamp falls within `[fromTimestamp, toTimestamp]` and there is a sample with that exact timestamp.

</details>

<details open>
<summary><code>FILTER_BY_VALUE min max</code> (since RedisTimeSeries v1.6)</summary>

filters results by minimum and maximum values.

</details>

<details open>
<summary><code>WITHLABELS</code></summary>

includes in the reply all label-value pairs representing metadata labels of the time series. 
If `WITHLABELS` or `SELECTED_LABELS` are not specified, by default, an empty list is reported as label-value pairs.

</details>

<details open>
<summary><code>SELECTED_LABELS label...</code> (since RedisTimeSeries v1.6)</summary>

returns a subset of the label-value pairs that represent metadata labels of the time series. 
Use when a large number of labels exists per series, but only the values of some of the labels are required. 
If `WITHLABELS` or `SELECTED_LABELS` are not specified, by default, an empty list is reported as label-value pairs.

</details>

<details open>
<summary><code>COUNT count</code></summary>

limits the number of returned samples.

</details>

<details open>
<summary><code>ALIGN value</code> (since RedisTimeSeries v1.6)</summary>

is a time bucket alignment control for `AGGREGATION`. 
It controls the time bucket timestamps by changing the reference timestamp on which a bucket is defined. 

Values include:
   
 - `start` or `-`: The reference timestamp will be the query start interval time (`fromTimestamp`) which can't be `-`
 - `end` or `+`: The reference timestamp will be the query end interval time (`toTimestamp`) which can't be `+`
 - A specific timestamp: align the reference timestamp to a specific time
   
<note><b>Note:</b> When not provided, alignment is set to `0`.</note>
</details>

<details open>
<summary><code>AGGREGATION aggregator bucketDuration</code></summary>

aggregates results into time buckets, where:

  - `aggregator` takes one of the following aggregation types:

    | `aggregator` &nbsp; &nbsp; &nbsp; | Description                                                      |
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
</details>

<details open>
<summary><code>[BUCKETTIMESTAMP bt]></code> (since RedisTimeSeries v1.8)</summary>

controls how bucket timestamps are reported.

| `bt`         | Description                                                |
| ------------ | ---------------------------------------------------------- |
| `-` or `low` | Timestamp is the start time (default)                      |
| `+` or `high` &nbsp; &nbsp; &nbsp;| Timestamp is the end time                                  |
| `~` or `mid` | Timestamp is the mid time (rounded down if not an integer) |
</details>

<details open>
<summary><code>[EMPTY]</code> (since RedisTimeSeries v1.8)</summary>

is a flag, which, when specified, reports aggregations for empty buckets.

| `aggregator`         | Value reported for each empty bucket |
| -------------------- | ------------------------------------ |
| `sum`, `count`       | `0`                                  |
| `min`, `max`, `range`, `avg` &nbsp; &nbsp; &nbsp; | Based on linear interpolation of the last value before the bucket’s start time and the first value on or after the bucket’s end time, calculates the min/max/range/avg within the bucket. Returns `NaN` if no values exist before or after the bucket.       |
| `first`              | Last value before the bucket’s start time. Returns `NaN` if no such value exists.     |
| `last`               | The first value on or after the bucket’s end time. Returns NaN if no such value exists. |
| `std.p`, `std.s`         | `NaN` |
| `twa` | Based on linear interpolation or extrapolation. Returns `NaN` when it cannot interpolate or extrapolate. |

Regardless of the values of fromTimestamp and toTimestamp, no data is reported for buckets that end before the oldest available raw sample, or begin after the newest available raw sample.

</details>

<details open>
<summary><code>GROUPBY label REDUCE reducer</code> (since RedisTimeSeries v1.6)</summary>

aggregates results across different time series, grouped by the provided label name. 
When combined with `AGGREGATION` the groupby/reduce is applied post aggregation stage.

  - `label` is label name to group a series by. A new series for each value is produced.

  - `reducer` is reducer type used to aggregate series that share the same label value.

    | `reducer` | Description                         |
    | --------- | ----------------------------------- |
    | `avg`     | per label value: arithmetic mean of all values (since RedisTimeSeries v1.8)  |
    | `sum`     | per label value: sum of all values  |
    | `min`     | per label value: minimum value      |
    | `max`     | per label value: maximum value      |
    | `range` &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;  | per label value: difference between the highest and the lowest value (since RedisTimeSeries v1.8) |
    | `count`   | per label value: number of values (since RedisTimeSeries v1.8) |
    | `std.p`   | per label value: population standard deviation of the values (since RedisTimeSeries v1.8) |
    | `std.s`   | per label value: sample standard deviation of the values (since RedisTimeSeries v1.8) |
    | `var.p`   | per label value: population variance of the values (since RedisTimeSeries v1.8) |
    | `var.s`   | per label value: sample variance of the values (since RedisTimeSeries v1.8) |

<note><b>Notes:</b> 
  - The produced time series is named `<label>=<groupbyvalue>`
  - The produced time series contains two labels with these label array structures:
    - `reducer`, the reducer used
    - `source`, the time series keys used to compute the grouped series (`key1,key2,key3,...`)
</note>
</details>

## Return value

For each time series matching the specified filters, the following is reported:
- The key name
- A list of label-value pairs
  - By default, an empty list is reported
  - If `WITHLABELS` is specified, all labels associated with this time series are reported
  - If `SELECTED_LABELS label...` is specified, the selected labels are reported
- Timestamp-value pairs for all samples/aggregations matching the range

<note><b>Note:</b> The `MRANGE` command cannot be part of transaction when running on a Redis cluster.</note>

## Examples

<details open>
<summary><b>Retrieve maximum stock price per timestamp</b></summary>

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
</details>

<details open>
<summary><b>Calculate average stock price and retrieve maximum average</b></summary> 

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
</details>

<details open>
<summary><b>Group query results</b></summary>

Query all time series with the metric label equal to `cpu`, then group the time series by the value of their `metric_name` label value and for each group return the maximum value and the time series keys (_source_) with that value.

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
</details>

<details open>
<summary><b>Filter query by value</b></summary>

Query all time series with the metric label equal to `cpu`, then filter values larger or equal to 90.0 and smaller or equal to 100.0.

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
</details>

<details open>
<summary><b>Query using a label</b></summary>

Query all time series with the metric label equal to `cpu`, but only return the team label.

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
</details>

## See also

`TS.RANGE` | `TS.MREVRANGE` | `TS.REVRANGE`

## Related topics

[RedisTimeSeries](/docs/stack/timeseries)
