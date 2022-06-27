### TS.MREVRANGE

Query a range across multiple time series by filters in reverse direction.

```sql
TS.MREVRANGE fromTimestamp toTimestamp          
          [FILTER_BY_TS TS...]
          [FILTER_BY_VALUE min max]
          [WITHLABELS | SELECTED_LABELS label...]
          [COUNT count]
          [[ALIGN value] AGGREGATION aggregator bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]
          FILTER filter..
          [GROUPBY label REDUCE reducer]
```

#### Mandatory parameters

- _fromTimestamp_ - Start timestamp for the range query. `-` can be used to express the minimum possible timestamp (0).
- _toTimestamp_ - End timestamp for range query, `+` can be used to express the maximum possible timestamp.
- FILTER _filter_...

  This is the list of possible filters:
  - _label_`=`_value_ - _label_ equals _value_
  - _label_`!=`_value_ - label doesn't equal _value_
  - _label_`=` - _key_ does not have the label _label_
  - _label_`!=` - _key_ has label _label_
  - _label_`=(`_value1_`,`_value2_`,`...`)` - key with label _label_ that equals one of the values in the list
  - _lable_`!=(`_value1_`,`_value2_`,`...`)` - key with label _label_ that doesn't equal any of the values in the list

  Note: Whenever filters need to be provided, a minimum of one _label_`=`_value_ filter must be applied.

#### Optional parameters

- `FILTER_BY_TS` _ts_... (since RedisTimeSeries v1.6)

  Followed by a list of timestamps to filter the result by specific timestamps.

- `FILTER_BY_VALUE` _min_ _max_ (since RedisTimeSeries v1.6)

  Filter result by value using minimum and maximum.

- `WITHLABELS`

  Include in the reply all label-value pairs representing metadata labels of the time series. 

  If `WITHLABELS` or `SELECTED_LABELS` are not specified, by default, an empty list is reported as the label-value pairs.

- `SELECTED_LABELS` _label_... (since RedisTimeSeries v1.6)

  Include in the reply a subset of the label-value pairs that represent metadata labels of the time series. This is usefull when there is a large number of labels per series, but only the values of some of the labels are required.
 
  If `WITHLABELS` or `SELECTED_LABELS` are not specified, by default, an empty list is reported as the label-value pairs.

- `COUNT` _count_

  Maximum number of returned samples per time series.

- `ALIGN` _value_ (since RedisTimeSeries v1.6)

  Time bucket alignment control for AGGREGATION. This will control the time bucket timestamps by changing the reference timestamp on which a bucket is defined.
     Possible values:
     * `start` or `-`: The reference timestamp will be the query start interval time (`fromTimestamp`) which can't be `-`
     * `end` or `+`: The reference timestamp will be the query end interval time (`toTimestamp`) which can't be `+`
     * A specific timestamp: align the reference timestamp to a specific time
     * **Note:** when not provided, alignment is set to `0`

- `AGGREGATION` _aggregator_ _bucketDuration_

  Aggregate results into time buckets.
  - _aggregator_ - Aggregation type: One of the following:
    | _aggregator_ | description                                                      |
    | ------------ | ---------------------------------------------------------------- |
    | `avg`        | arithmetic mean of all values                                    |
    | `sum`        | sum of all values                                                |
    | `min`        | minimum value                                                    |
    | `max`        | maximum value                                                    |
    | `range`      | difference between the highest and the lowest value              |
    | `count`      | number of values                                                 |
    | `first`      | the value with the lowest timestamp in the bucket                |
    | `last`       | the value with the highest timestamp in the bucket               |
    | `std.p`      | population standard deviation of the values                      |
    | `std.s`      | sample standard deviation of the values                          |
    | `var.p`      | population variance of the values                                |
    | `var.s`      | sample variance of the values                                    |
    | `twa`        | time-weighted average of all values (since RedisTimeSeries v1.8) |
    
  - _bucketDuration_ - duration of each bucket, in milliseconds

  - [BUCKETTIMESTAMP _bt_] (since RedisTimeSeries v1.8)

    Controls how bucket timestamps are reported.

    | _bt_         | description                                                                             |
    | ------------ | --------------------------------------------------------------------------------------- |
    | `-` or `low` | The timestamp reported for each bucket is its start time (default)                      |
    | `+` or `high`| The timestamp reported for each bucket is its end time                                  |
    | `~` or `mid` | The timestamp reported for each bucket is its mid time (rounded down if not an integer) |

  - [EMPTY] (since RedisTimeSeries v1.8)

    When this flag is specified, aggregations are reported for empty buckets as well:

    | _aggregator_         | Value reported for each empty bucket |
    | -------------------- | ------------------------------------ |
    | sum, count           | 0                                    |
    | min, max, range, avg | Based on linear interpolation of the last value before the bucket’s start time and the first value on or after the bucket’s end time - calculate the min/max/range/avg within the bucket; NaN if there is no values before or after the bucket       |
    | first                | The last value before the bucket’s start time; NaN if there is no such value     |
    | last                 | The first value on or after the bucket’s end time; NaN if there is no such value |
    | std.p, std.s         | NaN                                                                              |
    | twa                  | Based on linear interpolation or extrapolation; NaN when cannot interpolate or extrapolate |

    Regardless of the values of fromTimestamp and toTimestamp, no data will be reported for buckets that end before the oldest available raw sample, or begin after the newest available raw sample.

- `GROUPBY` _label_ `REDUCE` _reducer_ (since RedisTimeSeries v1.6)

  Aggregate results across different time series, grouped by the provided label name.
  
  When combined with `AGGREGATION` the groupby/reduce is applied post aggregation stage.
    - _label_ - label name to group series by.  A new series for each value will be produced.
    - _reducer_ - Reducer type used to aggregate series that share the same label value. One of the following:
      | _reducer_ | description                                                                                       |
      | --------- | ------------------------------------------------------------------------------------------------- |
      | `avg`     | per label value: arithmetic mean of all values (since RedisTimeSeries v1.8)                       |
      | `sum`     | per label value: sum of all values                                                                |
      | `min`     | per label value: minimum value                                                                    |
      | `max`     | per label value: maximum value                                                                    |
      | `range`   | per label value: difference between the highest and the lowest value (since RedisTimeSeries v1.8) |
      | `count`   | per label value: number of values (since RedisTimeSeries v1.8)                                    |
      | `std.p`   | per label value: population standard deviation of the values (since RedisTimeSeries v1.8)         |
      | `std.s`   | per label value: sample standard deviation of the values (since RedisTimeSeries v1.8)             |
      | `var.p`   | per label value: population variance of the values (since RedisTimeSeries v1.8)                   |
      | `var.s`   | per label value: sample variance of the values (since RedisTimeSeries v1.8)                       |
    - **Note:** The produced time series will be named `<label>=<groupbyvalue>`
    - **Note:** The produced time series will contain 2 labels with the following label array structure:
         - `__reducer__` : the reducer used
         - `__source__` : the time series keys used to compute the grouped series ("key1,key2,key3,...")

#### Return Value

For each time series matching the specified filters, the following is reported:
- The key name
- A list of label-value pairs
  - By default, an empty list is reported
  - If `WITHLABELS` is specified, all labels associated with this time series are reported
  - If `SELECTED_LABELS` _label_... is specified, the selected labels are reported
- timestamp-value pairs for all samples/aggregations matching the range

Note: MREVRANGE command can't be part of transaction when running on Redis cluster.

#### Examples

##### Query by Filters Example
```sql
127.0.0.1:6379> TS.MRANGE 1548149180000 1548149210000 AGGREGATION avg 5000 FILTER area_id=32 sensor_id!=1
1) 1) "temperature:2:32"
   2) (empty list or set)
   3) 1) 1) (integer) 1548149180000
         2) "27.600000000000001"
      2) 1) (integer) 1548149185000
         2) "23.800000000000001"
      3) 1) (integer) 1548149190000
         2) "24.399999999999999"
      4) 1) (integer) 1548149195000
         2) "24"
      5) 1) (integer) 1548149200000
         2) "25.600000000000001"
      6) 1) (integer) 1548149205000
         2) "25.800000000000001"
      7) 1) (integer) 1548149210000
         2) "21"
2) 1) "temperature:3:32"
   2) (empty list or set)
   3) 1) 1) (integer) 1548149180000
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

##### Query by Filters Example with WITHLABELS option

```sql
127.0.0.1:6379> TS.MRANGE 1548149180000 1548149210000 AGGREGATION avg 5000 WITHLABELS FILTER area_id=32 sensor_id!=1
1) 1) "temperature:2:32"
   2) 1) 1) "sensor_id"
         2) "2"
      2) 1) "area_id"
         2) "32"
   3) 1) 1) (integer) 1548149180000
         2) "27.600000000000001"
      2) 1) (integer) 1548149185000
         2) "23.800000000000001"
      3) 1) (integer) 1548149190000
         2) "24.399999999999999"
      4) 1) (integer) 1548149195000
         2) "24"
      5) 1) (integer) 1548149200000
         2) "25.600000000000001"
      6) 1) (integer) 1548149205000
         2) "25.800000000000001"
      7) 1) (integer) 1548149210000
         2) "21"
2) 1) "temperature:3:32"
   2) 1) 1) "sensor_id"
         2) "3"
      2) 1) "area_id"
         2) "32"
   3) 1) 1) (integer) 1548149180000
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

##### Query time series with metric=cpu, group them by metric_name reduce max

```sql
127.0.0.1:6379> TS.ADD ts1 1548149180000 90 labels metric cpu metric_name system
(integer) 1
127.0.0.1:6379> TS.ADD ts1 1548149185000 45
(integer) 2
127.0.0.1:6379> TS.ADD ts2 1548149180000 99 labels metric cpu metric_name user
(integer) 2
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
```

##### Query time series with metric=cpu, filter values larger or equal to 90.0 and smaller or equal to 100.0

```sql
127.0.0.1:6379> TS.ADD ts1 1548149180000 90 labels metric cpu metric_name system
(integer) 1
127.0.0.1:6379> TS.ADD ts1 1548149185000 45
(integer) 2
127.0.0.1:6379> TS.ADD ts2 1548149180000 99 labels metric cpu metric_name user
(integer) 2
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
```


##### Query time series with metric=cpu, but only reply the team label

```sql
127.0.0.1:6379> TS.ADD ts1 1548149180000 90 labels metric cpu metric_name system team NY
(integer) 1
127.0.0.1:6379> TS.ADD ts1 1548149185000 45
(integer) 2
127.0.0.1:6379> TS.ADD ts2 1548149180000 99 labels metric cpu metric_name user team SF
(integer) 2
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
```
