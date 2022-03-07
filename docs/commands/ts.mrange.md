### TS.MRANGE

Query a range across multiple time-series by filters in forward direction.

```sql
TS.MRANGE fromTimestamp toTimestamp
          [FILTER_BY_TS TS1 TS2 ..]
          [FILTER_BY_VALUE min max]
          [WITHLABELS | SELECTED_LABELS label1 ..]
          [COUNT count] [ALIGN value]
          [AGGREGATION aggregationType bucketDuration]
          FILTER filter..
          [GROUPBY <label> REDUCE <reducer>]
```

* fromTimestamp - Start timestamp for the range query. `-` can be used to express the minimum possible timestamp (0).
* toTimestamp - End timestamp for range query, `+` can be used to express the maximum possible timestamp.
* filter - [See Filtering](#filtering)

Optional parameters:

* FILTER_BY_TS - Followed by a list of timestamps to filter the result by specific timestamps
* FILTER_BY_VALUE - Filter result by value using minimum and maximum.

* WITHLABELS - Include in the reply the label-value pairs that represent metadata labels of the time series. If `WITHLABELS` or `SELECTED_LABELS` are not set, by default, an empty Array will be replied on the labels array position.

* SELECTED_LABELS - Include in the reply a subset of the label-value pairs that represent metadata labels of the time series. This is usefull when you have a large number of labels per serie but are only interested in the value of some of the labels. If `WITHLABELS` or `SELECTED_LABELS` are not set, by default, an empty Array will be replied on the labels array position.

* COUNT - Maximum number of returned samples per time series.

* ALIGN - Time bucket alignment control for AGGREGATION. This will control the time bucket timestamps by changing the reference timestamp on which a bucket is defined.
     Possible values:
     * `start` or `-`: The reference timestamp will be the query start interval time (`fromTimestamp`).
     * `end` or `+`: The reference timestamp will be the query end interval time (`toTimestamp`).
     * A specific timestamp: align the reference timestamp to a specific time.
     * **Note:** when not provided alignment is set to `0`.

* AGGREGATION - Aggregate result into time buckets (the following aggregation parameters are mandtory)
    * aggregationType - Aggregation type: avg, sum, min, max, range, count, first, last, std.p, std.s, var.p, var.s
    * bucketDuration - Time bucket duration for aggregation in milliseconds.

* GROUPBY - Aggregate results across different time series, grouped by the provided label name.
  When combined with `AGGREGATION` the groupby/reduce is applied post aggregation stage.
    * label - label name to group series by.  A new series for each value will be produced.
    * reducer - Reducer type used to aggregate series that share the same label value. Available reducers: sum, min, max.
    * **Note:** The resulting series will contain 2 labels with the following label array structure:
         * `__reducer__=<reducer>` : containing the used reducer.
         * `__source__=key1,key2,key3` : containing the source time series used to compute the grouped serie.
    * **note** The produced series will be named `<label>=<groupbyvalue>`


#### Return Value

Array-reply, specifically:

The command returns the entries with labels matching the specified filter.
The returned entries are complete, that means that the name, labels and all the samples that match the range are returned.

The returned array will contain key1,labels1,lastsample1,...,keyN,labelsN,lastsampleN, with labels and lastsample being also of array data types. By default, the labels array will be an empty Array for each of the returned time series.

If the `WITHLABELS` or `SELECTED_LABELS` option is specified the labels Array will be filled with label-value pairs that represent metadata labels of the time series.


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