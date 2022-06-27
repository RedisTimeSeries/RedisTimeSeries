### TS.REVRANGE

Query a range in reverse direction.

```sql
TS.REVRANGE key fromTimestamp toTimestamp
         [LATEST]
         [FILTER_BY_TS TS...]
         [FILTER_BY_VALUE min max]
         [COUNT count]
         [[ALIGN value] AGGREGATION aggregator bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]
```
## Arguments

#### Mandatory arguments

- _key_ - Key name for timeseries
- _fromTimestamp_ - Start timestamp for the range query. `-` can be used to express the minimum possible timestamp (0).
- _toTimestamp_ - End timestamp for range query, `+` can be used to express the maximum possible timestamp.

#### Optional arguments

- `LATEST` (since RedisTimeSeries v1.8)

  When the time series is a compaction: With `LATEST`, TS.REVRANGE will also report the compacted value of the latest (possibly partial) bucket (given that that bucket start times falls within [fromTimestamp, toTimestamp] and that the bucket is not empty). Without `LATEST`, TS.REVRANGE will not report the latest (possibly partial) bucket. When the series is not a compaction: `LATEST` is ignored.
  
  The data in the latest bucket of a compaction is possibly partial. A bucket is 'closed' and compacted only upon arrival of a new sample that 'opens' a 'new latest' bucket. There are cases, however, when the compacted value of the latest (possibly partial) bucket is also required. When so, `LATEST` can be used.

- `FILTER_BY_TS` _ts_... (since RedisTimeSeries v1.6)

  Followed by a list of timestamps to filter the result by specific timestamps.

- `FILTER_BY_VALUE` _min_ _max_ (since RedisTimeSeries v1.6)

  Filter result by value using minimum and maximum.

- `COUNT` _count_ - Maximum number of returned samples.

* `ALIGN` _value_ - Time bucket alignment control for AGGREGATION. This will control the time bucket timestamps by changing the reference timestamp on which a bucket is defined.
     Possible values:
     * `start` or `-`: The reference timestamp will be the query start interval time (`fromTimestamp`)which can't be `-`
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

#### Complexity

TS.REVRANGE complexity is O(n/m+k).

n = Number of data points
m = Chunk size (data points per chunk)
k = Number of data points that are in the requested range

This can be improved in the future by using binary search to find the start of the range, which makes this O(Log(n/m)+k*m).
But because m is pretty small, we can neglect it and look at the operation as O(Log(n) + k).

#### Notes

- When the time series is a compaction: the last compacted value may aggregate raw values with timestamp beyond _toTimestamp_. This is because _toTimestamp_ only limits the timestamp of the compacted value, which is the start time of the raw bucket that was compacted.

#### Aggregated Query Example

```sql
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
