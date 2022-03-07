### TS.RANGE

Query a range in forward direction.

```sql
TS.RANGE key fromTimestamp toTimestamp
         [FILTER_BY_TS TS1 TS2 ..]
         [FILTER_BY_VALUE min max]
         [COUNT count] [ALIGN value]
         [AGGREGATION aggregationType bucketDuration]
```

- key - Key name for timeseries
- fromTimestamp - Start timestamp for the range query. `-` can be used to express the minimum possible timestamp (0).
- toTimestamp - End timestamp for range query, `+` can be used to express the maximum possible timestamp.

Optional parameters:

* FILTER_BY_TS - Followed by a list of timestamps to filter the result by specific timestamps
* FILTER_BY_VALUE - Filter result by value using minimum and maximum.

* COUNT - Maximum number of returned samples.

* ALIGN - Time bucket alignment control for AGGREGATION. This will control the time bucket timestamps by changing the reference timestamp on which a bucket is defined.
     Possible values:
     * `start` or `-`: The reference timestamp will be the query start interval time (`fromTimestamp`).
     * `end` or `+`: The reference timestamp will be the query end interval time (`toTimestamp`).
     * A specific timestamp: align the reference timestamp to a specific time.
     * **Note:** when not provided alignment is set to `0`.

* AGGREGATION - Aggregate result into time buckets (the following aggregation parameters are mandtory)
  * aggregationType - Aggregation type: avg, sum, min, max, range, count, first, last, std.p, std.s, var.p, var.s
  * bucketDuration - Time bucket duration for aggregation in milliseconds

#### Complexity

TS.RANGE complexity is O(n/m+k).

n = Number of data points
m = Chunk size (data points per chunk)
k = Number of data points that are in the requested range

This can be improved in the future by using binary search to find the start of the range, which makes this O(Log(n/m)+k*m).
But because m is pretty small, we can neglect it and look at the operation as O(Log(n) + k).

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