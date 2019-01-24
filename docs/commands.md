# RedisTimeSeries Commands

## TS.CREATE - create a new time-series
```sql
TS.CREATE KEY [retentionSecs] [maxSamplesPerChunk]
```
* key - key name for timeseries

Optional args:
* retentionSecs - max age for samples compared to current time (in seconds).
    * Default: 0
    * When set to 0, the series will not be trimmed at all
* maxSamplesPerChunk - how many samples to keep per memory chunk
    * Default: 360

## TS.CREATERULE - create a compaction rule
```sql
TS.CREATERULE SOURCE_KEY AGG_TYPE BUCKET_SIZE_SEC DEST_KEY
```
* SOURCE_KEY - key name for source time series
* AGG_TYPE - aggregation type one of the following: avg, sum, min, max, range, count, first, last
* BUCKET_SIZE_SEC - time bucket for aggregated compaction,
* DEST_KEY - key name for destination time series

> DEST_KEY should be of a `timeseries` type, and should be created before TS.CREATERULE is called.

> *Performance Notice*: if a compaction rule exits on a timeseries `TS.ADD` performance might be reduced, the complexity of `TS.ADD` is always O(M) when M is the amount of compactions rules or O(1).

## TS.DELETERULE - delete a compaction rule
```sql
TS.DELETERULE SOURCE_KEY DEST_KEY
```

* SOURCE_KEY - key name for source time series
* DEST_KEY - key name for destination time series


## TS.add - append a new value to the series
```sql
TS.ADD key TIMESTAMP value
```
* TIMESTAMP - unix timestamp (in seconds) or `*` for automatic timestamp (using the system clock)
* value - sample numeric data value (double)

### Complexity
if a compaction rule exits on a timeseries `TS.ADD` performance might be reduced, the complexity of `TS.ADD` is always O(M) when M is the amount of compactions rules or O(1).

## TS.RANGE - ranged query
```sql
TS.RANGE key FROM_TIMESTAMP TO_TIMESTAMP [aggregationType] [bucketSizeSeconds]
1) 1) (integer) 1487426646
   2) "3.6800000000000002"
2) 1) (integer) 1487426648
   2) "3.6200000000000001"
3) 1) (integer) 1487426650
   2) "3.6200000000000001"
4) 1) (integer) 1487426652
   2) "3.6749999999999998"
5) 1) (integer) 1487426654
   2) "3.73"
```
* key - key name for timeseries
Optional args:
    * aggregationType - one of the following: avg, sum, min, max, range, count, first, last
    * bucketSizeSeconds - time bucket for aggregation in seconds

### Complexity
TS.RANGE complexity is O(n/m+k)

n = number of data points
m = chunk size (data points per chunk)
k = number of data points that are in the requested range

This can be improved in the future by using binary search to find the start of the range, which will make this O(Log(n/m)+k*m), but since m is pretty small, we can neglect it and look at the operation as O(Log(n) + k).

### Example for aggregated query
```sql
ts.range stats_counts.statsd.packets_received 1487527100 1487527133 avg 5
1) 1) (integer) 1487527100
   2) "284.39999999999998"
2) 1) (integer) 1487527105
   2) "281"
3) 1) (integer) 1487527110
   2) "278.80000000000001"
4) 1) (integer) 1487527115
   2) "279.60000000000002"
5) 1) (integer) 1487527120
   2) "215"
6) 1) (integer) 1487527125
   2) "266.80000000000001"
7) 1) (integer) 1487527130
   2) "310.75"
127.0.0.1:6379>

```


## TS.INCRBY/TS.DECRBY - Increment the latest value
```sql
TS.INCRBY key [VALUE] [RESET] [TIME_BUCKET]
```
This command can be used as a counter/gauge that get automatic history as a time series.

* key - key name for timeseries
Optional args:
    * VALUE - one of the following: avg, sum, min, max, range, count, first, last
    * RESET - Should the current counter should be resetted when TIME_BUCKET is changes
    * TIME_BUCKET - time bucket for resetting the current counter in seconds

## TS.INFO - query the series metadata
```sql
TS.INFO key
1) lastTimestamp
2) (integer) 1486289265
3) retentionSecs
4) (integer) 0
5) chunkCount
6) (integer) 139
7) maxSamplesPerChunk
8) (integer) 360
```
