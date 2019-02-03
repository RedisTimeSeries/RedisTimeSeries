# RedisTimeSeries Commands

## TS.CREATE - create a new time-series
```sql
TS.CREATE key [retentionSecs] [maxSamplesPerChunk] [labels]
```
* key - key name for timeseries

Optional args:
   * retentionSecs - max age for samples compared to current time (in seconds).
      * Default: 0
      * When set to 0, the series will not be trimmed at all
   * maxSamplesPerChunk - how many samples to keep per memory chunk
      * Default: 360
   * labels - set of key-value pairs that represent metadata labels of the key

### Example
```sql
TS.CREATE temperature 60 360 sensor_id=2 area_id=32
```

## TS.CREATERULE - create a compaction rule
```sql
TS.CREATERULE sourceKey aggType bucketSizeSec destKey
```
* sourceKey - key name for source time series
* aggType - aggregation type one of the following: avg, sum, min, max, range, count, first, last
* bucketSizeSec - time bucket for aggregated compaction,
* destKey - key name for destination time series

> DEST_KEY should be of a `timeseries` type, and should be created before TS.CREATERULE is called.

> *Performance Notice*: if a compaction rule exits on a timeseries `TS.ADD` performance might be reduced, the complexity of `TS.ADD` is always O(M) when M is the amount of compactions rules or O(1).

## TS.DELETERULE - delete a compaction rule
```sql
TS.DELETERULE sourceKey destKey
```

* sourceKey - key name for source time series
* destKey - key name for destination time series


## TS.ADD - append a new value to the series
```sql
TS.ADD key [labels] timestamp value
```
* labels - set of key-value pairs that represent metadata labels of the key. This will be used if the module was started
with default settings. see TODO.
* timestamp - unix timestamp (in seconds) or `*` for automatic timestamp (using the system clock)
* value - sample numeric data value (double)

### Examples
```sql
TS.ADD temperature:2:32 sensor_id=2 area_id=32 1548149180 26
TS.ADD temperature:3:11 sensor_id=3 area_id=32 1548149180 27
TS.ADD temperature:3:11 1548149181 30
```

### Complexity
if a compaction rule exits on a timeseries `TS.ADD` performance might be reduced, the complexity of `TS.ADD` is always O(M) when M is the amount of compactions rules or O(1).

## TS.RANGE - ranged query
```sql
TS.RANGE key fromTimestamp toTimestamp [aggregationType] [bucketSizeSeconds]
```
* key - key name for timeseries
* fromTimestamp - start timestamp for range query
* toTimestamp - end timestamp for range query

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
127.0.0.1:6379> TS.RANGE temperature:3:32 1548149180 1548149210 avg 5
1) 1) (integer) 1548149180
   2) "26.199999999999999"
2) 1) (integer) 1548149185
   2) "27.399999999999999"
3) 1) (integer) 1548149190
   2) "24.800000000000001"
4) 1) (integer) 1548149195
   2) "23.199999999999999"
5) 1) (integer) 1548149200
   2) "25.199999999999999"
6) 1) (integer) 1548149205
   2) "28"
7) 1) (integer) 1548149210
   2) "20"

```
## TS.RANGEBYLABELS - ranged query by labels
```sql
TS.RANGEBYLABELS key (labels) fromTimestamp toTimestamp [aggregationType] [bucketSizeSeconds]
```

* labels - set of key-pair selector (`k=v`, `k!=v,` `k=` contains a key, `k!=` doesn't contain a key)
* fromTimestamp - start timestamp for range query
* toTimestamp - end timestamp for range query

Optional args:
   * aggregationType - one of the following: avg, sum, min, max, count, first, last
   * bucketSizeSeconds - time bucket for aggregation in seconds

### Examples
```sql
127.0.0.1:6379> TS.RANGEBYLABELS area_id=32 1548149180 1548149210 avg 5
1) 1) "temperature:2:32"
   2) 1) 1) "sensor_id"
         2) "2"
      2) 1) "area_id"
         2) "32"
   3) 1) 1) (integer) 1548149180
         2) "27.600000000000001"
      2) 1) (integer) 1548149185
         2) "23.800000000000001"
      3) 1) (integer) 1548149190
         2) "24.399999999999999"
      4) 1) (integer) 1548149195
         2) "24"
      5) 1) (integer) 1548149200
         2) "25.600000000000001"
      6) 1) (integer) 1548149205
         2) "25.800000000000001"
      7) 1) (integer) 1548149210
         2) "21"
2) 1) "temperature:3:32"
   2) 1) 1) "sensor_id"
         2) "3"
      2) 1) "area_id"
         2) "32"
   3) 1) 1) (integer) 1548149180
         2) "26.199999999999999"
      2) 1) (integer) 1548149185
         2) "27.399999999999999"
      3) 1) (integer) 1548149190
         2) "24.800000000000001"
      4) 1) (integer) 1548149195
         2) "23.199999999999999"
      5) 1) (integer) 1548149200
         2) "25.199999999999999"
      6) 1) (integer) 1548149205
         2) "28"
      7) 1) (integer) 1548149210
         2) "20"
127.0.0.1:6379> TS.RANGEBYLABELS area_id=32 id=3 1548149180 1548149210 avg 5
(empty list or set)
127.0.0.1:6379> TS.RANGEBYLABELS area_id=32 sensor_id=3 1548149180 1548149210 avg 5
1) 1) "temperature:3:32"
   2) 1) 1) "sensor_id"
         2) "3"
      2) 1) "area_id"
         2) "32"
   3) 1) 1) (integer) 1548149180
         2) "26.199999999999999"
      2) 1) (integer) 1548149185
         2) "27.399999999999999"
      3) 1) (integer) 1548149190
         2) "24.800000000000001"
      4) 1) (integer) 1548149195
         2) "23.199999999999999"
      5) 1) (integer) 1548149200
         2) "25.199999999999999"
      6) 1) (integer) 1548149205
         2) "28"
      7) 1) (integer) 1548149210
         2) "20"
127.0.0.1:6379> TS.RANGEBYLABELS area_id=32 sensor_id!=3 1548149180 1548149210 avg 5
1) 1) "temperature:2:32"
   2) 1) 1) "sensor_id"
         2) "2"
      2) 1) "area_id"
         2) "32"
   3) 1) 1) (integer) 1548149180
         2) "27.600000000000001"
      2) 1) (integer) 1548149185
         2) "23.800000000000001"
      3) 1) (integer) 1548149190
         2) "24.399999999999999"
      4) 1) (integer) 1548149195
         2) "24"
      5) 1) (integer) 1548149200
         2) "25.600000000000001"
      6) 1) (integer) 1548149205
         2) "25.800000000000001"
      7) 1) (integer) 1548149210
         2) "21"
```

## TS.INCRBY/TS.DECRBY - Increment the latest value
```sql
TS.INCRBY key value [RESET time-bucket]
```
or
```sql
TS.DECRBY key value [RESET time-bucket]
```
This command can be used as a counter/gauge that get automatic history as a time series.

* key - key name for timeseries
* value - sample numeric data value (double)

Optional args:
   * time-bucket - time bucket for resetting the current counter in seconds

## TS.INFO - query the series metadata
```sql
TS.INFO temperature:2:32
 1) lastTimestamp
 2) (integer) 1548149279
 3) retentionSecs
 4) (integer) 0
 5) chunkCount
 6) (integer) 1
 7) maxSamplesPerChunk
 8) (integer) 1024
 9) labels
10) 1) 1) "sensor_id"
       2) "2"
    2) 1) "area_id"
       2) "32"
11) rules
12) (empty list or set)
```
