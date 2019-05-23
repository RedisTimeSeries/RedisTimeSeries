# RedisTimeSeries Commands

## Create

### TS.CREATE

Create a new time-series.

```sql
TS.CREATE key [RETENTION retentionSecs] [LABELS field value..]
```

* key - Key name for timeseries

Optional args:

 * retentionSecs - Maximum age for samples compared to last event time (in seconds)
    * Default: The global retention secs configuration of the database (by default, `0`)
    * When set to 0, the series is not trimmed at all
 * labels - Set of key-value pairs that represent metadata labels of the key

#### Create Example

```sql
TS.CREATE temperature RETENTION 60 LABELS sensor_id 2 area_id 32
```

## Update

### TS.ADD

Append (or create and append) a new value to the series.

```sql
TS.ADD key timestamp value [RETENTION retentionSecs] [LABELS field value..]
```

* timestamp - UNIX timestamp (in seconds) or `*` for automatic timestamp (using the system clock)
* value - Sample numeric data value (double)

These arguments are optional because they can be set by TS.CREATE:

 * retentionSecs - Maximum age for samples compared to last event time (in seconds)
    * Default: The global retention secs configuration of the database (by default, `0`)
    * When set to 0, the series is not trimmed at all
 * labels - Set of key-value pairs that represent metadata labels of the key

If this command is used to add data to an existing timeseries, `retentionSecs` and `labels` are ignored.

#### Examples
```sql
TS.ADD temperature:2:32 1548149180 26 LABELS sensor_id 2 area_id 32
TS.ADD temperature:3:11 1548149180 27 RETENTION 3600
TS.ADD temperature:3:11 1548149181 30
```

#### Complexity

If a compaction rule exits on a timeseries, `TS.ADD` performance might be reduced.
The complexity of `TS.ADD` is always O(M) when M is the amount of compaction rules or O(1) with no compaction.

#### Notes

- You can use this command to add data to an non existing timeseries in a single command.
  This is the reason why `labels` and `retentionsecs` are optional arguments.
- When specified and the key doesn't exist, RedisTimeSeries will create the key with the specified `labels` and or `retentionSecs`.
  Setting the `labels` and `retentionSecs` introduces additional time complexity.

### TS.INCRBY/TS.DECRBY

Increment the latest value.

```sql
TS.INCRBY key value [RESET time-bucket] [RETENTION retentionSecs] [LABELS field value..]
```

or

```sql
TS.DECRBY key value [RESET time-bucket] [RETENTION retentionSecs] [LABELS field value..]
```

This command can be used as a counter or gauge that automatically gets history as a time series.

* key - Key name for timeseries
* value - Sample numeric data value (double)

Optional args:

* time-bucket - Time bucket for resetting the current counter in seconds
* retentionSecs - Maximum age for samples compared to last event time (in seconds)
  * Default: The global retention secs configuration of the database (by default, `0`)
  * When set to 0, the series is not trimmed at all
* labels - Set of key-value pairs that represent metadata labels of the key

If this command is used to add data to an existing timeseries, `retentionSecs` and `labels` are ignored.

#### Notes

- You can use this command to add data to an non existing timeseries in a single command.
  This is the reason why `labels` and `retentionsecs` are optional arguments.
- When specified and the key doesn't exist, RedisTimeSeries will create the key with the specified `labels` and or `retentionSecs`.
  Setting the `labels` and `retentionSecs` introduces additional time complexity.

## Aggregation, Compaction, Downsampling

### TS.CREATERULE

Create a compaction rule.

```sql
TS.CREATERULE sourceKey destKey AGGREGATION aggType bucketSizeSeconds
```

- sourceKey - Key name for source time series
- destKey - Key name for destination time series
- aggType - Aggregation type: avg, sum, min, max, range, count, first, last
- bucketSizeSeconds - Time bucket for aggregation in seconds

DEST_KEY should be of a `timeseries` type, and should be created before TS.CREATERULE is called.

### TS.DELETERULE

Delete a compaction rule.

```sql
TS.DELETERULE sourceKey destKey
```

- sourceKey - Key name for source time series
- destKey - Key name for destination time series

## Query

### TS.RANGE

Query a range.

```sql
TS.RANGE key fromTimestamp toTimestamp [AGGREGATION aggregationType bucketSizeSeconds]
```

- key - Key name for timeseries
- fromTimestamp - Start timestamp for range query
- toTimestamp - End timestamp for range query

Optional args:

- aggregationType - Aggregation type: avg, sum, min, max, range, count, first, last
- bucketSizeSeconds - Time bucket for aggregation in seconds

#### Complexity

TS.RANGE complexity is O(n/m+k).

n = Number of data points
m = Chunk size (data points per chunk)
k = Number of data points that are in the requested range

This can be improved in the future by using binary search to find the start of the range, which makes this O(Log(n/m)+k*m).
But because m is pretty small, we can neglect it and look at the operation as O(Log(n) + k).

#### Aggregated Query Example

```sql
127.0.0.1:6379> TS.RANGE temperature:3:32 1548149180 1548149210 AGGREGATION avg 5
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

### TS.MRANGE

Query a range by filters.

```sql
TS.MRANGE fromTimestamp toTimestamp [AGGREGATION aggregationType bucketSizeSeconds] FILTER filter..
```

* fromTimestamp - Start timestamp for range query
* toTimestamp - End timestamp for range query
* filters - Set of key-pair fitlers (`k=v`, `k!=v,` `k=` contains a key, `k!=` doesn't contain a key)

Optional args:

 * aggregationType - Aggregation type: avg, sum, min, max, count, first, last
 * bucketSizeSeconds - Time bucket for aggregation in seconds

#### Query by Filters Example

```sql
127.0.0.1:6379> TS.MRANGE 1548149180 1548149210 AGGREGATION avg 5 FILTER area_id=32 sensor_id!=1
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
```

### TS.GET

Get the last sample.

```sql
TS.GET key
```

* key - Key name for timeseries

#### Get Example

```sql
127.0.0.1:6379> TS.GET temperature:2:32
1) (integer) 1548149279
2) "23"
```

## General

### TS.INFO

Query the series metadata

```sql
TS.INFO key
```

* key - Key name for timeseries

#### Info Example

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
