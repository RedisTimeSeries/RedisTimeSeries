# RedisTimeSeries Commands

## Create

### TS.CREATE

Create a new time-series.

```sql
TS.CREATE key [RETENTION retentionTime] [LABELS field value..]
```

* key - Key name for timeseries

Optional args:

 * retentionTime - Maximum age for samples compared to last event time (in milliseconds)
    * Default: The global retention secs configuration of the database (by default, `0`)
    * When set to 0, the series is not trimmed at all
 * labels - Set of key-value pairs that represent metadata labels of the key

#### Create Example

```sql
TS.CREATE temperature RETENTION 60 LABELS sensor_id 2 area_id 32
```

## Update

### TS.ALTER

Update the retention, labels of an existing key. The parameters are the same as TS.CREATE.

```sql
TS.ALTER key [RETENTION retentionTime] [LABELS field value..]
```

#### Alter Example

```sql
TS.ALTER temperature LABELS sensor_id 2 area_id 32 sub_area_id 15
```

#### Notes
* The command only alters the fields that are given,
  e.g. if labels are given but retention isn't, then only the retention is altered.
* If the labels are altered, the given label-list is applied,
  i.e. labels that are not present in the given list are removed implicitly.
* Supplying the labels keyword without any fields will remove all existing labels.  

### TS.ADD

Append (or create and append) a new value to the series.

```sql
TS.ADD key timestamp value [RETENTION retentionTime] [LABELS field value..]
```

* timestamp - UNIX timestamp (in milliseconds) or `*` for automatic timestamp (using the system clock)
* value - Sample numeric data value (double)

These arguments are optional because they can be set by TS.CREATE:

 * retentionTime - Maximum age for samples compared to last event time (in milliseconds)
    * Default: The global retention secs configuration of the database (by default, `0`)
    * When set to 0, the series is not trimmed at all
 * labels - Set of key-value pairs that represent metadata labels of the key

If this command is used to add data to an existing timeseries, `retentionTime` and `labels` are ignored.

#### Examples
```sql
127.0.0.1:6379>TS.ADD temperature:2:32 1548149180 26 LABELS sensor_id 2 area_id 32
(integer) 1548149180
127.0.0.1:6379>TS.ADD temperature:3:11 1548149183 27 RETENTION 3600
(integer) 1548149183
127.0.0.1:6379>TS.ADD temperature:3:11 * 30
(integer) 1559718352
```

#### Complexity

If a compaction rule exits on a timeseries, `TS.ADD` performance might be reduced.
The complexity of `TS.ADD` is always O(M) when M is the amount of compaction rules or O(1) with no compaction.

#### Notes

- You can use this command to add data to an non existing timeseries in a single command.
  This is the reason why `labels` and `retentionTime` are optional arguments.
- When specified and the key doesn't exist, RedisTimeSeries will create the key with the specified `labels` and or `retentionTime`.
  Setting the `labels` and `retentionTime` introduces additional time complexity.

### TS.MADD

Append new values to a list of series.

```sql
TS.ADD key timestamp value [key timestamp value ...]
```

* timestamp - UNIX timestamp or `*` for automatic timestamp (using the system clock)
* value - Sample numeric data value (double)

#### Examples
```sql
127.0.0.1:6379>TS.MADD temperature:2:32 1548149180 26 cpu:2:32 1548149183 54
1) (integer) 1548149180
2) (integer) 1548149183
127.0.0.1:6379>TS.MADD temperature:2:32 1548149181 45 cpu:2:32 1548149180 30
1) (integer) 1548149181
2) (error) TSDB: timestamp is too old
```

#### Complexity

If a compaction rule exits on a timeseries, `TS.MADD` performance might be reduced.
The complexity of `TS.MADD` is always O(N*M) when N is the amount of series updated and M is the amount of compaction rules or O(N) with no compaction.

### TS.INCRBY/TS.DECRBY

Increment the latest value.

```sql
TS.INCRBY key value [RESET time-bucket] [RETENTION retentionTime] [LABELS field value..]
```

or

```sql
TS.DECRBY key value [RESET time-bucket] [RETENTION retentionTime] [LABELS field value..]
```

This command can be used as a counter or gauge that automatically gets history as a time series.

* key - Key name for timeseries
* value - Sample numeric data value (double)

Optional args:

* time-bucket - Time bucket for resetting the current counter in milliseconds
* retentionTime - Maximum age for samples compared to last event time (in milliseconds)
  * Default: The global retention secs configuration of the database (by default, `0`)
  * When set to 0, the series is not trimmed at all
* labels - Set of key-value pairs that represent metadata labels of the key

If this command is used to add data to an existing timeseries, `retentionTime` and `labels` are ignored.

#### Notes

- You can use this command to add data to an non existing timeseries in a single command.
  This is the reason why `labels` and `retentionTime` are optional arguments.
- When specified and the key doesn't exist, RedisTimeSeries will create the key with the specified `labels` and or `retentionTime`.
  Setting the `labels` and `retentionTime` introduces additional time complexity.

## Aggregation, Compaction, Downsampling

### TS.CREATERULE

Create a compaction rule.

```sql
TS.CREATERULE sourceKey destKey AGGREGATION aggType timeBucket
```

- sourceKey - Key name for source time series
- destKey - Key name for destination time series
- aggType - Aggregation type: avg, sum, min, max, range, count, first, last
- timeBucket - Time bucket for aggregation in milliseconds

DEST_KEY should be of a `timeseries` type, and should be created before TS.CREATERULE is called.

### TS.DELETERULE

Delete a compaction rule.

```sql
TS.DELETERULE sourceKey destKey
```

- sourceKey - Key name for source time series
- destKey - Key name for destination time series

## Query

### Filtering
For certain read commands a list of filters needs to be applied.  This is the list of possible filters:
* `l=v` label equals value
* `l!=v` label doesn't equal value
* `l=` key does not ha * ve the label `l`
* `l!=` key has label `l`
* `l=(v1, v2, ...)` key with label `l` that equals one of the values in the list
* `l!=(v1, v2, ...)` key with label `l` that doesn't equals to the values in the list

Note: Whenever filters need to be provided, a minimum of one filter should be applied.

### TS.RANGE

Query a range.

```sql
TS.RANGE key fromTimestamp toTimestamp [AGGREGATION aggregationType timeBucket]
```

- key - Key name for timeseries
- fromTimestamp - Start timestamp for range query
- toTimestamp - End timestamp for range query

Optional args:

- aggregationType - Aggregation type: avg, sum, min, max, range, count, first, last
- timeBucket - Time bucket for aggregation in milliseconds

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
TS.MRANGE fromTimestamp toTimestamp [AGGREGATION aggregationType timeBucket] FILTER filter..
```

* fromTimestamp - Start timestamp for range query
* toTimestamp - End timestamp for range query
* filter - [See Filtering](#filtering)

Optional args:

 * aggregationType - Aggregation type: avg, sum, min, max, count, first, last
 * timeBucket - Time bucket for aggregation in milliseconds

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

### TS.MGET
Get the last samples matching the specific filter.

```sql
TS.MGET FILTER filter...
```
* filter - [See Filtering](#filtering)

#### MGET Example

```sql
127.0.0.1:6379> TS.MGET FILTER area_id=32
1) 1) "temperature:2:32"
   2) 1) 1) "sensor_id"
         2) "2"
      2) 1) "area_id"
         2) "32"
   3) (integer) 1548149181
   4) "30"
2) 1) "temperature:3:32"
   2) 1) 1) "sensor_id"
         2) "3"
      2) 1) "area_id"
         2) "32"
   3) (integer) 1548149181
   4) "29"
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
 3) retentionTime
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

### TS.QUERYINDEX

Get all the keys matching the filter list.

```sql
TS.QUERYINDEX filter...
```

* filter - [See Filtering](#filtering)

### Query index example
```sql
127.0.0.1:6379> TS.QUERYINDEX sensor_id=2
1) "temperature:2:32"
2) "temperature:2:33"
```
