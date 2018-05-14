# Redis Time-Series Module [![Build Status](https://travis-ci.org/danni-m/redis-timeseries.svg?branch=master)](https://travis-ci.org/danni-m/redis-timeseries)

Time series data structure for redis.

## License: AGPL
## Using with other tools metrics tools
See [Tools](tools/) directory.
Including Integration with:
1. StatsD, Graphite exports using graphite protocol.
2. Grafana - using SimpleJson datasource.

## Memory model
A time series is a linked list of memory chunks.
Each chunk has a predefined size of samples, each sample is a tuple of the time and the value.
Each sample is the size of 128bit (64bit for the timestamp and 64bit for the value).

## Features
* Quick inserts (50K samples per sec)
* Query by start time and end-time
* Aggregated queries (Min, Max, Avg, Sum, Count, First, Last) for any time bucket
* Configurable max retention period
* Compactions/Roll-ups - automatically updated aggregated timeseries

## Build
```bash
git submodule init
git submodule update
cd src
make all
```

## Run 
In your redis-server run: `loadmodule redis-tsdb-module.so`.

More infomation about modules can be found at redis offical documentation: https://redis.io/topics/modules-intro

### Tests
Tests are written in python using the [rmtest](https://github.com/RedisLabs/rmtest) library.
```
$ cd src
$ pip install -r tests/requirements.txt # optional, use virtualenv
$ make test
```

## Commands
### TS.create - create a new time-series
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

### TS.createrule - create a compaction rule
```sql
TS.CREATERULE SOURCE_KEY AGG_TYPE BUCKET_SIZE_SEC DEST_KEY
```
* SOURCE_KEY - key name for source time series
* AGG_TYPE - aggregation type one of the following: avg, sum, min, max, count, first, last
* BUCKET_SIZE_SEC - time bucket for aggregated compaction,
* DEST_KEY - key name for destination time series

> DEST_KEY should be of a `timeseries` type, and should be created before TS.CREATERULE is called.

> *Performance Notice*: if a compaction rule exits on a timeseries `TS.ADD` performance might be reduced, the complexity of `TS.ADD` is always O(M) when M is the amount of compactions rules or O(1).

### TS.deleterule - delete a compaction rule
```sql
TS.DELETERULE SOURCE_KEY DEST_KEY
```

* SOURCE_KEY - key name for source time series
* DEST_KEY - key name for destination time series


### TS.add - append a new value to the series
```sql
TS.ADD key TIMESTAMP value
```
#### Complexity
if a compaction rule exits on a timeseries `TS.ADD` performance might be reduced, the complexity of `TS.ADD` is always O(M) when M is the amount of compactions rules or O(1).

### TS.range - ranged query
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
    * aggregationType - one of the following: avg, sum, min, max, count, first, last
    * bucketSizeSeconds - time bucket for aggregation in seconds

#### Complexity
TS.RANGE complexity is O(n/m+k*m)

n = number of data points
m = chunk size (data points per chunk)
k = number of data points that are in the requested range

This can be improved in the future by using binary search to find the start of the range, which will make this O(Log(n/m)+k*m), but since m is pretty small, we can neglect it and look at the operation as O(Log(n) + k).

#### Example for aggregated query
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


### TS.INCRBY/TS.DECRBY - Incrment the latest value
```sql
TS.INCRBY key [VALUE] [RESET] [TIME_BUCKET]
```
This command can be used as a counter/gauge that get automatic history as a time series.

* key - key name for timeseries
Optional args:
    * VALUE - one of the following: avg, sum, min, max, count, first, last
    * RESET - Should the current counter should be resetted when TIME_BUCKET is changes
    * TIME_BUCKET - time bucket for resetting the current counter in seconds

### TS.info - query the series metadata
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
