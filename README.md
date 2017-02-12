# Redis Time-Series Module

## Overview
With this module you can now store timeseries data efficiently in Redis.
The data is stored in a compact way.

## License: AGPL

## Memory model
Each series has a linked list of chunks.
Each chunk has 1+ samples.
Sample is a timestamp+value.

## Features
* Quick inserts (50K samples per sec)
* Query by start time and end-time
* Configurable max retention period

## Build
1. `cd src`
2. `make`
3. `loadmodule redis-tsdb-module.so`

## Cmds
### TS.create - create a new time-series
```sql
TS.create KEY [retentionSecs] [maxSamplesPerChunk]
```
* key - key name for timeseries
Optional args:
* retentionSecs - max age for samples compared to current time (in seconds).
    * Default: 0
    * When set to 0, the series will not be trimmed at all
* maxSamplesPerChunk - how many samples to keep per memory chunk
    * Default: 360

### TS.add - append a new value to the series
```sql
TS.ADD key TIMESTAMP value
```

### TS.range - ranged query
```sql
TS.RANGE key FROM_TIMESTAMP TO_TIMESTAMP [aggregationType] [bucketSizeSeconds]
 1) (integer) 1486289260
 2) (integer) 49994
 3) (integer) 1486289261
 4) (integer) 49995
 5) (integer) 1486289262
 6) (integer) 49996
 7) (integer) 1486289263
 8) (integer) 49997
 9) (integer) 1486289264
10) (integer) 49998
11) (integer) 1486289265
12) (integer) 49999
```
* key - key name for timeseries
Optional args:
    * aggregationType - one of the following: avg, sum, min, max
    * bucketSizeSeconds - time bucket for aggreagation in seconds

#### Example for aggregated query
```sql
TS.range test 1486932612 1486932700 max 15
 1) (integer) 1486932600
 2) (integer) 2
 3) (integer) 1486932615
 4) (integer) 9
 5) (integer) 1486932630
 6) (integer) 9
 7) (integer) 1486932645
 8) (integer) 9
 9) (integer) 1486932660
10) (integer) 9
11) (integer) 1486932675
12) (integer) 9
```

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
