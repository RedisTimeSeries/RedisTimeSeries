# Redis TSDB Module

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
```sql
TS.INSERT key TIMESTAMP value
```
```sql
TS.QUERY key FROM_TIMESTAMP TO_TIMESTAMP
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
```sql
TS.META key
1) lastTimestamp
2) (integer) 1486289265
3) retentionSecs
4) (integer) 0
5) chunkCount
6) (integer) 139
7) maxSamplesPerChunk
8) (integer) 360
```
