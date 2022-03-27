### TS.INFO

#### Format
```sql
TS.INFO key [DEBUG]
```

#### Description

Returns information and statistics on the time series.

#### Parameters

* _key_ - Key name of the time series
* `DEBUG` - An optional flag to get a more detailed information about the chunks.

#### Complexity

O(1)

#### Return Value

Array-reply, specifically:

* `totalSamples` - Total number of samples in the time series.
* `memoryUsage` - Total number of bytes allocated for the time series.
* `firstTimestamp` - First timestamp present in the time series.
* `lastTimestamp` - Last timestamp present in the time series.
* `retentionTime` - Retention time, in milliseconds, for the time series.
* `chunkCount` - Number of Memory Chunks used for the time series.
* `chunkSize` - Amount of memory, in bytes, allocated for data.
* `chunkType` - The chunk type, `compressed` or `uncompressed`.
* `duplicatePolicy` - [Duplicate sample policy](configuration.md#DUPLICATE_POLICY).
* `labels` - A nested array of label-value pairs that represent the metadata labels of the time series.
* `sourceKey` - Key name for source time series in case the current series is a target of a [rule](#tscreaterule).
* `rules` - A nested array of compaction [rules](#tscreaterule) of the time series.

When `DEBUG` is passed, the response will contain an additional array field called `Chunks`.
Each item (per chunk) will contain:
* `startTimestamp` - First timestamp present in the chunk.
* `endTimestamp` - Last timestamp present in the chunk.
* `samples` - Total number of samples in the chunk.
* `size` - The chunk *data* size in bytes (this is the exact size that used for data only inside the chunk, 
  doesn't include other overheads)
* `bytesPerSample` - Ratio of `size` and `samples`

#### `TS.INFO` Example

```sql
TS.INFO temperature:2:32
 1) totalSamples
 2) (integer) 100
 3) memoryUsage
 4) (integer) 4184
 5) firstTimestamp
 6) (integer) 1548149180
 7) lastTimestamp
 8) (integer) 1548149279
 9) retentionTime
10) (integer) 0
11) chunkCount
12) (integer) 1
13) chunkSize
14) (integer) 256
15) chunkType
16) compressed
17) duplicatePolicy
18) (nil)
19) labels
20) 1) 1) "sensor_id"
       2) "2"
    2) 1) "area_id"
       2) "32"
21) sourceKey
22) (nil)
23) rules
24) (empty list or set)
```

With `DEBUG`:
```
...
23) rules
24) (empty list or set)
25) keySelfName
26) "temperature:2:32"
25) Chunks
26) 1)  1) startTimestamp
        2) (integer) 1548149180
        3) endTimestamp
        4) (integer) 1548149279
        5) samples
        6) (integer) 100
        7) size
        8) (integer) 256
        9) bytesPerSample
       10) "1.2799999713897705"
```
