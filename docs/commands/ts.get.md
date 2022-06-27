### TS.GET

Get the last sample.

```sql
TS.GET key [LATEST}
```

* _key_ - Key name for time series

* [LATEST] (since RedisTimeSeries v1.8)

  When the time series is a compaction: With LATEST, TS.GET will report the compacted value of the latest (possibly partial) raw bucket. Without LATEST, TS.GET will report the compacted value of the last 'closed' bucket. When the series is not a compaction: LATEST is ignored.
  
  The data in the latest bucket of a compaction is possibly partial. A bucket is 'closed' and compacted only upon arrival of a new sample that 'opens' a 'new latest' bucket. There are cases, however, when the compacted value of the latest (possibly partial) bucket is required instead of the compacted value of the last 'closed' bucket. LATEST can be used when this is required.

#### Return Value

Array-reply, specifically:

The returned array will contain:
- The last sample timestamp, followed by the last sample value - when the time series contains data. 
- An empty array - when the time series is empty.


#### Complexity

TS.GET complexity is O(1).

#### Examples

##### Get Example on time series containing data

```sql
127.0.0.1:6379> TS.GET temperature:2:32
1) (integer) 1548149279
2) "23"
```

##### Get Example on empty time series 

```sql
127.0.0.1:6379> redis-cli TS.GET empty_ts
(empty array)
```
