### TS.GET

Get the last sample.

```sql
TS.GET key
```

* key - Key name for timeseries


#### Return Value

Array-reply, specifically:

The returned array will contain:
- The last sample timestamp followed by the last sample value, when the time-series contains data. 
- An empty array, when the time-series is empty.


#### Complexity

TS.GET complexity is O(1).

#### Examples

##### Get Example on time-series containing data

```sql
127.0.0.1:6379> TS.GET temperature:2:32
1) (integer) 1548149279
2) "23"
```

##### Get Example on empty time-series 

```sql
127.0.0.1:6379> redis-cli TS.GET empty_ts
(empty array)
```