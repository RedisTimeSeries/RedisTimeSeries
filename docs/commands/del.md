## Delete

### DEL

A time series can be deleted using Redis [`DEL`](https://redis.io/commands/del) command.

The time series' expiration timeout can be set using Redis [`EXPIRE`](https://redis.io/commands/expire) command.


```sql
DEL key...
```

* key - Key name for timeseries

#### Complexity

DEL complexity is O(N) where N is the number of keys that will be removed.

#### Delete Serie Example

```sql
DEL temperature:2:32
```

#### Expire in 60 seconds Example

```sql
EXPIRE temperature:2:32 60
```
