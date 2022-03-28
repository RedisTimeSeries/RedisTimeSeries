### TS.DEL

Delete samples between two timestamps for a given time series.

The given timestamp interval is closed (inclusive), meaning samples which timestamp eqauls the _fromTimestamp_ or _toTimestamp_ will also be deleted.

```sql
TS.DEL key fromTimestamp toTimestamp
```

- _key_ - Key name for time series
- _fromTimestamp_ - Start timestamp for the range deletion.
- _toTimestamp_ - End timestamp for the range deletion.

#### Return value

Integer reply: The number of samples that were removed.

#### Complexity

TS.DEL complexity is O(N) where N is the number of data points that will be removed.

#### Delete range of data points example

```sql
127.0.0.1:6379>TS.DEL temperature:2:32 1548149180000 1548149183000
(integer) 150
```
