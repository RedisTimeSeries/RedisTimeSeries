### TS.DECRBY

Decrease the value of the sample with the maximal existing timestamp, or create a new sample with a value equal to the value of the sample with the maximal existing timestamp with a given decrement.

```sql
TS.DECRBY key value [TIMESTAMP timestamp] [RETENTION retentionPeriod] [UNCOMPRESSED] [CHUNK_SIZE size] [LABELS {label value}...]
```
If the time series does not exist - it will be automatically created.

This command can be used as a counter or gauge that automatically gets history as a time series.

- _key_ - Key name for time series
- _value_ - numeric data value of the sample (double)

Optional args:

- `TIMESTAMP` _timestamp_ - (integer) UNIX sample timestamp **in milliseconds**. `*` can be used for an automatic timestamp from the  server's clock.

  _timestamp_ must be equal to or higher than the maximal existing timestamp. When equal, the value of the sample with the maximal existing timestamp is decreased. When higher, a new sample with a timestamp set to _timestamp_ will be created, and its value will be set to the value of the sample with the maximal existing timestamp minus _value_. If the time series is empty - the value would be set to _value_.

- `RETENTION` _retentionPeriod_ - Maximum retention period, compared to maximal existing timestamp (in milliseconds).

  Used only if a new time series is created. Ignored When adding samples to an existing time series.

  When set to 0, the series is not trimmed. If not specified: set to the global [RETENTION_POLICY](https://redis.io/docs/stack/timeseries/configuration/#retention_policy) configuration of the database (which, by default, is 0).
 
- `UNCOMPRESSED` - Changes data storage from compressed (by default) to uncompressed

  Used only if a new time series is created. Ignored When adding samples to an existing time series.

- `CHUNK_SIZE` _size_ - Memory size, in bytes, allocated for each data chunk. Must be a multiple of 8 in the range [128 .. 1048576].

  Used only if a new time series is created. Ignored When adding samples to an existing time series.

  If not specified: set to 4096.

- `LABELS` {_label_ _value_}... - Set of label-value pairs that represent metadata labels of the time series.

  Used only if a new time series is created. Ignored When adding samples to an existing time series.

#### Notes

- You can use this command to add data to a nonexisting time series in a single command.
  This is why `RETENTION`, `UNCOMPRESSED`,  `CHUNK_SIZE`, and `LABELS` are optional arguments.
- When specified and the key doesn't exist, a new time series is created.
  Setting the `RETENTION` and `LABELS` introduces additional time complexity.
