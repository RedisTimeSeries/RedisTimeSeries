## Update

### TS.ALTER

Update the retention, chunk size, duplicate policy, and labels of an existing time series.

This command alters only the specified element. E.g., if only `RETENTION` and `LABELS` are specified - the chunk size and the duplicate policy won't be altered.

```sql
TS.ALTER key [RETENTION retentionPeriod] [CHUNK_SIZE size] [DUPLICATE_POLICY policy] [LABELS [{label value}...]]
```

- _key_ - Key name for time series

- `RETENTION` _retentionPeriod_ - Maximum retention period, compared to maximal existing timestamp (in milliseconds).

  See `RETENTION` in [TS.CREATE](https://redis.io/commands/ts.create/)

- `CHUNK_SIZE` _size_ - Memory size, in bytes, allocated for each data chunk.

  See `CHUNK_SIZE` in [TS.CREATE](https://redis.io/commands/ts.create/)

- `DUPLICATE_POLICY` _policy_ - Policy for handling multiple samples with identical timestamps.

  See `DUPLICATE_POLICY` in [TS.CREATE](https://redis.io/commands/ts.create/)

- `LABELS` [{_label_ _value_}...] - Set of label-value pairs that represent metadata labels of the key and serve as a secondary index.

  If `LABELS` is specified, the given label-list is applied. Labels that are not present in the given list are removed implicitly. Specifying `LABELS` with no label-value pairs will remove all existing labels.
  
  See `LABELS` in [TS.CREATE](https://redis.io/commands/ts.create/)

#### Alter Example

```sql
TS.ALTER temperature:2:32 LABELS sensor_id 2 area_id 32 sub_area_id 15
```
