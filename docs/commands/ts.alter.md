## Update

### TS.ALTER

Update the retention, chunk size, duplicate policy, and labels of an existing time series.

```sql
TS.ALTER key [RETENTION retentionTime] [CHUNK_SIZE size] [DUPLICATE_POLICY policy] [LABELS [{label value}...]]
```

- _key_ - Key name for time series
- `RETENTION` _retentionTime_ - Maximum age for samples compared to last event time (in milliseconds)
   - Default: The global retention secs configuration of the database (by default, `0`)
   - When set to 0, the series is not trimmed at all
- `CHUNK_SIZE` _size_ - amount of memory, in bytes, allocated for data. Must be a multiple of 8. Default: 4096.
- `DUPLICATE_POLICY` _policy_ - Policy for handling multiple samples with identical timestamps. One of the following values:
  - `BLOCK` - an error will occur for any out of order sample
  - `FIRST` - ignore any newly reported value
  - `LAST` - override with the newly reported value
  - `MIN` - only override if the value is lower than the existing value
  - `MAX` - only override if the value is higher than the existing value
  - `SUM` - If a previous sample exists, add the new sample to it so that the updated value is equal to (previous + new). If no previous sample exists, set the updated value equal to the new value.

  When not specified, the server-wide default will be used.

- `LABELS` [{_label_ _value_}...] - Set of label-value pairs that represent metadata labels of the key

  If `LABELS` is specified, the given label-list is applied. Labels that are not present in the given list are removed implicitly.  

  Specifying `LABELS` with no label-value pairs will remove all existing labels.
  

#### Alter Example

```sql
TS.ALTER temperature:2:32 LABELS sensor_id 2 area_id 32 sub_area_id 15
```

#### Notes
* This command alters only the given element. E.g., if `LABELS` is specified, but `RETENTION` isn't, only the labels are altered.
