## Update

### TS.ALTER

Update the retention, chunk size, duplicate policy, and labels of an existing key. The parameters are the same as TS.CREATE.

```sql
TS.ALTER key [RETENTION retentionTime] [CHUNK_SIZE size] [DUPLICATE_POLICY policy] [LABELS {label value}...]
```

#### Alter Example

```sql
TS.ALTER temperature:2:32 LABELS sensor_id 2 area_id 32 sub_area_id 15
```

#### Notes
* The command only alters the labels that are given,
  e.g. if labels are given but retention isn't, then only the labels are altered.
* If the labels are altered, the given label-list is applied,
  i.e. labels that are not present in the given list are removed implicitly.
* Supplying the `LABELS` keyword without any labels will remove all existing labels.
* CHUNK_SIZE _size_ - amount of memory, in bytes, allocated for data. Must be a multiple of 8.
