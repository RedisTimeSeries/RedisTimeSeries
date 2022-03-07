## Create

### TS.CREATE

Create a new time-series. 

```sql
TS.CREATE key [RETENTION retentionTime] [ENCODING [UNCOMPRESSED|COMPRESSED]] [CHUNK_SIZE size] [DUPLICATE_POLICY policy] [LABELS label value..]
```

* key - Key name for timeseries

Optional args:

 * RETENTION - Maximum age for samples compared to last event time (in milliseconds)
    * Default: The global retention secs configuration of the database (by default, `0`)
    * When set to 0, the series is not trimmed at all
 * ENCODING - Specify the series samples encoding format.
    * COMPRESSED: apply the DoubleDelta compression to the series samples, meaning compression of Delta of Deltas between timestamps and compression of values via XOR encoding.
    * UNCOMPRESSED: keep the raw samples in memory.
   Adding this flag will keep data in an uncompressed form. Compression not only saves
   memory but usually improve performance due to lower number of memory accesses. 
 * CHUNK_SIZE - amount of memory, in bytes, allocated for data. Must be a multiple of 8, Default: 4096.
 * DUPLICATE_POLICY - configure what to do on duplicate sample.
   When this is not set, the server-wide default will be used. 
   For further details: [Duplicate sample policy](configuration.md#DUPLICATE_POLICY).
 * labels - Set of label-value pairs that represent metadata labels of the key

#### Complexity

TS.CREATE complexity is O(1).

#### Create Example

```sql
TS.CREATE temperature:2:32 RETENTION 60000 DUPLICATE_POLICY MAX LABELS sensor_id 2 area_id 32
```

#### Errors

* If a key already exists you get a normal Redis error reply `TSDB: key already exists`. You can check for the existince of a key with Redis [EXISTS command](https://redis.io/commands/exists).

#### Notes

`TS.ADD` can create new time-series in fly, when called on a time-series that does not exist.
