### TS.DELETERULE

Delete a compaction rule.

```sql
TS.DELETERULE sourceKey destKey
```

- sourceKey - Key name for source time series
- destKey - Key name for destination time series

## Query

### Filtering
For certain read commands a list of filters needs to be applied.  This is the list of possible filters:

* `l=v` label equals value
* `l!=v` label doesn't equal value
* `l=` key does not have the label `l`
* `l!=` key has label `l`
* `l=(v1,v2,...)` key with label `l` that equals one of the values in the list
* `l!=(v1,v2,...)` key with label `l` that doesn't equal any of the values in the list

Note: Whenever filters need to be provided, a minimum of one `l=v` filter must be applied.