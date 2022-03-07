### TS.QUERYINDEX

Get all the keys matching the filter list.

```sql
TS.QUERYINDEX filter...
```

* filter - [See Filtering](#filtering)

### Query index example
```sql
127.0.0.1:6379> TS.QUERYINDEX sensor_id=2
1) "temperature:2:32"
2) "temperature:2:33"
```