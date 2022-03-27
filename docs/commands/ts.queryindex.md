### TS.QUERYINDEX

Get all the keys matching the filter list.

```sql
TS.QUERYINDEX filter...
```

- _filter_...

  This is the list of possible filters:
  - _label_`=`_value_ - _label_ equals _value_
  - _label_`!=`_value_ - label doesn't equal _value_
  - _label_`=` - _key_ does not have the label _label_
  - _label_`!=` - _key_ has label _label_
  - _label_`=(`_value1_`,`_value2_`,`...`)` - key with label _label_ that equals one of the values in the list
  - _lable_`!=(`_value1_`,`_value2_`,`...`)` - key with label _label_ that doesn't equal any of the values in the list

  Note: Whenever filters need to be provided, a minimum of one _label_`=`_value_ filter must be applied.


### Query index example
```sql
127.0.0.1:6379> TS.QUERYINDEX sensor_id=2
1) "temperature:2:32"
2) "temperature:2:33"
```
