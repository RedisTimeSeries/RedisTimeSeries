### TS.MGET
Get the last samples matching a specific filter.

```sql
TS.MGET [WITHLABELS | SELECTED_LABELS label...] FILTER filter...
```
- FILTER _filter_...

  This is the list of possible filters:
  - _label_`=`_value_ - _label_ equals _value_
  - _label_`!=`_value_ - label doesn't equal _value_
  - _label_`=` - _key_ does not have the label _label_
  - _label_`!=` - _key_ has label _label_
  - _label_`=(`_value1_`,`_value2_`,`...`)` - key with label _label_ that equals one of the values in the list
  - _lable_`!=(`_value1_`,`_value2_`,`...`)` - key with label _label_ that doesn't equal any of the values in the list

  Note: Whenever filters need to be provided, a minimum of one _label_`=`_value_ filter must be applied.

Optional args:

- `WITHLABELS` - Include in the reply the label-value pairs that represent metadata labels of the time series. 
- `SELECTED_LABELS` _label_... - Include in the reply a subset of the label-value pairs that represent metadata labels of the time series. This is usefull when you have a large number of labels per serie but are only interested in the value of some of the labels. 
 
If `WITHLABELS` or `SELECTED_LABELS` are not specified, by default, an empty array will be replied on the labels array position.

#### Return Value

Array-reply, specifically:

The command returns the entries with labels matching the specified filter.
The returned entries are complete, that means that the name, labels and all the last sample of the time serie.

The returned array will contain _key1_, _labels1_, _lastsample1_, ... , _keyN_, _labelsN_, _lastsampleN_, with labels and lastsample being also of array data types. By default, the labels array will be an empty Array for each of the returned time series.

If the `WITHLABELS` or `SELECTED_LABELS` option is specified the labels array will be filled with label-value pairs that represent metadata labels of the time series.


#### Complexity

TS.MGET complexity is O(n).

n = Number of time series that match the filters

#### Examples

##### MGET Example with default behaviour
```sql
127.0.0.1:6379> TS.MGET FILTER area_id=32
1) 1) "temperature:2:32"
   2) (empty list or set)
   3) 1) (integer) 1548149181000
      2) "30"
2) 1) "temperature:3:32"
   2) (empty list or set)
   3) 1) (integer) 1548149181000
      2) "29"
```

##### MGET Example with WITHLABELS option
```sql
127.0.0.1:6379> TS.MGET WITHLABELS FILTER area_id=32
1) 1) "temperature:2:32"
   2) 1) 1) "sensor_id"
         2) "2"
      2) 1) "area_id"
         2) "32"
   3) 1) (integer) 1548149181000
      2) "30"
2) 1) "temperature:3:32"
   2) 1) 1) "sensor_id"
         2) "2"
      2) 1) "area_id"
         2) "32"
   3) 1) (integer) 1548149181000
      2) "29"
```
