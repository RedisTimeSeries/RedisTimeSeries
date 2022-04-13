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

- `WITHLABELS` - Include in the reply all label-value pairs representing metadata labels of the time series. 
- `SELECTED_LABELS` _label_... - Include in the reply a subset of the label-value pairs that represent metadata labels of the time series. This is usefull when there is a large number of labels per series, but only the values of some of the labels are required.
 
If `WITHLABELS` or `SELECTED_LABELS` are not specified, by default, an empty list is reported as the label-value pairs.

#### Return Value

For each time series matching the specified filters, the following is reported:
- The key name
- A list of label-value pairs
  - By default, an empty list is reported
  - If `WITHLABELS` is specified, all labels associated with this time series are reported
  - If `SELECTED_LABELS` _label_... is specified, the selected labels are reported
- The last sample's timetag-value pair

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
