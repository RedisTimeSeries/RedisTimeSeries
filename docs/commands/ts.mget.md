---
syntax: 
---

Get the last samples matching a specific filter

## Syntax

{{< highlight bash >}}
TS.MGET [LATEST] [WITHLABELS | SELECTED_LABELS label...] FILTER filter...
{{< / highlight >}}

[Examples](#examples)

## Required arguments

<details open>
<summary><code>FILTER filter..</code></summary> 

filters time series based on their labels and label values, with these options:

  - `label = value`, where `label` equals `value`
  - `label != value`, where `label` does not equal `value`
  - `label = `, where `key` does not have label `label`
  - `label != `, where `key` has label `label`
  - `label = (_value1_,_value2_,...)`, where `key` with label `label` equals one of the values in the list
  - `label != (value1,value2,...)` where key with label `label` does not equal any of the values in the list

  <note><b>NOTES:</b> 
   - When using filters, apply a minimum of one `label = value` filter.
   - Filters are conjunctive. For example, the FILTER `type = temperature room = study` means the a time series is a temperature time series of a study room.
   </note>

  </details>

## Optional arguments

<details open>
<summary><code>LATEST</code> (since RedisTimeSeries v1.8)</summary> 

is used when a time series is a compaction. With `LATEST`, TS.MGET also reports the compacted value of the latest possibly partial bucket, given that this bucket's start time falls within `[fromTimestamp, toTimestamp]`. Without `LATEST`, TS.MGET does not report the latest possibly partial bucket. When a time series is not a compaction, `LATEST` is ignored.
  
The data in the latest bucket of a compaction is possibly partial. A bucket is _closed_ and compacted only upon arrival of a new sample that _opens_ a new _latest_ bucket. There are cases, however, when the compacted value of the latest possibly partial bucket is also required. In such a case, use `LATEST`.
</details>

<details open>
<summary><code>WITHLABELS</code></summary> 

includes in the reply all label-value pairs representing metadata labels of the time series. 
If `WITHLABELS` or `SELECTED_LABELS` are not specified, by default, an empty list is reported as label-value pairs.
</details>

<details open>
<summary><code>SELECTED_LABELS label...</code> (since RedisTimeSeries v1.6)</summary> 

returns a subset of the label-value pairs that represent metadata labels of the time series. 
Use when a large number of labels exists per series, but only the values of some of the labels are required. 
If `WITHLABELS` or `SELECTED_LABELS` are not specified, by default, an empty list is reported as label-value pairs.
</details>

## Return value

For each time series matching the specified filters, the following is reported:
- The key name
- A list of label-value pairs
  - By default, an empty list is reported
  - If `WITHLABELS` is specified, all labels associated with this time series are reported
  - If `SELECTED_LABELS label...` is specified, the selected labels are reported
- Timestamp-value pairs for all samples/aggregations matching the range

<note><b>Note:</b> The `MGET` command cannot be part of transaction when running on a Redis cluster.</note>

## Examples

<details open>
<summary><b>Select labels to retrieve</b></summary>

Create time series for temperature in Tel Aviv and Jerusalem, then add different temperature samples.

{{< highlight bash >}}
127.0.0.1:6379> TS.CREATE temp:TLV LABELS type temp location TLV
OK
127.0.0.1:6379> TS.CREATE temp:JLM LABELS type temp location JLM
OK
127.0.0.1:6379> TS.MADD temp:TLV 1000 30 temp:TLV 1010 35 temp:TLV 1020 9999 temp:TLV 1030 40
1) (integer) 1000
2) (integer) 1010
3) (integer) 1020
4) (integer) 1030
127.0.0.1:6379> TS.MADD temp:JLM 1005 30 temp:JLM 1015 35 temp:JLM 1025 9999 temp:JLM 1035 40
1) (integer) 1005
2) (integer) 1015
3) (integer) 1025
4) (integer) 1035
{{< / highlight >}}

Get all the labels associated with the last sample.

{{< highlight bash >}}
127.0.0.1:6379> TS.MGET WITHLABELS FILTER type=temp
1) 1) "temp:JLM"
   2) 1) 1) "type"
         2) "temp"
      2) 1) "location"
         2) "JLM"
   3) 1) (integer) 1035
      2) 40
2) 1) "temp:TLV"
   2) 1) 1) "type"
         2) "temp"
      2) 1) "location"
         2) "TLV"
   3) 1) (integer) 1030
      2) 40
{{< / highlight >}}

To get only the `location` label for each last sample, use `SELECTED_LABELS`.

{{< highlight bash >}}
127.0.0.1:6379> TS.MGET SELECTED_LABELS location FILTER type=temp
1) 1) "temp:JLM"
   2) 1) 1) "location"
         2) "JLM"
   3) 1) (integer) 1035
      2) 40
2) 1) "temp:TLV"
   2) 1) 1) "location"
         2) "TLV"
   3) 1) (integer) 1030
      2) 40
{{< / highlight >}}
</details>

## See also

`TS.MRANGE` | `TS.RANGE` | `TS.MREVRANGE` | `TS.REVRANGE`

## Related topics

[RedisTimeSeries](/docs/stack/timeseries)

