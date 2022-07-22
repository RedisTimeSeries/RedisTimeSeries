---
syntax: 
---

Append a sample to a time series


## Syntax

{{< highlight bash >}}
TS.ADD key timestamp value [RETENTION retentionPeriod] [ENCODING [COMPRESSED|UNCOMPRESSED]] [CHUNK_SIZE size] [ON_DUPLICATE policy] [LABELS {label value}...]
{{< / highlight >}}

[Examples](#examples)

## Required arguments

<details open><summary><code>key</code></summary> 

is key name for the time series.
</details>

<details open><summary><code>timestamp</code></summary> 

is (integer) UNIX sample timestamp in milliseconds or `*` to set the timestamp to the server clock.
</details>

<details open><summary><code>value</code></summary> 

is (double) numeric data value of the sample. The double number should follow [RFC 7159](https://tools.ietf.org/html/rfc7159) (JSON standard). In particular, the parser rejects overly large values that do not fit in binary64. It does not accept NaN or infinite values.
</details>

<note><b>Note:</b> If the time series does not exist, it is automatically created.</note>

## Optional arguments

The following arguments are optional because they can be set by `TS.CREATE`.

<details open><summary><code>RETENTION retentionPeriod</code></summary> 
 
 is maximum retention period, compared to the maximum existing timestamp, in milliseconds.

Use it only if you are creating a new time series. It is ignored if you are adding samples to an existing time series. See `RETENTION` in `TS.CREATE`.
</details>
    
<details open><summary><code>ENCODING enc</code></summary> 

specifies the series sample's encoding format.

Use it only if you are creating a new time series. It is ignored if you are adding samples to an existing time series. See `ENCODING` in `TS.CREATE`.
</details>

<details open><summary><code>CHUNK_SIZE size</code></summary> is memory size, in bytes, allocated for each data chunk.

Use it only if you are creating a new time series. It is ignored if you are adding samples to an existing time series. See `CHUNK_SIZE` in `TS.CREATE`.
</details>

<details open><summary><code>ON_DUPLICATE_policy</code></summary> 

is overwrite key and database configuration for [DUPLICATE_POLICY](/docs/stack/timeseries/configuration/#duplicate_policy), the policy for handling samples with identical timestamps. It is used with one of the following values:
   - `BLOCK` - An error occurs for any out-of-order sample.
   - `FIRST` - Ignores any newly reported value.
   - `LAST` - Overrides with the newly reported value.
   - `MIN` - Overrides only if the value is lower than the existing value.
   - `MAX` - Overrides only if the value is higher than the existing value.
   - `SUM` - If a previous sample exists, adds the new sample to it so that the updated value is equal to (previous + new). If no previous sample exists, it sets the updated value equal to the new value.
</details>

<details open><summary><code>LABELS {label value}...</code></summary> 

is set of label-value pairs that represent metadata labels of the time series.

Use it only if you are creating a new time series. It is ignored if you are adding samples to an existing time series. See `LABELS` in `TS.CREATE`.
</details>

<note><b>Notes:</b>
- You can use this command to add data to a nonexisting time series in a single command.
  This is why `RETENTION`, `ENCODING`, `CHUNK_SIZE`, `ON_DUPLICATE`, and `LABELS` are optional arguments.
- When specified key does not exist, a new time series is created.
  Setting `RETENTION` and `LABELS` introduces additional time complexity.
- If `timestamp` is older than the retention period compared to the maximum existing timestamp, the sample is appended.
- When adding a sample to a time series for which compaction rules are defined:
  - If all the original samples for an affected aggregated time bucket are available, the compacted value is recalculated based on the reported sample and the original samples.
  - If only a part of the original samples for an affected aggregated time bucket is available due to trimming caused in accordance with the time series RETENTION policy, the compacted value is recalculated based on the reported sample and the available original samples.
  - If the original samples for an affected aggregated time bucket are not available due to trimming caused in accordance with the time series RETENTION policy, the compacted value bucket is not updated.
  </note>

## Complexity

If a compaction rule exits on a time series, the performance of `TS.ADD` can be reduced.
The complexity of `TS.ADD` is always `O(M)`, where `M` is the number of compaction rules or `O(1)` with no compaction.

## Examples

<details open><summary><b>Append a sample to a temperature time series</b></summary>

Create a temperature time series.

{{< highlight bash >}}
127.0.0.1:6379> TS.CREATE temperature:2:32 RETENTION 60000 DUPLICATE_POLICY MAX LABELS sensor_id 2 area_id 32
OK
{{< / highlight >}}

Append a sample to the time series.

{{< highlight bash >}}
127.0.0.1:6379>TS.ADD temperature:2:32 1548149180000 26 LABELS sensor_id 2 area_id 32
(integer) 1548149180000
127.0.0.1:6379>TS.ADD temperature:3:11 1548149183000 27 RETENTION 3600
(integer) 1548149183000
127.0.0.1:6379>TS.ADD temperature:3:11 * 30
(integer) 1559718352000
{{< / highlight >}}
</details>

## See also

`TS.CREATE` 

## Related topics

[RedisTimeSeries](/docs/stack/timeseries)
