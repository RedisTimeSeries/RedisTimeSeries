---
syntax: |
  TS.ADD key timestamp value 
    [RETENTION retentionPeriod] 
    [ENCODING [COMPRESSED|UNCOMPRESSED]] 
    [CHUNK_SIZE size] 
    [ON_DUPLICATE policy] 
    [LABELS {label value}...]
---

Append a sample to a time series

[Examples](#examples)

## Required arguments

<details open><summary><code>key</code></summary> 

is key name for the time series.
</details>

<details open><summary><code>timestamp</code></summary> 

is Unix time (integer, in milliseconds) specifying the sample timestamp or `*` to set the sample timestamp to the Unix time of the server's clock.

Unix time is the number of milliseconds that have elapsed since 00:00:00 UTC on 1 January 1970, the Unix epoch, without adjustments made due to leap seconds.
</details>

<details open><summary><code>value</code></summary> 

is (double) numeric data value of the sample. The double number should follow [RFC 7159](https://tools.ietf.org/html/rfc7159) (JSON standard). In particular, the parser rejects overly large values that do not fit in binary64. It does not accept NaN or infinite values.
</details>

<note><b>Notes:</b>
- When specified key does not exist, a new time series is created.

  if a [COMPACTION_POLICY](/docs/stack/timeseries/configuration/#compaction_policy) configuration parameter is defined, compacted time series would be created as well.

- If `timestamp` is older than the retention period compared to the maximum existing timestamp, the sample is discarded and an error is returned.
- When adding a sample to a time series for which compaction rules are defined:
  - If all the original samples for an affected aggregated time bucket are available, the compacted value is recalculated based on the reported sample and the original samples.
  - If only a part of the original samples for an affected aggregated time bucket is available due to trimming caused in accordance with the time series RETENTION policy, the compacted value is recalculated based on the reported sample and the available original samples.
  - If the original samples for an affected aggregated time bucket are not available due to trimming caused in accordance with the time series RETENTION policy, the compacted value bucket is not updated.
- Explicitly adding samples to a compacted time series (using `TS.ADD`, `TS.MADD`, `TS.INCRBY`, or `TS.DECRBY`) may result in inconsistencies between the raw and the compacted data. The compaction process may override such samples.
</note>

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

<details open><summary><code>ON_DUPLICATE policy</code></summary> 

is overwrite key and database configuration for [DUPLICATE_POLICY](/docs/stack/timeseries/configuration/#duplicate_policy), the policy for handling samples with identical timestamps.
This override is effective only for this single command and does not set the time series duplication policy (which can be set with `TS.ALTER`).

`policy` can be one of the following values:
  - `BLOCK`: ignore any newly reported value and reply with an error
  - `FIRST`: ignore any newly reported value
  - `LAST`: override with the newly reported value
  - `MIN`: only override if the value is lower than the existing value
  - `MAX`: only override if the value is higher than the existing value
  - `SUM`: If a previous sample exists, add the new sample to it so that the updated value is set to (previous + new). If no previous sample exists, the value is set to the new value.

This argument has no effect when a new time series is created by this command.
</details>

<details open><summary><code>LABELS {label value}...</code></summary> 

is set of label-value pairs that represent metadata labels of the time series.

Use it only if you are creating a new time series. It is ignored if you are adding samples to an existing time series. See `LABELS` in `TS.CREATE`.
</details>

<note><b>Notes:</b>
- You can use this command to create a new time series and add data to it in a single command.
  `RETENTION`, `ENCODING`, `CHUNK_SIZE`, and `LABELS` are used only when creating a new time series, and ignored when adding samples to an existing time series.
- Setting `RETENTION` and `LABELS` introduces additional time complexity.
</note>

## Return value

Returns one of these replies:

- @integer-reply - the timestamp of the upserted sample
- @error-reply on error (invalid arguments, wrong key type, etc.), when duplication policy is `BLOCK`, or when `timestamp` is older than the retention period compared to the maximum existing timestamp

## Complexity

If a compaction rule exists on a time series, the performance of `TS.ADD` can be reduced.
The complexity of `TS.ADD` is always `O(M)`, where `M` is the number of compaction rules or `O(1)` with no compaction.

## Examples

<details open><summary><b>Append a sample to a temperature time series</b></summary>

Create a temperature time series, set its retention to 1 year, and append a sample.

{{< highlight bash >}}
127.0.0.1:6379> TS.ADD temperature:3:11 1548149183000 27 RETENTION 31536000000
(integer) 1548149183000
{{< / highlight >}}

<note><b>Note:</b> If a time series with such a name already exists, the sample is added, but the retention does not change.</note>

Add a sample to the time series, setting the sample's timestamp to the current Unix time of the server's clock.

{{< highlight bash >}}
127.0.0.1:6379> TS.ADD temperature:3:11 * 30
(integer) 1662042954573
{{< / highlight >}}
</details>

## See also

`TS.CREATE` 

## Related topics

[RedisTimeSeries](/docs/stack/timeseries)
