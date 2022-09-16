---
syntax: 
---

Delete a compaction rule

## Syntax

{{< highlight bash >}}
TS.DELETERULE sourceKey destKey
{{< / highlight >}}

## Required arguments
<details open><summary><code>sourceKey</code></summary>

is key name for the source time series.
</details>

<details open><summary><code>destKey</code></summary> 

is key name for destination (compacted) time series.
</details>

<note><b>Note:</b> This command does not delete the compacted series.</note>

## See also

`TS.CREATERULE` 

## Related topics

[RedisTimeSeries](/docs/stack/timeseries)