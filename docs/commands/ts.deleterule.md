---
syntax: |
  TS.DELETERULE sourceKey destKey

---

Delete a compaction rule

## Required arguments

<details open><summary><code>sourceKey</code></summary>

is key name for the source time series.
</details>

<details open><summary><code>destKey</code></summary> 

is key name for destination (compacted) time series.
</details>

<note><b>Note:</b> This command does not delete the compacted series.</note>

## Return value

Returns one of these replies:

- @simple-string-reply - `OK` if executed correctly
- @error-reply on error (invalid arguments, etc.), or when such rule does not exist

## See also

`TS.CREATERULE` 

## Related topics

[RedisTimeSeries](/docs/stack/timeseries)
