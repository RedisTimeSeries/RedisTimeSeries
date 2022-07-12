---
syntax: 
---

Get all time series keys matching a filter list.

## Syntax

{{< highlight bash >}}
TS.QUERYINDEX filter...
{{< / highlight >}}

[**Examples**](#examples)

## Required arguments

FILTER filter..` uses these filters:

  - `label = value`, where `label` equals `value`
  - `label != value`, where `label` does not equal `value`
  - `label = `, where `key` does not have label `label`
  - `label != `, where `key` has label `label`
  - `label = (_value1_,_value2_,...)`, where `key` with label `label` equals one of the values in the list
  - `label != (value1,value2,...)` is key with label `label` that does not equal any of the values in the list

**NOTES:** 
 - When using filters, apply a minimum of one `label = value` filter. 
 - `QUERYINDEX` cannot be part of a transaction that runs on a Redis cluster.

## Examples

### Find keys by location and sensor type

Create a set of sensors to measure temperature and humidity in your office and kitchen.

{{< highlight bash >}}
127.0.0.1:6379> TS.CREATE telemetry:office:temperature LABELS room office type temperature
OK
127.0.0.1:6379> TS.CREATE telemetry:office:humidity LABELS room office type humidity
OK
127.0.0.1:6379> TS.CREATE telemetry:kitchen:temperature LABELS room kitchen type temperature
OK
127.0.0.1:6379> TS.CREATE telemetry:kitchen:humidity LABELS room kitchen type humidity
OK
{{< / highlight >}}

Query the sensors in the kitchen to find all the keys associated with that room. 

{{< highlight bash >}}
127.0.0.1:6379> TS.QUERYINDEX room=kitchen
1) "telemetry:kitchen:humidity"
2) "telemetry:kitchen:temperature"
{{< / highlight >}}

To monitor all the keys for temperature, use this query:

{{< highlight bash >}}
127.0.0.1:6379> TS.QUERYINDEX type=temperature
1) "telemetry:kitchen:temperature"
2) "telemetry:office:temperature"
{{< / highlight >}}

## See also

`TS.CREATE` | `TS.MRANGE` | `TS.MREVRANGE` | `MGET`

## Related topics

[RedisTimeSeries](/docs/stack/timeseries)
