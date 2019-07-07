# Run-time configuration

RedisTimeSeries supports a few run-time configuration options that should be determined when loading the module. In time more options will be added. 

## Passing Configuration Options During Loading

In general, passing configuration options is done by appending arguments after the `--loadmodule` argument in the command line, `loadmodule` configuration directive in a Redis config file, or the `MODULE LOAD` command. For example:

In redis.conf:

```
loadmodule redistimeseries.so OPT1 OPT2
```

From redis-cli:

```
127.0.0.6379> MODULE load redistimeseries.so OPT1 OPT2
```

From command line:

```
$ redis-server --loadmodule ./redistimeseries.so OPT1 OPT2
```

## RedisTimeSeries configuration options

### COMPACTION_POLICY {policy}

Default compaction/downsampling rules for newly created key with `TS.ADD`.

Each rule is separated by a semicolon (`;`), the rule consists of several fields that are separated by a colon (`:`):
* aggregation function - avg, sum, min, max, count, first, last
* time bucket - number and the time representation (Example for 1 minute: 1M)
    * m - millisecond
    * M - minute
    * s - seconds
    * d - day
* retention time - in milliseconds

Example:

`max:1m:1h` - Aggregate using max over 1 minute and retain the last 1 hour
#### Default

<Empty>

#### Example

```
$ redis-server --loadmodule ./redistimeseries.so COMPACTION_POLICY max:1m:1h;min:10s:5d:10d;last:5M:10ms;avg:2h:10d;avg:3d:100d
```

### RETENTION_POLICY

Maximum age for samples compared to last event time (in milliseconds) per key, this configuration will set
the default retention for newly created keys that do not have a an override.

#### Default

0

#### Example

```
$ redis-server --loadmodule ./redistimeseries.so 20
```
