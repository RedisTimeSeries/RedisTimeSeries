[![Release](https://img.shields.io/github/release/RedisTimeSeries/RedisTimeSeries.svg?sort=semver&kill_cache=1)](https://github.com/RedisTimeSeries/RedisTimeSeries/releases/latest)
[![CircleCI](https://circleci.com/gh/RedisTimeSeries/RedisTimeSeries/tree/master.svg?style=svg)](https://circleci.com/gh/RedisTimeSeries/RedisTimeSeries/tree/master)
[![Dockerhub](https://img.shields.io/docker/pulls/redis/redis-stack-server?label=redis-stack-server)](https://hub.docker.com/r/redis/redis-stack-server/)
[![codecov](https://codecov.io/gh/RedisTimeSeries/RedisTimeSeries/branch/master/graph/badge.svg)](https://codecov.io/gh/RedisTimeSeries/RedisTimeSeries)

# RedisTimeSeries

[![Discord](https://img.shields.io/discord/697882427875393627?style=flat-square)](https://discord.gg/KExRgMb)

<img src="docs/docs/images/logo.svg" alt="logo" width="300"/>

> [!NOTE]
> Starting with Redis 8, the Time Series data structure is integral to Redis. You don't need to install this module separately.
>
> We no longer release standalone versions of RedisTimeSeries.
>
> See https://github.com/redis/redis

> [!NOTE]
> 32 bit systems are not supported.

## Overview

RedisTimeSeries can hold multiple time series. Each time series is accessible via a single Redis key (similar to any other Redis data structure).

### What is a Redis time series?

A Redis time series comprises:

- **Raw samples**: each raw sample is a {time tag, value} pair.
  - Time tags are measured in milliseconds since January 1st, 1970, at 00:00:00.

    Time tags can be specified by the client or filled automatically by the server.

  - 64-bit floating-point values.

  The intervals between time tags can be constant or variable.

  Raw samples can be reported in-order or out-of-order.

  Duplication policy for samples with identical time tags can be set: block/first/last/min/max/sum.

- An optional configurable **retention period**.

  Raw samples older than the retention period (relative to the raw sample with the highest time tag) are discarded.

- **Series Metadata**: a set of name-value pairs (e.g., room = 3; sensorType = ‘xyz’).

  RedisTimeSeries supports cross-time-series commands. One can, for example, aggregate data over all sensors in the same room or all sensors of the same type.

- Zero or more **compactions**.

  Compactions are an economical way to retain historical data.

  Each compaction is defined by:
  - A timeframe. E.g., 10 minutes
  - An aggregator: min, max, sum, avg, …
  - An optional retention period. E.g., 10 year

  For example, the following compaction: {10 minutes; avg; 10 years} will store the average of the raw values measured in each 10-minutes time frame - for 10 years.

## Examples of time series
- Sensor data: e.g., temperatures or fan velocity for a server in a server farm
- Historical prices of a stock
- Number of vehicles passing through a given road (count per 1-minute timeframes)

## Features
- High volume inserts, low latency reads
- Query by start time and end-time
- Aggregated queries (Min, Max, Avg, Sum, Range, Count, First, Last, STD.P, STD.S, Var.P, Var.S, twa) for any time bucket
- Configurable maximum retention period
- Compactions - automatically updated aggregated timeseries
- Secondary index - each time series has labels (name-value pairs) which will allows to query by labels

## Using with other tools metrics tools
In the [RedisTimeSeries](https://github.com/RedisTimeSeries) organization you can
find projects that help you integrate RedisTimeSeries with other tools, including:

1. [Prometheus](https://github.com/RedisTimeSeries/prometheus-redistimeseries-adapter) - read/write adapter to use RedisTimeSeries as backend db.
2. [Grafana](https://github.com/RedisTimeSeries/grafana-redis-datasource) - using the [Redis Data Source](https://redis.com/blog/introducing-the-redis-data-source-plug-in-for-grafana/).
3. [Telegraf](https://github.com/RedisTimeSeries/telegraf)
4. StatsD, Graphite exports using graphite protocol.

## Documentation

https://redis.io/docs/latest/develop/data-types/timeseries/

## License

Starting with Redis 8, RedisTimeSeries is licensed under your choice of: (i) Redis Source Available License 2.0 (RSALv2); (ii) the Server Side Public License v1 (SSPLv1); or (iii) the GNU Affero General Public License version 3 (AGPLv3). Please review the license folder for the full license terms and conditions. Prior versions remain subject to (i) and (ii).

## Code contributions

By contributing code to this Redis module in any form, including sending a pull request via GitHub, a code fragment or patch via private email or public discussion groups, you agree to release your code under the terms of the Redis Software Grant and Contributor License Agreement. Please see the CONTRIBUTING.md file in this source distribution for more information. For security bugs and vulnerabilities, please see SECURITY.md. 
