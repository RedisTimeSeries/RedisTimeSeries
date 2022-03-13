---
title: RedisTimeSeries Documentation
titleLink: RedisTimeSeries Documentation
type: docs
---

<img src="images/logo.svg" alt="logo" width="200"/>

[![Forum](https://img.shields.io/badge/Forum-RedisTimeSeries-blue)](https://forum.redislabs.com/c/modules/redistimeseries)
[![Discord](https://img.shields.io/discord/697882427875393627?style=flat-square)](https://discord.gg/KExRgMb)

RedisTimeSeries is a Redis Module adding a Time Series data structure to Redis.

## Features
Read more about the v1.0 GA features [here](https://redislabs.com/blog/redistimeseries-ga-making-4th-dimension-truly-immersive/).

- High volume inserts, low latency reads
- Query by start time and end-time
- Aggregated queries (Min, Max, Avg, Sum, Range, Count, First, Last, STD.P, STD.S, Var.P, Var.S) for any time bucket
- Configurable maximum retention period
- Downsampling/Compaction - automatically updated aggregated timeseries
- Secondary index - each time series has labels (field value pairs) which will allows to query by labels


## Client Libraries

Official and community client libraries in Python, Java, JavaScript, Ruby, Go, C#, Rust, and PHP. 

See the [Clients page](clients) for the full list.

## Using with other tools metrics tools
In the [RedisTimeSeries](https://github.com/RedisTimeSeries) organization you can
find projects that help you integrate RedisTimeSeries with other tools, including:

1. [Prometheus](https://github.com/RedisTimeSeries/prometheus-redistimeseries-adapter) - read/write adapter to use RedisTimeSeries as backend db.
2. [Grafana 7.1+](https://github.com/RedisTimeSeries/grafana-redis-datasource) - using the [Redis Data Source](https://redislabs.com/blog/introducing-the-redis-data-source-plug-in-for-grafana/).
3. [Telegraph](https://github.com/RedisTimeSeries/telegraf)
4. StatsD, Graphite exports using graphite protocol.

## Memory model

A time series is a linked list of memory chunks.
Each chunk has a predefined size of samples.
Each sample is a tuple of the time and the value of 128 bits,
64 bits for the timestamp and 64 bits for the value.

## Mailing List / Forum

Got questions? Feel free to ask at the [RedisTimeSeries mailing list](https://forum.redislabs.com/c/modules/redistimeseries).

## License

Redis Source Available License Agreement - see [LICENSE](https://raw.githubusercontent.com/RedisTimeSeries/RedisTimeSeries/master/LICENSE)