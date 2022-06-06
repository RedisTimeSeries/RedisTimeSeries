---
title: RedisTimeSeries
linkTitle: Time Series
description: Ingest and query time series data with Redis
type: docs
---

[![Discord](https://img.shields.io/discord/697882427875393627?style=flat-square)](https://discord.gg/KExRgMb)
[![GitHub](https://img.shields.io/static/v1?label=&message=repository&color=5961FF&logo=github)](https://github.com/RedisTimeSeries/RedisTimeSeries/)

RedisTimeSeries is a Redis module that adds a time series data structure to Redis.

## Features
* High volume inserts, low latency reads
* Query by start time and end-time
* Aggregated queries (min, max, avg, sum, range, count, first, last, STD.P, STD.S, Var.P, Var.S, twa) for any time bucket
* Configurable maximum retention period
* Downsampling / compaction for automatically updated aggregated timeseries
* Secondary indexing for time series entries. Each time series has labels (field value pairs) which will allows to query by labels

## Client libraries

Official and community client libraries in Python, Java, JavaScript, Ruby, Go, C#, Rust, and PHP. 

See the [clients page](clients) for the full list.

## Using with other metrics tools

In the [RedisTimeSeries](https://github.com/RedisTimeSeries) organization you can
find projects that help you integrate RedisTimeSeries with other tools, including:

1. [Prometheus](https://github.com/RedisTimeSeries/prometheus-redistimeseries-adapter) - read/write adapter to use RedisTimeSeries as backend db.
2. [Grafana 7.1+](https://github.com/RedisTimeSeries/grafana-redis-datasource) - using the [Redis Data Source](https://redislabs.com/blog/introducing-the-redis-data-source-plug-in-for-grafana/).
3. [Telegraph](https://github.com/RedisTimeSeries/telegraf)
4. StatsD, Graphite exports using graphite protocol.

## Memory model

A time series is a linked list of memory chunks. Each chunk has a predefined size of samples. Each sample is a 128-bit tuple: 64 bits for the timestamp and 64 bits for the value.

## Forum

Got questions? Feel free to ask at the [RedisTimeSeries mailing list](https://forum.redislabs.com/c/modules/redistimeseries).

## License

Redis Source Available License Agreement. See [LICENSE](https://raw.githubusercontent.com/RedisTimeSeries/RedisTimeSeries/master/LICENSE)
