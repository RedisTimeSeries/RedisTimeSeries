---
title: Time series
linkTitle: Time series
description: Ingest and query time series data with Redis
type: docs
stack: true
aliases:
    - /docs/stack/timeseries/
---

[![Discord](https://img.shields.io/discord/697882427875393627?style=flat-square)](https://discord.gg/KExRgMb)
[![Github](https://img.shields.io/static/v1?label=&message=repository&color=5961FF&logo=github)](https://github.com/RedisTimeSeries/RedisTimeSeries/)

Redis Stack (specifically, its RedisTimeSeries module) adds a time series data structure to Redis.

## Features
* High volume inserts, low latency reads
* Query by start time and end-time
* Aggregated queries (min, max, avg, sum, range, count, first, last, STD.P, STD.S, Var.P, Var.S, twa) for any time bucket
* Configurable maximum retention period
* Compaction for automatically updated aggregated timeseries
* Secondary indexing for time series entries. Each time series has labels (field value pairs) which will allows to query by labels

## Client libraries

Official and community client libraries in Python, Java, JavaScript, Ruby, Go, C#, Rust, and PHP. 

See the [clients page](clients) for the full list.

## Using with other metrics tools

In the [RedisTimeSeries](https://github.com/RedisTimeSeries) GitHub organization you can
find projects that help you integrate RedisTimeSeries with other tools, including:

1. [Prometheus](https://github.com/RedisTimeSeries/prometheus-redistimeseries-adapter), read/write adapter to use RedisTimeSeries as backend db.
2. [Grafana 7.1+](https://github.com/RedisTimeSeries/grafana-redis-datasource), using the [Redis Data Source](https://redislabs.com/blog/introducing-the-redis-data-source-plug-in-for-grafana/).
3. [Telegraf](https://github.com/influxdata/telegraf). Download the plugin from [InfluxData](https://portal.influxdata.com/downloads/). 
4. StatsD, Graphite exports using graphite protocol.

## Memory model

A time series is a linked list of memory chunks. Each chunk has a predefined size of samples. Each sample is a 128-bit tuple: 64 bits for the timestamp and 64 bits for the value.

## Forum

Got questions? Feel free to ask at the [RedisTimeSeries mailing list](https://forum.redislabs.com/c/modules/redistimeseries).

## License
RedisTimeSeries is licensed under the [Redis Source Available License 2.0 (RSALv2)](https://redis.com/legal/rsalv2-agreement) or the [Server Side Public License v1 (SSPLv1)](https://www.mongodb.com/licensing/server-side-public-license).
