<img src="images/logo.png" alt="logo" width="100"/>

# RedisTimeSeries Module
Time series data structure for Redis.

## Using with other tools metrics tools
See [RedisTimeSeries](https://github.com/RedisTimeSeries) organization.
Including Integration with:

1. Prometheus - [Adapter for Prometheus](https://github.com/RedisTimeSeries/prometheus-redistimeseries-adapter) to use RedisTimeSeries as backend db.
2. StatsD, Graphite exports using graphite protocol.
3. Grafana - using SimpleJson datasource.

## Memory model
A time series is a linked list of memory chunks.
Each chunk has a predefined size of samples, each sample is a tuple of the time and the value.
Each sample is the size of 128bit (64bit for the timestamp and 64bit for the value).

## Features
* Quick inserts (50K samples per sec)
* Query by start time and end-time
* Aggregated queries (Min, Max, Avg, Sum, Range, Count, First, Last) for any time bucket
* Configurable max retention period
* Compactions/Roll-ups - automatically updated aggregated timeseries

## Docker

To quickly tryout Redis-TimeSeries, launch an instance using docker:

```sh
docker run -p 6379:6379 -it --rm redislabs/redistimeseries
```

## Give it a try

Here we'll create a time series representing sensor temperature measurements.
Once created, temperature measurements can be sent.
Last the data can queried for a time range while based on some aggreagation rule

### With `redis-cli`

```sh
$ redis-cli
127.0.0.1:6379> TS.CREATE temperature RETENTION 60 LABELS sensor_id 2 area_id 32
OK
127.0.0.1:6379> TS.ADD temperature:3:11 1548149181 30
OK
127.0.0.1:6379> TS.ADD temperature:3:11 1548149191 42
OK
127.0.0.1:6379>  TS.RANGE temperature:3:11 1548149180 1548149210 AGGREGATION avg 5
1) 1) (integer) 1548149180
   2) "30"
2) 1) (integer) 1548149190
   2) "42"
```

### Client libraries

Some languages have client libraries that provide support for RedisTimeSeries's commands:

| Project | Language | License | Author | URL |
| ------- | -------- | ------- | ------ | --- |
| JRedisTimeSeries | Java | BSD-3 | [RedisLabs](https://redislabs.com/) | [Github](https://github.com/RedisTimeSeries/JRedisTimeSeries/) |
| redistimeseries-go | Go | Apache-2 | [RedisLabs](https://redislabs.com/) | [Github](https://github.com/RedisTimeSeries/redistimeseries-go) |
