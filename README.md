[![CircleCI](https://circleci.com/gh/RedisLabsModules/RedisTimeSeries/tree/master.svg?style=svg)](https://circleci.com/gh/RedisLabsModules/RedisTimeSeries/tree/master)
[![GitHub issues](https://img.shields.io/github/release/RedisLabsModules/redis-timeseries.svg)](https://github.com/RedisLabsModules/redis-timeseries/releases/latest)

# RedisTimeSeries
RedisTimeSeries is a Redis Module adding a Time Series data structure to Redis.

## Features
* Quick inserts (50K samples per sec)
* Query by start time and end-time
* Query by labels sets
* Aggregated queries (Min, Max, Avg, Sum, Range, Count, First, Last) for any time bucket
* Configurable max retention period
* Compactions/Roll-ups - automatically updated aggregated timeseries
* labels index - each key has labels which will allows query by labels

## Using with other tools metrics tools
See [RedisTimeSeries](https://github.com/RedisTimeSeries) organization.
Including Integration with:

1. Prometheus - [Adapter for Prometheus](https://github.com/RedisTimeSeries/prometheus-redistimeseries-adapter) to use RedisTimeSeries as backend db.
2. StatsD, Graphite exports using graphite protocol.
3. Grafana - using SimpleJson datasource.

## Memory model
A time series is a linked list of memory chunks.
Each chunk has a predefined size of samples, each sample is a tuple of the time and the value.

## Docker
To quickly tryout Redis-TimeSeries, launch an instance using docker:

```sh
docker run -p 6379:6379 -it --rm redislabs/redistimeseries
```

### Give it a try
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

## Build
```bash
git submodule init
git submodule update
cd src
make all
```

## Run
In your redis-server run: `loadmodule redistimeseries.so`.

More infomation about modules can be found at redis offical documentation: https://redis.io/topics/modules-intro

## Tests
Tests are written in python using the [rmtest](https://github.com/RedisLabs/rmtest) library.
```
$ cd src
$ pip install -r tests/requirements.txt # optional, use virtualenv
$ make tests
```

## Documentation
Read the docs at http://redistimeseries.io

## Mailing List / Forum
Got questions? Feel free to ask at the [RedisTimeSeries mailing list](https://groups.google.com/forum/#!forum/redistimeseries).

## License
Redis Source Available License Agreement, see [LICENSE](LICENSE)
