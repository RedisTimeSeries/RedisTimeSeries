[![CircleCI](https://circleci.com/gh/RedisLabsModules/RedisTimeSeries/tree/master.svg?style=svg)](https://circleci.com/gh/RedisLabsModules/RedisTimeSeries/tree/master)
[![GitHub issues](https://img.shields.io/github/release/RedisLabsModules/redis-timeseries.svg)](https://github.com/RedisLabsModules/redis-timeseries/releases/latest)

# Redis Time-Series Module
Time series data structure for redis.

## Using with other tools metrics tools
See [RedisTimeSeries](https://github.com/RedisTimeSeries) organization.
Including Integration with:

1. StatsD, Graphite exports using graphite protocol.
2. Grafana - using SimpleJson datasource.

## Memory model
A time series is a linked list of memory chunks.
Each chunk has a predefined size of samples, each sample is a tuple of the time and the value.
Each sample is the size of 128bit (64bit for the timestamp and 64bit for the value).

## Features
* Quick inserts (50K samples per sec)
* Query by start time and end-time
* Query by labels sets
* Aggregated queries (Min, Max, Avg, Sum, Range, Count, First, Last) for any time bucket
* Configurable max retention period
* Compactions/Roll-ups - automatically updated aggregated timeseries
* labels index - each key has labels which will allows query by labels

## Docker

To quickly tryout Redis-TimeSeries, launch an instance using docker:

```sh
docker run -p 6379:6379 -it --rm redislabs/redistimeseries
```

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

## Give it a try

After you load RedisTimeSeries, you can interact with it using redis-cli.

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
