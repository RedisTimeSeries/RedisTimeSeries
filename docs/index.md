# Redis Time-Series Module
Time series data structure for Redis.

## Using with other tools metrics tools
See [Tools](tools/) directory.
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
* Aggregated queries (Min, Max, Avg, Sum, Range, Count, First, Last) for any time bucket
* Configurable max retention period
* Compactions/Roll-ups - automatically updated aggregated timeseries


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
In your redis-server run: `loadmodule redis-tsdb-module.so`.

More infomation about modules can be found at redis offical documentation: https://redis.io/topics/modules-intro

### Tests
Tests are written in python using the [rmtest](https://github.com/RedisLabs/rmtest) library.
```
$ cd src
$ pip install -r tests/requirements.txt # optional, use virtualenv
$ make test
```

### Client libraries

Some languages have client libraries that provide support for RedisTimeSeries's commands:

| Project | Language | License | Author | URL |
| ------- | -------- | ------- | ------ | --- |

## Mailing List / Forum

Got questions? Feel free to ask at the [RediGraph mailing list](https://groups.google.com/forum/#!forum/redisgraph).

## License

Apache 2.0 with Commons Clause - see [LICENSE](https://raw.githubusercontent.com/RedisLabsModules/RedisGraph/master/LICENSE)
