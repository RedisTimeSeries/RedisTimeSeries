[![GitHub issues](https://img.shields.io/github/release/RedisTimeSeries/RedisTimeSeries.svg?kill_cache=1)](https://github.com/RedisTimeSeries/RedisTimeSeries/releases/latest)
[![CircleCI](https://circleci.com/gh/RedisTimeSeries/RedisTimeSeries/tree/master.svg?style=svg)](https://circleci.com/gh/RedisTimeSeries/RedisTimeSeries/tree/master)
[![Docker Cloud Build Status](https://img.shields.io/docker/cloud/build/redislabs/redistimeseries.svg)](https://hub.docker.com/r/redislabs/redistimeseries/builds/)

# RedisTimeSeries
RedisTimeSeries is a Redis Module adding a Time Series data structure to Redis.

## Features
Read more about the v1.0 GA features [here](https://redislabs.com/blog/redistimeseries-ga-making-4th-dimension-truly-immersive/).
- High volume inserts, low latency reads
- Query by start time and end-time
- Aggregated queries (Min, Max, Avg, Sum, Range, Count, First, Last) for any time bucket
- Configurable maximum retention period
- Downsampling/Compaction - automatically updated aggregated timeseries
- Secondary index - each time series has labels (field value pairs) which will allows to query by labels

## Using with other tools metrics tools
In the [RedisTimeSeries](https://github.com/RedisTimeSeries) organization you can
find projects that help you integrate RedisTimeSeries with other tools, including:

1. [Prometheus](https://github.com/RedisTimeSeries/prometheus-redistimeseries-adapter) - read/write adapter to use RedisTimeSeries as backend db.
2. [Grafana](https://github.com/RedisTimeSeries/grafana-redistimeseries) - using a SimpleJson datasource.
3. [Telegraph](https://github.com/RedisTimeSeries/telegraf)
4. StatsD, Graphite exports using graphite protocol.

## Memory model
A time series is a linked list of memory chunks.
Each chunk has a predefined size of samples.
Each sample is a tuple of the time and the value of 128 bits,
64 bits for the timestamp and 64 bits for the value.

## Setup

You can either get RedisTimeSeries setup in a Docker container or on your own machine.

### Docker
To quickly try out RedisTimeSeries, launch an instance using docker:
```sh
docker run -p 6379:6379 -it --rm redislabs/redistimeseries
```

### Build and Run it yourself

You can also build and run RedisTimeSeries on your own machine.

#### Requirements
-  build-essential
-  The RedisTimeSeries repository: `git clone https://github.com/RedisTimeSeries/RedisTimeSeries.git`

#### Build

```bash
cd RedisTimeSeries
git submodule init
git submodule update
cd src
make all
```

#### Run

In your redis-server run: `loadmodule redistimeseries.so`

For more information about modules, go to the [redis official documentation](https://redis.io/topics/modules-intro).

## Give it a try

After you setup RedisTimeSeries, you can interact with it using redis-cli.

Here we'll create a time series representing sensor temperature measurements. 
After you create the time series, you can send temperature measurements.
Then you can query the data for a time range on some aggregation rule.

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

Some languages have client libraries that provide support for RedisTimeSeries commands:

| Project | Language | License | Author | URL |
| ------- | -------- | ------- | ------ | --- |
| JRedisTimeSeries | Java | BSD-3 | [RedisLabs](https://redislabs.com/) | [Github](https://github.com/RedisTimeSeries/JRedisTimeSeries/) |
| redistimeseries-go | Go | Apache-2 | [RedisLabs](https://redislabs.com/) | [Github](https://github.com/RedisTimeSeries/redistimeseries-go) |
| redistimeseries-py | Python | BSD-3 | [RedisLabs](https://redislabs.com/) | [Github](https://github.com/RedisTimeSeries/redistimeseries-py) |

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
