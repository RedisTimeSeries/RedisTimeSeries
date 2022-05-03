[![Release](https://img.shields.io/github/release/RedisTimeSeries/RedisTimeSeries.svg?sort=semver&kill_cache=1)](https://github.com/RedisTimeSeries/RedisTimeSeries/releases/latest)
[![CircleCI](https://circleci.com/gh/RedisTimeSeries/RedisTimeSeries/tree/master.svg?style=svg)](https://circleci.com/gh/RedisTimeSeries/RedisTimeSeries/tree/master)
[![Dockerhub](https://img.shields.io/badge/dockerhub-redislabs%2Fredistimeseries-blue)](https://hub.docker.com/r/redislabs/redistimeseries/tags/)
[![Language grade: C/C++](https://img.shields.io/lgtm/grade/cpp/g/RedisTimeSeries/RedisTimeSeries.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/RedisTimeSeries/RedisTimeSeries/context:cpp)
[![codecov](https://codecov.io/gh/RedisTimeSeries/RedisTimeSeries/branch/master/graph/badge.svg)](https://codecov.io/gh/RedisTimeSeries/RedisTimeSeries)

# RedisTimeSeries
[![Forum](https://img.shields.io/badge/Forum-RedisTimeSeries-blue)](https://forum.redis.com/c/modules/redistimeseries)
[![Discord](https://img.shields.io/discord/697882427875393627?style=flat-square)](https://discord.gg/KExRgMb)

RedisTimeSeries is a time-series database (TSDB) module for Redis, by Redis.

RedisTimeSeries can hold multiple time series. Each time series is accessible via a single Redis key (similar to any other Redis data structure).

## What is a Redis time series?

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
- Aggregated queries (Min, Max, Avg, Sum, Range, Count, First, Last, STD.P, STD.S, Var.P, Var.S) for any time bucket
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

Major Linux distributions as well as macOS are supported.

#### Requirements

First, clone the RedisTimeSeries repository from git:

```
git clone --recursive https://github.com/RedisTimeSeries/RedisTimeSeries.git
```

Then, to install required build artifacts, invoke the following:

```
cd RedisTimeSeries
make setup
```
Or you can install required dependencies manually listed in [system-setup.py](https://github.com/RedisTimeSeries/RedisTimeSeries/blob/master/system-setup.py).

If ```make``` is not yet available, the following commands are equivalent:

```
./deps/readies/bin/getpy3
./system-setup.py
```

Note that ```system-setup.py``` **will install various packages on your system** using the native package manager and pip. This requires root permissions (i.e. sudo) on Linux.

If you prefer to avoid that, you can:

* Review system-setup.py and install packages manually,
* Utilize a Python virtual environment,
* Use Docker with the ```--volume``` option to create an isolated build environment.

#### Build

```bash
make build
```

Binary artifacts are placed under the ```bin``` directory.

#### Run

In your redis-server run: `loadmodule bin/redistimeseries.so`

For more information about modules, go to the [Redis official documentation](https://redis.io/topics/modules-intro).

## Give it a try

After you setup RedisTimeSeries, you can interact with it using redis-cli.

Here we'll create a time series representing sensor temperature measurements.
After you create the time series, you can send temperature measurements.
Then you can query the data for a time range on some aggregation rule.

### With `redis-cli`
```sh
$ redis-cli
127.0.0.1:6379> TS.CREATE temperature:3:11 RETENTION 60 LABELS sensor_id 2 area_id 32
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

| Project | Language | License | Author | Stars | Comment |
| ------- | -------- | ------- | ------ | --- | --- |
| [Jedis][Jedis-url] | Java | MIT | [Redis][Jedis-author] |  [![Jedis-stars]][Jedis-url] |
| [JRedisTimeSeries][JRedisTimeSeries-url] | Java | BSD-3 | [Redis][JRedisTimeSeries-author] |  [![JRedisTimeSeries-stars]][JRedisTimeSeries-url] | Deprecated |
| [redis-modules-java][redis-modules-java-url] | Java | Apache-2 | [dengliming][redis-modules-java-author] | [![redis-modules-java-stars]][redis-modules-java-url] |
| [redistimeseries-go][redistimeseries-go-url] | Go | Apache-2 | [Redis][redistimeseries-go-author] |  [![redistimeseries-go-stars]][redistimeseries-go-url]  |
| [rueidis][rueidis-url] | Go | Apache-2 | [Rueian][rueidis-author] |  [![rueidis-stars]][rueidis-url]  |
| [redis-py][redis-py-url] | Python | MIT | [Redis][redis-py-author] | [![redis-py-stars]][redis-py-url] |
| [NRedisTimeSeries][NRedisTimeSeries-url] | .NET | BSD-3 | [Redis][NRedisTimeSeries-author] |  [![NRedisTimeSeries-stars]][NRedisTimeSeries-url] |
| [phpRedisTimeSeries][phpRedisTimeSeries-url] | PHP | MIT | [Alessandro Balasco][phpRedisTimeSeries-author] |  [![phpRedisTimeSeries-stars]][phpRedisTimeSeries-url] |
| [redis-time-series][redis-time-series-url] | JavaScript | MIT | [Rafa Campoy][redis-time-series-author] | [![redis-time-series-stars]][redis-time-series-url] |
| [redistimeseries-js][redistimeseries-js-url] | JavaScript | MIT | [Milos Nikolovski][redistimeseries-js-author] | [![redistimeseries-js-stars]][redistimeseries-js-url] |
| [redis-modules-sdk][redis-modules-sdk-url] | Typescript | BSD-3-Clause | [Dani Tseitlin][redis-modules-sdk-author] |[![redis-modules-sdk-stars]][redis-modules-sdk-url]|
| [redis_ts][redis_ts-url] | Rust | BSD-3 | [Thomas Profelt][redis_ts-author] | [![redis_ts-stars]][redis_ts-url] |
| [redistimeseries][redistimeseries-url] | Ruby | MIT | [Eaden McKee][redistimeseries-author] | [![redistimeseries-stars]][redistimeseries-url] |
| [redis-time-series][redis-time-series-rb-url] | Ruby | MIT | [Matt Duszynski][redis-time-series-rb-author] | [![redis-time-series-rb-stars]][redis-time-series-rb-url] |

[Jedis-url]: https://github.com/redis/jedis/
[Jedis-author]: https://redis.com
[Jedis-stars]: https://img.shields.io/github/stars/redis/jedis.svg?style=social&amp;label=Star&amp;maxAge=2592000

[JRedisTimeSeries-url]: https://github.com/RedisTimeSeries/JRedisTimeSeries/
[JRedisTimeSeries-author]: https://redis.com
[JRedisTimeSeries-stars]: https://img.shields.io/github/stars/RedisTimeSeries/JRedisTimeSeries.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redis-modules-java-url]: https://github.com/dengliming/redis-modules-java
[redis-modules-java-author]: https://github.com/dengliming
[redis-modules-java-stars]: https://img.shields.io/github/stars/dengliming/redis-modules-java.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redistimeseries-go-url]: https://github.com/RedisTimeSeries/redistimeseries-go/
[redistimeseries-go-author]: https://redis.com
[redistimeseries-go-stars]: https://img.shields.io/github/stars/RedisTimeSeries/redistimeseries-go.svg?style=social&amp;label=Star&amp;maxAge=2592000

[rueidis-url]: https://github.com/rueian/rueidis
[rueidis-author]: https://github.com/rueian
[rueidis-stars]: https://img.shields.io/github/stars/rueian/rueidis.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redis-py-url]: https://github.com/redis/redis-py/
[redis-py-author]: https://redis.com
[redis-py-stars]: https://img.shields.io/github/stars/redis/redis-py.svg?style=social&amp;label=Star&amp;maxAge=2592000

[NRedisTimeSeries-url]: https://github.com/RedisTimeSeries/NRedisTimeSeries/
[NRedisTimeSeries-author]: https://redis.com
[NRedisTimeSeries-stars]: https://img.shields.io/github/stars/RedisTimeSeries/NRedisTimeSeries.svg?style=social&amp;label=Star&amp;maxAge=2592000

[phpRedisTimeSeries-url]: https://github.com/palicao/phpRedisTimeSeries
[phpRedisTimeSeries-author]: https://github.com/palicao
[phpRedisTimeSeries-stars]: https://img.shields.io/github/stars/palicao/phpRedisTimeSeries.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redis-time-series-url]: https://github.com/averias/redis-time-series
[redis-time-series-author]: https://github.com/averias
[redis-time-series-stars]: https://img.shields.io/github/stars/averias/redis-time-series.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redistimeseries-js-url]: https://github.com/nikolovskimilos/redistimeseries-js
[redistimeseries-js-author]: https://github.com/nikolovskimilos
[redistimeseries-js-stars]: https://img.shields.io/github/stars/nikolovskimilos/redistimeseries-js.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redis-modules-sdk-url]: https://github.com/danitseitlin/redis-modules-sdk
[redis-modules-sdk-author]: https://github.com/danitseitlin
[redis-modules-sdk-stars]: https://img.shields.io/github/stars/danitseitlin/redis-modules-sdk.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redis_ts-url]: https://github.com/tompro/redis_ts
[redis_ts-author]: https://github.com/tompro
[redis_ts-stars]: https://img.shields.io/github/stars/tompro/redis_ts.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redistimeseries-url]: https://github.com/eadz/redistimeseries
[redistimeseries-author]: https://github.com/eadz
[redistimeseries-stars]: https://img.shields.io/github/stars/eadz/redistimeseries.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redis-time-series-rb-url]: https://github.com/dzunk/redis-time-series
[redis-time-series-rb-author]: https://github.com/dzunk
[redis-time-series-rb-stars]: https://img.shields.io/github/stars/dzunk/redis-time-series.svg?style=social&amp;label=Star&amp;maxAge=2592000

## Tests

The module includes a basic set of unit tests and integration tests.

**Unit tests**

To run all unit tests, follow these steps:

    $ make unittests


**Integration tests**


Integration tests are based on [RLTest](https://github.com/RedisLabsModules/RLTest), and specific setup parameters can be provided
to configure tests. By default the tests will be ran for all common commands, and with variation of persistency and replication.


To run all integration tests in a Python virtualenv, follow these steps:

    $ mkdir -p .env
    $ virtualenv .env
    $ source .env/bin/activate
    $ pip install -r tests/flow/requirements.txt
    $ make test

To understand what test options are available simply run:

    $ make help

For example, to run the tests strictly desigined for TS.ADD command, follow these steps:

    $ make test TEST=test_ts_add.py


## Documentation
Read the docs at http://redistimeseries.io

## Mailing List / Forum
Got questions? Feel free to ask at the [RedisTimeSeries forum](https://forum.redis.com/c/modules/redistimeseries).

## License
Redis Source Available License Agreement, see [LICENSE](LICENSE)
