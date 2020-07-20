[![Release](https://img.shields.io/github/release/RedisTimeSeries/RedisTimeSeries.svg?kill_cache=1)](https://github.com/RedisTimeSeries/RedisTimeSeries/releases/latest)
[![CircleCI](https://circleci.com/gh/RedisTimeSeries/RedisTimeSeries/tree/master.svg?style=svg)](https://circleci.com/gh/RedisTimeSeries/RedisTimeSeries/tree/master)
[![Docker Cloud Build Status](https://img.shields.io/docker/cloud/build/redislabs/redistimeseries.svg)](https://hub.docker.com/r/redislabs/redistimeseries/builds/)
[![Language grade: C/C++](https://img.shields.io/lgtm/grade/cpp/g/RedisTimeSeries/RedisTimeSeries.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/RedisTimeSeries/RedisTimeSeries/context:cpp)
[![codecov](https://codecov.io/gh/RedisTimeSeries/RedisTimeSeries/branch/master/graph/badge.svg)](https://codecov.io/gh/RedisTimeSeries/RedisTimeSeries)

# RedisTimeSeries
[![Forum](https://img.shields.io/badge/Forum-RedisTimeSeries-blue)](https://forum.redislabs.com/c/modules/redistimeseries)
[![Gitter](https://badges.gitter.im/RedisLabs/RedisTimeSeries.svg)](https://gitter.im/RedisLabs/RedisTimeSeries?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

RedisTimeSeries is a Redis Module adding a Time Series data structure to Redis.

## Features
Read more about the v1.0 GA features [here](https://redislabs.com/blog/redistimeseries-ga-making-4th-dimension-truly-immersive/).
- High volume inserts, low latency reads
- Query by start time and end-time
- Aggregated queries (Min, Max, Avg, Sum, Range, Count, First, Last, STD.P, STD.S, Var.P, Var.S) for any time bucket
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
./deps/readies/bin/getpy2
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

For more information about modules, go to the [redis official documentation](https://redis.io/topics/modules-intro).

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

| Project | Language | License | Author | URL |
| ------- | -------- | ------- | ------ | --- |
| JRedisTimeSeries | Java | BSD-3 | [RedisLabs](https://redislabs.com/) | [Github](https://github.com/RedisTimeSeries/JRedisTimeSeries/) |
| redis-modules-java | Java | Apache-2 | [dengliming](https://github.com/dengliming) | [Github](https://github.com/dengliming/redis-modules-java) |
| redistimeseries-go | Go | Apache-2 | [RedisLabs](https://redislabs.com/) | [Github](https://github.com/RedisTimeSeries/redistimeseries-go) |
| redistimeseries-py | Python | BSD-3 | [RedisLabs](https://redislabs.com/) | [Github](https://github.com/RedisTimeSeries/redistimeseries-py) |
| NRedisTimeSeries | .NET | BSD-3 | [RedisLabs](https://redislabs.com/) | [Github](https://github.com/RedisTimeSeries/NRedisTimeSeries) |
| phpRedisTimeSeries | PHP | MIT | [Alessandro Balasco](https://github.com/palicao) | [Github](https://github.com/palicao/phpRedisTimeSeries) |
| redis-time-series | JavaScript | MIT | [Rafa Campoy](https://github.com/averias) | [Github](https://github.com/averias/redis-time-series) |
| redistimeseries-js | JavaScript | MIT | [Milos Nikolovski](https://github.com/nikolovskimilos) | [Github](https://github.com/nikolovskimilos/redistimeseries-js) |
| redis_ts | Rust | BSD-3 | [Thomas Profelt](https://github.com/tompro) | [Github](https://github.com/tompro/redis_ts) |
| redistimeseries | Ruby | MIT | [Eaden McKee](https://github.com/eadz) | [Github](https://github.com/eadz/redistimeseries) |
| redis-time-series | Ruby | MIT | [Matt Duszynski](https://github.com/dzunk) | [Github](https://github.com/dzunk/redis-time-series) |

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
Got questions? Feel free to ask at the [RedisTimeSeries forum](https://forum.redislabs.com/c/modules/redistimeseries).

## License
Redis Source Available License Agreement, see [LICENSE](LICENSE)
