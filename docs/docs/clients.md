---
title: "Clients"
linkTitle: "Clients"
weight: 5
description: >
    Time Series Client Libraries
aliases:
    - /docs/stack/timeseries/clients    
---

You can use Redis time series with several client libraries, written by the module authors and community members - abstracting the API in different programming languages. 

While it is possible and simple to use the raw Redis commands API, in most cases it's more convenient to use a client library abstracting it. 

## Currently available Libraries

Some languages have client libraries that provide support for the time series commands:

| Project | Language | License | Author | Stars | Package |
| ------- | -------- | ------- | ------ | --- | --- |
| [jedis][jedis-url] | Java | MIT | [Redis][redis-url] | ![Stars][jedis-stars] | [Maven][jedis-package]||
| [redis-py][redis-py-url] | Python | MIT | [Redis][redis-url] | ![Stars][redis-py-stars] | [pypi][redis-py-package]||
| [node-redis][node-redis-url] | Node.js | MIT | [Redis][redis-url] | ![Stars][node-redis-stars] | [npm][node-redis-package]||
| [nredisstack][nredisstack-url] | .NET | MIT | [Redis][redis-url] | ![Stars][nredisstack-stars] | [nuget][nredisstack-package]||
| [redistimeseries-go][redistimeseries-go-url] | Go | Apache-2 | [Redis][redistimeseries-go-author] |  [![redistimeseries-go-stars]][redistimeseries-go-url]  | [GitHub][redistimeseries-go-url] |
| [rueidis][rueidis-url] | Go | Apache-2 | [Rueian][rueidis-author] |  [![rueidis-stars]][rueidis-url]  | [GitHub][rueidis-url] |
| [phpRedisTimeSeries][phpRedisTimeSeries-url] | PHP | MIT | [Alessandro Balasco][phpRedisTimeSeries-author] |  [![phpRedisTimeSeries-stars]][phpRedisTimeSeries-url] | [GitHub][phpRedisTimeSeries-url] |
| [redis-time-series][redis-time-series-url] | JavaScript | MIT | [Rafa Campoy][redis-time-series-author] | [![redis-time-series-stars]][redis-time-series-url] | [GitHub][redis-time-series-url] |
| [redistimeseries-js][redistimeseries-js-url] | JavaScript | MIT | [Milos Nikolovski][redistimeseries-js-author] | [![redistimeseries-js-stars]][redistimeseries-js-url] | [GitHub][redistimeseries-js-url] |
| [redis_ts][redis_ts-url] | Rust | BSD-3 | [Thomas Profelt][redis_ts-author] | [![redis_ts-stars]][redis_ts-url] | [GitHub][redis_ts-url] | 
| [redistimeseries][redistimeseries-url] | Ruby | MIT | [Eaden McKee][redistimeseries-author] | [![redistimeseries-stars]][redistimeseries-url] | [GitHub][redistimeseries-url] |
| [redis-time-series][redis-time-series-rb-url] | Ruby | MIT | [Matt Duszynski][redis-time-series-rb-author] | [![redis-time-series-rb-stars]][redis-time-series-rb-url] | [GitHub][redis-time-series-rb-url] |

[redis-url]: https://redis.com

[redis-py-url]: https://github.com/redis/redis-py
[redis-py-stars]: https://img.shields.io/github/stars/redis/redis-py.svg?style=social&amp;label=Star&amp;maxAge=2592000
[redis-py-package]: https://pypi.python.org/pypi/redis

[jedis-url]: https://github.com/redis/jedis
[jedis-stars]: https://img.shields.io/github/stars/redis/jedis.svg?style=social&amp;label=Star&amp;maxAge=2592000
[Jedis-package]: https://search.maven.org/artifact/redis.clients/jedis

[nredisstack-url]: https://github.com/redis/nredisstack
[nredisstack-stars]: https://img.shields.io/github/stars/redis/nredisstack.svg?style=social&amp;label=Star&amp;maxAge=2592000
[nredisstack-package]: https://www.nuget.org/packages/nredisstack/

[node-redis-url]: https://github.com/redis/node-redis
[node-redis-stars]: https://img.shields.io/github/stars/redis/node-redis.svg?style=social&amp;label=Star&amp;maxAge=2592000
[node-redis-package]: https://www.npmjs.com/package/redis


[redistimeseries-go-url]: https://github.com/RedisTimeSeries/redistimeseries-go/
[redistimeseries-go-author]: https://redis.com
[redistimeseries-go-stars]: https://img.shields.io/github/stars/RedisTimeSeries/redistimeseries-go.svg?style=social&amp;label=Star&amp;maxAge=2592000

[rueidis-url]: https://github.com/rueian/rueidis
[rueidis-author]: https://github.com/rueian
[rueidis-stars]: https://img.shields.io/github/stars/rueian/rueidis.svg?style=social&amp;label=Star&amp;maxAge=2592000

[phpRedisTimeSeries-url]: https://github.com/palicao/phpRedisTimeSeries
[phpRedisTimeSeries-author]: https://github.com/palicao
[phpRedisTimeSeries-stars]: https://img.shields.io/github/stars/palicao/phpRedisTimeSeries.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redis-time-series-url]: https://github.com/averias/redis-time-series
[redis-time-series-author]: https://github.com/averias
[redis-time-series-stars]: https://img.shields.io/github/stars/averias/redis-time-series.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redistimeseries-js-url]: https://github.com/nikolovskimilos/redistimeseries-js
[redistimeseries-js-author]: https://github.com/nikolovskimilos
[redistimeseries-js-stars]: https://img.shields.io/github/stars/nikolovskimilos/redistimeseries-js.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redis_ts-url]: https://github.com/tompro/redis_ts
[redis_ts-author]: https://github.com/tompro
[redis_ts-stars]: https://img.shields.io/github/stars/tompro/redis_ts.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redistimeseries-url]: https://github.com/eadz/redistimeseries
[redistimeseries-author]: https://github.com/eadz
[redistimeseries-stars]: https://img.shields.io/github/stars/eadz/redistimeseries.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redis-time-series-rb-url]: https://github.com/dzunk/redis-time-series
[redis-time-series-rb-author]: https://github.com/dzunk
[redis-time-series-rb-stars]: https://img.shields.io/github/stars/dzunk/redis-time-series.svg?style=social&amp;label=Star&amp;maxAge=2592000

[rustis-url]: https://github.com/dahomey-technologies/rustis
[rustis-author]: https://github.com/dahomey-technologies
[rustis-stars]: https://img.shields.io/github/stars/dahomey-technologies/rustis.svg?style=social&amp;label=Star&amp;maxAge=2592000