# RedisTimeSeries Client Libraries

RedisTimeSeries has several client libraries, written by the module authors and community members - abstracting the API in different programming languages. 

While it is possible and simple to use the raw Redis commands API, in most cases it's more convenient to use a client library abstracting it. 

## Currently available Libraries

Some languages have client libraries that provide support for RedisTimeSeries commands:

| Project | Language | License | Author | Stars |
| ------- | -------- | ------- | ------ | --- |
| [JRedisTimeSeries][JRedisTimeSeries-url] | Java | BSD-3 | [RedisLabs][JRedisTimeSeries-author] |  [![JRedisTimeSeries-stars]][JRedisTimeSeries-url] |
| [redis-modules-java][redis-modules-java-url] | Java | Apache-2 | [dengliming][redis-modules-java-author] | [![redis-modules-java-stars]][redis-modules-java-url] |
| [redistimeseries-go][redistimeseries-go-url] | Go | Apache-2 | [RedisLabs][redistimeseries-go-author] |  [![redistimeseries-go-stars]][redistimeseries-go-url]  |
| [redistimeseries-py][redistimeseries-py-url] | Python | BSD-3 | [RedisLabs][redistimeseries-py-author] | [![redistimeseries-py-stars]][redistimeseries-py-url] |
| [NRedisTimeSeries][NRedisTimeSeries-url] | .NET | BSD-3 | [RedisLabs][NRedisTimeSeries-author] |  [![NRedisTimeSeries-stars]][NRedisTimeSeries-url] |
| [phpRedisTimeSeries][phpRedisTimeSeries-url] | PHP | MIT | [Alessandro Balasco][phpRedisTimeSeries-author] |  [![phpRedisTimeSeries-stars]][phpRedisTimeSeries-url] |
| [redis-time-series][redis-time-series-url] | JavaScript | MIT | [Rafa Campoy][redis-time-series-author] | [![redis-time-series-stars]][redis-time-series-url] |
| [redistimeseries-js][redistimeseries-js-url] | JavaScript | MIT | [Milos Nikolovski][redistimeseries-js-author] | [![redistimeseries-js-stars]][redistimeseries-js-url] |
| [redis-modules-sdk][redis-modules-sdk-url] | Typescript | BSD-3-Clause | [Dani Tseitlin][redis-modules-sdk-author] |[![redis-modules-sdk-stars]][redis-modules-sdk-url]| 
| [redis_ts][redis_ts-url] | Rust | BSD-3 | [Thomas Profelt][redis_ts-author] | [![redis_ts-stars]][redis_ts-url] |
| [redistimeseries][redistimeseries-url] | Ruby | MIT | [Eaden McKee][redistimeseries-author] | [![redistimeseries-stars]][redistimeseries-url] |
| [redis-time-series][redis-time-series-rb-url] | Ruby | MIT | [Matt Duszynski][redis-time-series-rb-author] | [![redis-time-series-rb-stars]][redis-time-series-rb-url] |

[JRedisTimeSeries-url]: https://github.com/RedisTimeSeries/JRedisTimeSeries/
[JRedisTimeSeries-author]: https://redislabs.com
[JRedisTimeSeries-stars]: https://img.shields.io/github/stars/RedisTimeSeries/JRedisTimeSeries.svg?style=social&amp;label=Star&amp;maxAge=2592000

[NRedisTimeSeries-url]: https://github.com/RedisTimeSeries/NRedisTimeSeries
[NRedisTimeSeries-author]: https://redislabs.com
[NRedisTimeSeries-stars]: https://img.shields.io/github/stars/RedisTimeSeries/NRedisTimeSeries.svg?style=social&amp;label=Star&amp;maxAge=2592000


[redis-modules-java-url]: https://github.com/dengliming/redis-modules-java
[redis-modules-java-author]: https://github.com/dengliming
[redis-modules-java-stars]: https://img.shields.io/github/stars/dengliming/redis-modules-java.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redistimeseries-go-url]: https://github.com/RedisTimeSeries/redistimeseries-go/
[redistimeseries-go-author]: https://redislabs.com
[redistimeseries-go-stars]: https://img.shields.io/github/stars/RedisTimeSeries/redistimeseries-go.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redistimeseries-py-url]: https://github.com/RedisTimeSeries/redistimeseries-py/
[redistimeseries-py-author]: https://redislabs.com
[redistimeseries-py-stars]: https://img.shields.io/github/stars/RedisTimeSeries/redistimeseries-py.svg?style=social&amp;label=Star&amp;maxAge=2592000

[NRedisTimeSeries-url]: https://github.com/RedisTimeSeries/NRedisTimeSeries/
[NRedisTimeSeries-author]: https://redislabs.com
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
