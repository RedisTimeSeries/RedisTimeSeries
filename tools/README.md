# Tools

## Overview
This folder contains some tools to be used with other metrics collection systems, and a Grafana datasource.

## GraphiteServer.py
### Overview
A python based, greenlet TCP server for accepting incoming metrics in graphite protocol

### Protocol
```
<metric path> <metric value> <metric timestamp>
```
Where
* metric path - String
* metric value - Float
* metric timestamp - int

### Usage
```
usage: GraphiteServer.py [-h] [--host HOST] [--port PORT]
                         [--redis-server REDIS_SERVER]
                         [--redis-port REDIS_PORT]
                         [--max-retention MAX_RETENTION]
                         [--samples-per-chunk SAMPLES_PER_CHUNK]

optional arguments:
  -h, --help            show this help message and exit
  --host HOST           server address to listen to
  --port PORT           port number to listen to
  --redis-server REDIS_SERVER
                        redis server address
  --redis-port REDIS_PORT
                        redis server port
  --max-retention MAX_RETENTION
                        default retention time (in seconds)
  --samples-per-chunk SAMPLES_PER_CHUNK
                        default samples per memory chunk

```

### Dependencies
To install the needed dependencies just run: `pip install -r requirements.txt`

### Usage with Statsd
Since this tools uses the same protocol as graphite, just configure your StatsD server to send it to the appropriate server.

#### Example
GraphiteServer.py cmd:
```
./GraphiteServer.py --host localhost --port 16000 --redis-server localhost --redis-port 6397
```

StatsD configuration:
```
{
  graphitePort: 16000
, graphiteHost: "localhost"
, port: 8125
, backends: [ "./backends/graphite" ]
}
```

#### Note regarding metric naming
The metrics names will be exactly as StatsD sends them for example: `stats.gauges.load`.
There's no way at the moment to query the keys as part of a tree, so to get the metrics data you should use the fullpath:
```
ts.range stats.gauges.load 1487262000 1487262890
```

## Grafana Datastore API Server
https://github.com/RedisTimeSeries/grafana-redistimeseries
