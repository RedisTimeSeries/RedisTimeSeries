---
title: "Quickstart"
linkTitle: "Quickstart"
weight: 2
description: >
    Quick Start Guide to RedisTimeSeries
---

## Setup

You can either get RedisTimeSeries setup in the cloud, in a Docker container or on your own machine.

### Redis Cloud

RedisTimeSeries is available on all Redis Cloud managed services, including a completely free managed database up to 30MB.

[Get started here](https://redislabs.com/try-free/)


### Docker
To quickly try out RedisTimeSeries, launch an instance using docker:
```sh
docker run -p 6379:6379 -it --rm redislabs/redistimeseries
```

### Download and running binaries

First download the pre-compiled version from the [Redis download center](https://redis.com/redis-enterprise-software/download-center/modules/).

Next, run Redis with RedisTimeSeries: 

```
$ redis-server --loadmodule /path/to/module/redistimeseries.so
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

For more information about modules, go to the [redis official documentation](https://redis.io/topics/modules-intro).

## Give it a try with `redis-cli`

After you setup RedisTimeSeries, you can interact with it using redis-cli.

```sh
$ redis-cli
127.0.0.1:6379> TS.CREATE sensor1
OK
```


## Creating a timeseries
A new timeseries can be created with the `TS.CREATE` command; for example, to create a timeseries named `sensor1` run the following:

```
TS.CREATE sensor1
```

You can prevent your timeseries growing indefinitely by setting a maximum age for samples compared to the last event time (in milliseconds) with the `RETENTION` option. The default value for retention is `0`, which means the series will not be trimmed.  

```
TS.CREATE sensor1 RETENTION 2678400000
```
This will create a timeseries called `sensor1` and trim it to values of up to one month.


## Adding data points
For adding new data points to a timeseries we use the `TS.ADD` command:

```
TS.ADD key timestamp value
```

The `timestamp` argument is the UNIX timestamp of the sample in milliseconds and `value` is the numeric data value of the sample. 

Example:
```
TS.ADD sensor1 1626434637914 26
```

To **add a datapoint with the current timestamp** you can use a `*` instead of a specific timestamp:

```
TS.ADD sensor1 * 26
```

You can **append data points to multiple timeseries** at the same time with the `TS.MADD` command:
```
TS.MADD key timestamp value [key timestamp value ...]
```


## Deleting data points
Data points between two timestamps (inclusive) can be deleted with the `TS.DEL` command:
```
TS.DEL key fromTimestamp toTimestamp
```
Example:
```
TS.DEL sensor1 1000 2000
```

To delete a single timestamp, use it as both the "from" and "to" timestamp:
```
TS.DEL sensor1 1000 1000
```

**Note:** When a sample is deleted, the data in all downsampled timeseries will be recalculated for the specific bucket. If part of the bucket has already been removed though, because it's outside of the retention period, we won't be able to recalculate the full bucket, so in those cases we will refuse the delete operation.


## Labels
Labels are key-value metadata we attach to data points, allowing us to group and filter. They can be either string or numeric values and are added to a timeseries on creation:

```
TS.CREATE sensor1 LABELS region east
```



## Downsampling
Another useful feature of RedisTimeSeries is compacting data by creating a rule for downsampling (`TS.CREATERULE`). For example, if you have collected more than one billion data points in a day, you could aggregate the data by every minute in order to downsample it, thereby reducing the dataset size to 24 * 60 = 1,440 data points. You can choose one of the many available aggregation types in order to aggregate multiple data points from a certain minute into a single one. The currently supported aggregation types are: `avg, sum, min, max, range, count, first, last, std.p, std.s, var.p, var.s and twa`.
 
It's important to point out that there is no data rewriting on the original timeseries; the compaction happens in a new series, while the original one stays the same. In order to prevent the original timeseries from growing indefinitely, you can use the retention option, which will trim it down to a certain period of time.

**NOTE:** You need to create the destination (the compacted) timeseries before creating the rule.

```
TS.CREATERULE sourceKey destKey AGGREGATION aggregationType bucketDuration
```

Example:

```
TS.CREATE sensor1_compacted  # Create the destination timeseries first
TS.CREATERULE sensor1 sensor1_compacted AGGREGATION avg 60000   # Create the rule
```

With this creation rule, datapoints added to the `sensor1` timeseries will be grouped into buckets of 60 seconds (60000ms), averaged, and saved in the `sensor1_compacted` timeseries.


## Filtering
RedisTimeSeries allows to filter by value, timestamp and by labels:

### Filtering by label
You can retrieve datapoints from multiple timeseries in the same query, and the way to do this is by using label filters. For example:

```
TS.MRANGE - + FILTER area_id=32
```

This query will show data from all sensors (timeseries) that have a label of `area_id` with a value of `32`. The results will be grouped by timeseries.

Or we can also use the `TS.MGET` command to get the last sample that matches the specific filter:

```
TS.MGET FILTER area_id=32
```

### Filtering by value
We can filter by value across a single or multiple timeseries:

```
TS.RANGE sensor1 - + FILTER_BY_VALUE 25 30
```
This command will return all data points whose value sits between 25 and 30, inclusive.

To achieve the same filtering on multiple series we have to combine the filtering by value with filtering by label:

```
TS.MRANGE - +  FILTER_BY_VALUE 20 30 FILTER region=east
```

### Filtering by timestamp
To retrieve the datapoints for specific timestamps on one or multiple timeseries we can use the `FILTER_BY_TS` argument:

Filter on one timeseries:
```
TS.RANGE sensor1 - + FILTER_BY_TS 1626435230501 1626443276598
```

Filter on multiple timeseries:
```
TS.MRANGE - +  FILTER_BY_TS 1626435230501 1626443276598 FILTER region=east
```


## Aggregation
It's possible to combine values of one or more timeseries by leveraging aggregation functions:
```
TS.RANGE ... AGGREGATION aggType bucketDuration...
```

For example, to find the average temperature per hour in our `sensor1` series we could run:
```
TS.RANGE sensor1 - + + AGGREGATION avg 3600000
```

To achieve the same across multiple sensors from the area with id of 32 we would run:
```
TS.MRANGE - + AGGREGATION avg 3600000 FILTER area_id=32
```

### Aggregation bucket alignment
When doing aggregations, the aggregation buckets will be aligned to 0 as so:
```
TS.RANGE sensor3 10 70 + AGGREGATION min 25
```

```
Value:        |      (1000)     (2000)     (3000)     (4000)     (5000)     (6000)     (7000)
Timestamp:    |-------|10|-------|20|-------|30|-------|40|-------|50|-------|60|-------|70|--->  

Bucket(25ms): |_________________________||_________________________||___________________________|
                           V                          V                           V
                  min(1000, 2000)=1000      min(3000, 4000)=3000     min(5000, 6000, 7000)=5000                
```

And we will get the following datapoints: 1000, 3000, 5000.

You can choose to align the buckets to the start or end of the queried interval as so:
```
TS.RANGE sensor3 10 70 + AGGREGATION min 25 ALIGN start
```

```
Value:        |      (1000)     (2000)     (3000)     (4000)     (5000)     (6000)     (7000)
Timestamp:    |-------|10|-------|20|-------|30|-------|40|-------|50|-------|60|-------|70|--->  

Bucket(25ms):          |__________________________||_________________________||___________________________|
                                    V                          V                           V
                        min(1000, 2000, 3000)=1000      min(4000, 5000)=4000     min(6000, 7000)=6000                
```
The result array will contain the following datapoints: 1000, 4000 and 6000


### Aggregation across timeseries

By default, results of multiple timeseries will be grouped by timeseries, but (since v1.6) you can use the `GROUPBY` and `REDUCE` options to group them by label and apply an additional aggregation.

To find minimum temperature per region, for example, we can run:

```
TS.MRANGE - + FILTER region=(east,west) GROUPBY region REDUCE min
```
