---
title: "Commands"
linkTitle: "Commands"
weight: 1
description: >
    Commands Overview
---

### RedisTimeSeries API

Details on module's [commands](/commands/?group=timeseries) can be filtered for a specific module or command, e.g., [`TS.CREATE`](/commands/?group=timeseries&name=ts.create).
The details also include the syntax for the commands, where:

*   Command and subcommand names are in uppercase, for example `TS.ADD`
*   Optional arguments are enclosed in square brackets, for example `[index]`
*   Additional optional arguments are indicated by three period characters, for example `...`

Commands usually require a key's name as their first argument. The [path](/redistimeseries/path) is generally assumed to be the root if not specified.
