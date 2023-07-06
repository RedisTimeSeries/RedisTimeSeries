---
title: "Use cases"
linkTitle: "Use cases"
weight: 5
description: >
    Time series use cases
aliases:
  - /docs/stack/search/reference/query_syntax/    
  - /docs/stack/use-cases/  
---

**Monitoring (data center)**

Modern data centers have a lot of moving pieces, such as infrastructure (servers and networks) and software systems (applications and services) that need to be monitored around the clock.

Redis Time Series allows you to plan for new resources upfront, optimize the utilization of existing resources, reconstruct the circumstances that led to outages, and identify application performance issues by analyzing and reporting on the following metrics:

- Maximum CPU utilization per server
- Maximum network latency between two services
- Average IO bandwidth utilization of a storage system
- 99th percentile of the response time of a specific application outages

**Weather analysis (environment)**

Redis Time Series can be used to track environmental measurements such as the number of daily sunshine hours and hourly rainfall depth, over a period of many years. Seasonally, you can measure average rainfall depth, average daily temperature, and the maximum number of sunny hours per day, for example. Watch the increase of the maximum daily temperature over the years. Predict the expected temperature and rainfall depth in a specific location for a particular week of the year.

Multiple time series can be collected, each for a different location. By utilizing secondary indexes, measurements can be aggregated over given geographical regions (e.g., minimal and maximal daily temperature in Europe) or over locations with specific attributes (e.g., average rainfall depth in mountainous regions).

Example metrics include: 

- Rain (cm)
- Temperature (C)
- Sunny periods (h)

**Analysis of the atmosphere (environment)**

The atmospheric concentration of CO2 is more important than ever before. Use TimeSeries to track average, maximum and minimum CO2 level per season and average yearly CO2 over the last decades. Example metrics include:

- Concentration of CO2 (ppm)
- Location

**Flight data recording (sensor data and IoT)**

Planes have a multitude of sensors. This sensor data is stored in a black box and also shared with external systems. TimeSeries can help you reconstruct the sequence of events over time, optimize operations and maintenance intervals, improve safety, and provide feedback to the equipment manufacturers about the part quality. Example metrics include:

- Altitude
- Flight path
- Engine temperature
- Level of vibrations
- Pressure

**Ship logbooks (sensor data and IoT)**

It's very common to keep track of ship voyages via (digital) logbooks. Use TimeSeries to calculate optimal routes using these metrics:

- Wind (km/h)
- Ocean conditions (classes)
- Speed (knots)
- Location (long, lat)

**Connected car (sensor data and IoT)**

Modern cars are exposing several metrics via a standard interface. Use TimeSeries to correlate average fuel consumption with the tire pressure, figure out how long to keep a car in the fleet, determine optimal maintenance intervals, and calculate tax savings by type of the road (taxable vs. nontaxable roads). Example metrics include:

- Acceleration
- Location (long, lat)
- Fuel level (liter)
- Distances (km)
- Speed (km/h)
- Tire pressure
- Distance until next maintenance check

**Smart metering (sensor data and IoT)**

Modern houses and facilities gather details about energy consumption/production. Use Redis Time Series to aggregate billing based on monthly consumption. Optimize the network by redirecting the energy delivery relative to the fluctuations in need. Provide recommendations on how to improve the energy consumption behavior. Example metrics include:

- Consumption per location
- Produced amount of electrical energy per location

**Quality of service (telecom)**

Mobile phone usage is increasing, producing a natural growth that just correlates to the increasing number of cellphones. However, there might also be spikes that correlate with specific events (for example, more messages around world championships). 

Telecom providers need to ensure that they are providing the necessary infrastructure to deliver the right quality of service. This includes using mini towers for short-term peaks. Use TimeSeries to correlate traffic peaks to specific events, load balance traffic over several towers or mini towers, and predictively plan the infrastructure. Metrics include the amount of traffic per tower.

**Stock trading (finance)**

Stock trading is highly automated today. Algorithms, and not just human beings, are trading, from the amount of bids and asks for the trading of a stock to the extreme volumes of trades per second (millions of ops per second). Computer-driven trading requires millisecond response times. It's necessary to keep a lot of data points within a very short period of time (for example, price fluctuations per second within a minute). In addition, the long-term history needs to be kept to make statements about trends or for regulatory purposes.  

Use Redis Time Series to identify correlations between the trading behavior and other events (for example, social network posts). Discover a developing market. Detect anomalies to discover insider trades. Example metrics include:

- Exact time and order of a trade by itself
- Type of the event (trade/bid)
- The stock price