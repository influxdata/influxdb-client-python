## 1.5.0 [unreleased]

## 1.4.0 [2020-02-14]

### Features
1. [#52](https://github.com/influxdata/influxdb-client-python/issues/52): Initialize client library from config file and environmental properties

### CI
1. [#54](https://github.com/influxdata/influxdb-client-python/pull/54): Add Python 3.7 and 3.8 to CI builds

### Bugs
1. [#56](https://github.com/influxdata/influxdb-client-python/pull/56): Fix default tags for write batching, added new test
1. [#58](https://github.com/influxdata/influxdb-client-python/pull/58): Source distribution also contains: requirements.txt, extra-requirements.txt and test-requirements.txt

## 1.3.0 [2020-01-17]

### Features
1. [#50](https://github.com/influxdata/influxdb-client-python/issues/50): Implemented default tags

### API
1. [#47](https://github.com/influxdata/influxdb-client-python/pull/47): Updated swagger to latest version

### CI
1. [#49](https://github.com/influxdata/influxdb-client-python/pull/49): Added beta release to continuous integration

### Bugs
1. [#48](https://github.com/influxdata/influxdb-client-python/pull/48): InfluxDBClient default org is used by WriteAPI

## 1.2.0 [2019-12-06]

### Features
1. [#44](https://github.com/influxdata/influxdb-client-python/pull/44): Optimized serialization into LineProtocol, Clarified how to use client for import large amount of data

### API
1. [#42](https://github.com/influxdata/influxdb-client-python/pull/42): Updated swagger to latest version

### Bugs
1. [#45](https://github.com/influxdata/influxdb-client-python/pull/45): Pandas is a optional dependency and has to installed separably

## 1.1.0 [2019-11-19]

### Features
1. [#29](https://github.com/influxdata/influxdb-client-python/issues/29): Added support for serialise response into Pandas DataFrame

## 1.0.0 [2019-11-11]

### Features
1. [#24](https://github.com/influxdata/influxdb-client-python/issues/24): Added possibility to write dictionary-style object
1. [#27](https://github.com/influxdata/influxdb-client-python/issues/27): Added possibility to write bytes type of data
1. [#30](https://github.com/influxdata/influxdb-client-python/issues/30): Added support for streaming a query response
1. [#35](https://github.com/influxdata/influxdb-client-python/pull/35): FluxRecord supports dictionary-style access
1. [#31](https://github.com/influxdata/influxdb-client-python/issues/31): Added support for delete metrics  

### API
1. [#28](https://github.com/bonitoo-io/influxdb-client-python/pull/28): Updated swagger to latest version

### Bugs
1. [#19](https://github.com/bonitoo-io/influxdb-client-python/pull/19): Removed strict checking of enum values 

### Documentation
1. [#22](https://github.com/bonitoo-io/influxdb-client-python/issues/22): Documented how to connect to InfluxCloud

## 0.0.2 [2019-09-26]

### Features
1. [#2](https://github.com/bonitoo-io/influxdb-client-python/issues/2): The write client is able to write data in batches (configuration: `batch_size`, `flush_interval`, `jitter_interval`, `retry_interval`)
1. [#5](https://github.com/bonitoo-io/influxdb-client-python/issues/5): Added support for gzip compression of query response and write body 
 
### API
1. [#10](https://github.com/bonitoo-io/influxdb-client-python/pull/10): Updated swagger to latest version

### Bugs
1. [#3](https://github.com/bonitoo-io/influxdb-client-python/issues/3): The management API correctly supports inheritance defined in Influx API
1. [#7](https://github.com/bonitoo-io/influxdb-client-python/issues/7): Drop NaN and infinity values from fields when writing to InfluxDB

### CI
1. [#11](https://github.com/bonitoo-io/influxdb-client-python/pull/11): Switch CI to CircleCI 
1. [#12](https://github.com/bonitoo-io/influxdb-client-python/pull/12): CI generate code coverage report on CircleCI 
