## 1.8.0 [unreleased]

### Features
1. [#92](https://github.com/influxdata/influxdb-client-python/issues/92): Optimize serializing Pandas DataFrame for writing

## 1.7.0 [2020-05-15]

### Features
1. [#79](https://github.com/influxdata/influxdb-client-python/issues/79): Added support for writing Pandas DataFrame

### Bug Fixes
1. [#85](https://github.com/influxdata/influxdb-client-python/issues/85): Fixed a possibility to generate empty write batch
2. [#86](https://github.com/influxdata/influxdb-client-python/issues/86): BREAKING CHANGE: Fixed parameters in delete api - now delete api accepts also bucket name and org name instead of only ids
1. [#93](https://github.com/influxdata/influxdb-client-python/pull/93): Remove trailing slash from connection URL

## 1.6.0 [2020-04-17]

### Documentation
1. [#75](https://github.com/influxdata/influxdb-client-python/issues/75): Updated docs to clarify how to use an org parameter
1. [#84](https://github.com/influxdata/influxdb-client-python/pull/84): Clarify how to use a client with InfluxDB 1.8

### Bug Fixes
1. [#72](https://github.com/influxdata/influxdb-client-python/issues/72): Optimize serializing data into Pandas DataFrame

## 1.5.0 [2020-03-13]

### Features
1. [#59](https://github.com/influxdata/influxdb-client-python/issues/59): Set User-Agent to influxdb-client-python/VERSION for all requests

### Bug Fixes
1. [#61](https://github.com/influxdata/influxdb-client-python/issues/61): Correctly parse CSV where multiple results include multiple tables
1. [#66](https://github.com/influxdata/influxdb-client-python/issues/66): Correctly close connection pool manager at exit
1. [#69](https://github.com/influxdata/influxdb-client-python/issues/69): `InfluxDBClient` and `WriteApi` could serialized by [pickle](https://docs.python.org/3/library/pickle.html#object.__getstate__) (python3.7 or higher)

## 1.4.0 [2020-02-14]

### Features
1. [#52](https://github.com/influxdata/influxdb-client-python/issues/52): Initialize client library from config file and environmental properties

### CI
1. [#54](https://github.com/influxdata/influxdb-client-python/pull/54): Add Python 3.7 and 3.8 to CI builds

### Bug Fixes
1. [#56](https://github.com/influxdata/influxdb-client-python/pull/56): Fix default tags for write batching, added new test
1. [#58](https://github.com/influxdata/influxdb-client-python/pull/58): Source distribution also contains: requirements.txt, extra-requirements.txt and test-requirements.txt

## 1.3.0 [2020-01-17]

### Features
1. [#50](https://github.com/influxdata/influxdb-client-python/issues/50): Implemented default tags

### API
1. [#47](https://github.com/influxdata/influxdb-client-python/pull/47): Updated swagger to latest version

### CI
1. [#49](https://github.com/influxdata/influxdb-client-python/pull/49): Added beta release to continuous integration

### Bug Fixes
1. [#48](https://github.com/influxdata/influxdb-client-python/pull/48): InfluxDBClient default org is used by WriteAPI

## 1.2.0 [2019-12-06]

### Features
1. [#44](https://github.com/influxdata/influxdb-client-python/pull/44): Optimized serialization into LineProtocol, Clarified how to use client for import large amount of data

### API
1. [#42](https://github.com/influxdata/influxdb-client-python/pull/42): Updated swagger to latest version

### Bug Fixes
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

### Bug Fixes
1. [#19](https://github.com/bonitoo-io/influxdb-client-python/pull/19): Removed strict checking of enum values 

### Documentation
1. [#22](https://github.com/bonitoo-io/influxdb-client-python/issues/22): Documented how to connect to InfluxCloud

## 0.0.2 [2019-09-26]

### Features
1. [#2](https://github.com/bonitoo-io/influxdb-client-python/issues/2): The write client is able to write data in batches (configuration: `batch_size`, `flush_interval`, `jitter_interval`, `retry_interval`)
1. [#5](https://github.com/bonitoo-io/influxdb-client-python/issues/5): Added support for gzip compression of query response and write body 
 
### API
1. [#10](https://github.com/bonitoo-io/influxdb-client-python/pull/10): Updated swagger to latest version

### Bug Fixes
1. [#3](https://github.com/bonitoo-io/influxdb-client-python/issues/3): The management API correctly supports inheritance defined in Influx API
1. [#7](https://github.com/bonitoo-io/influxdb-client-python/issues/7): Drop NaN and infinity values from fields when writing to InfluxDB

### CI
1. [#11](https://github.com/bonitoo-io/influxdb-client-python/pull/11): Switch CI to CircleCI 
1. [#12](https://github.com/bonitoo-io/influxdb-client-python/pull/12): CI generate code coverage report on CircleCI 
