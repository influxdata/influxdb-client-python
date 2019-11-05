## 0.0.3 [unreleased]

### Features
1. [#24](https://github.com/influxdata/influxdb-client-python/issues/24): Added possibility to write dictionary-style object
1. [#27](https://github.com/influxdata/influxdb-client-python/issues/27): Added possibility to write bytes type of data
1. [#30](https://github.com/influxdata/influxdb-client-python/issues/30): Added support for streaming a query response
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
