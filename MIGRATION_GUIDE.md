# Migration Guide

This guide is meant to help you migrate your Python code from [influxdb-python](https://github.com/influxdata/influxdb-python) to `influxdb-client-python` by providing code examples that cover common usages. 

If there is something missing, please feel free to create a [new request](https://github.com/influxdata/influxdb-client-python/issues/new?assignees=&labels=documentation&title=docs(migration%20guide):%20&template=feature_request.md) for a guide enhancement.

## Before You Start

Please take a moment to review the following client docs:

- [User Guide](https://influxdb-client.readthedocs.io/en/stable/usage.html), [README.rst](README.rst)
- [Examples](examples/README.md#examples)
- [API Reference](https://influxdb-client.readthedocs.io/en/stable/api.html)
- [CHANGELOG.md](CHANGELOG.md)

## Initializing Client

**influxdb-python**

```python
from influxdb import InfluxDBClient

client = InfluxDBClient(host='127.0.0.1', port=8086, username='root', password='root', database='dbname')
```

**influxdb-client-python**

```python
from influxdb_client import InfluxDBClient

client = InfluxDBClient(url='http://localhost:8086', token='my-token', org='my-org')
```

## Creating Database/Bucket

## Dropping Database/Bucket

## Writing LineProtocol

## Writing Point Object

## Writing Structured Data

## Querying

