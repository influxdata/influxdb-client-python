# Migration Guide

This guide is meant to help you migrate your Python code from [influxdb-python](https://github.com/influxdata/influxdb-python) to `influxdb-client-python` by providing code examples that cover common usages. 

If there is something missing, please feel free to create a [new request](https://github.com/influxdata/influxdb-client-python/issues/new?assignees=&labels=documentation&title=docs(migration%20guide):%20&template=feature_request.md) for a guide enhancement.

## Before You Start

Please take a moment to review the following client docs:

- [User Guide](https://influxdb-client.readthedocs.io/en/stable/usage.html), [README.rst](README.rst)
- [Examples](examples/README.md#examples)
- [API Reference](https://influxdb-client.readthedocs.io/en/stable/api.html)
- [CHANGELOG.md](CHANGELOG.md)

## Content

- [Initializing Client](#initializing-client)
- [Creating Database/Bucket](#creating-databasebucket)
- [Dropping Database/Bucket](#dropping-databasebucket)

## Initializing Client

**influxdb-python**

```python
from influxdb import InfluxDBClient

client = InfluxDBClient(host='127.0.0.1', port=8086, username='root', password='root', database='dbname')
```

**influxdb-client-python**

```python
from influxdb_client import InfluxDBClient

with InfluxDBClient(url='http://localhost:8086', token='my-token', org='my-org') as client:
    pass
```

## Creating Database/Bucket

**influxdb-python**

```python
from influxdb import InfluxDBClient

client = InfluxDBClient(host='127.0.0.1', port=8086, username='root', password='root', database='dbname')

dbname = 'example'
client.create_database(dbname)
client.create_retention_policy('awesome_policy', '60m', 3, database=dbname, default=True)
```

**influxdb-client-python**

```python
from influxdb_client import InfluxDBClient, BucketRetentionRules

org = 'my-org'

with InfluxDBClient(url='http://localhost:8086', token='my-token', org=org) as client:
    buckets_api = client.buckets_api()

    # Create Bucket with retention policy set to 3600 seconds and name "bucket-by-python"
    retention_rules = BucketRetentionRules(type="expire", every_seconds=3600)
    created_bucket = buckets_api.create_bucket(bucket_name="bucket-by-python",
                                               retention_rules=retention_rules,
                                               org=org)
```

## Dropping Database/Bucket

**influxdb-python**

```python
from influxdb import InfluxDBClient

client = InfluxDBClient(host='127.0.0.1', port=8086, username='root', password='root', database='dbname')

dbname = 'example'
client.drop_database(dbname)
```

**influxdb-client-python**

```python
from influxdb_client import InfluxDBClient

with InfluxDBClient(url='http://localhost:8086', token='my-token', org='my-org') as client:
    buckets_api = client.buckets_api()

    bucket = buckets_api.find_bucket_by_name("my-bucket")
    buckets_api.delete_bucket(bucket)
```

## Writing LineProtocol

**influxdb-python**

```python
```

**influxdb-client-python**

```python
```

## Writing Point Object

**influxdb-python**

```python
```

**influxdb-client-python**

```python
```

## Writing Structured Data

**influxdb-python**

```python
```

**influxdb-client-python**

```python
```

## Querying

**influxdb-python**

```python
```

**influxdb-client-python**

```python
```
