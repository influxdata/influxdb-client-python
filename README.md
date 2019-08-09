# influxdb-client-python

[![Build Status](https://travis-ci.org/bonitoo-io/influxdb-client-python.svg?branch=master)](https://travis-ci.org/bonitoo-io/influxdb-client-python)

InfluxDB 2.0 python client library. TODO...

- [Requirements](#requirements)
- [Getting Started](#getting-started)
- [Features](#how-to-use)
    - [Writing data](#writes)
        - [How to efficiently import large dataset](#how-to-efficiently-import-large-dataset)

## Requirements

Python 2.7 and 3.4+

## Installation & Usage
### pip install

If the python package is hosted on Github, you can install directly from Github

```sh
pip install git+https://github.com/bonitoo-io/influxdb-client-python.git
```
(you may need to run `pip` with root permission: `sudo pip install git+https://github.com/bonitoo-io/influxdb-client-python.git`)

Then import the package:
```python
import influxdb2 
```

### Setuptools

Install via [Setuptools](http://pypi.python.org/pypi/setuptools).

```sh
python setup.py install --user
```
(or `sudo python setup.py install` to install the package for all users)

Then import the package:
```python
import influxdb2
```

## Getting Started

Please follow the [installation procedure](#installation--usage) and then run the following:

```python
from influxdb2.client.influxdb_client import InfluxDBClient
from influxdb2.client.write.point import Point
from influxdb2.client.write_api import SYNCHRONOUS

bucket = "test_bucket"

client = InfluxDBClient(url="http://localhost:9999/api/v2", token="my-token-123", org="my-org")

write_api = client.write_api(write_options=SYNCHRONOUS)
query_api = client.query_api()

p = Point("my_measurement").tag("location", "Prague").field("temperature", 25.3)

write_api.write(bucket=bucket, org="my-org", record=p)

## using Table structure
tables = query_api.query('from(bucket:"my-bucket") |> range(start: -10m)')

for table in tables:
    print(table)
    for row in table.records:
        print (row.values)


## using csv library
csv_result = query_api.query_csv('from(bucket:"my-bucket") |> range(start: -10m)')
val_count = 0
for row in csv_result:
    for cell in row:
        val_count += 1
```

## How to use

### Writes

The [WriteApiClient](https://github.com/bonitoo-io/influxdb-client-python/blob/master/influxdb2/client/write_api.py) supports synchronous, asynchronous and batching writes into InfluxDB 2.0 and could be configured by `WriteOptions`:

| Property | Description | Default Value |
| --- | --- | --- |
| [**write_type**](#write_type) | how the client writes data; allowed values: `batching`, `asynchronous`, `synchronous`| `batching` |
| **batch_size** | the number of data point to collect in batch | `1000` |
| **flush_interval** | the number of milliseconds before the batch is written | `1000` |
| **jitter_interval** | the number of milliseconds to increase the batch flush interval by a random amount | `0` |
| **retry_interval** | the number of milliseconds to retry unsuccessful write. The retry interval is used when the InfluxDB server does not specify "Retry-After" header. | `1000` |

##### write_type
* `batching` - data are writes in batches defined by `batch_size`, `flush_interval`, ...
* `asynchronous` - data are writes in asynchronous HTTP request
* `synchronous` - data are writes in synchronous HTTP request

#### How to efficiently import large dataset

```python
"""
Import VIX - CBOE Volatility Index - from "vix-daily.csv" file into InfluxDB 2.0

https://datahub.io/core/finance-vix#data
"""

from collections import OrderedDict
from csv import DictReader
from datetime import datetime

import rx
from rx import operators as ops

from influxdb2.client.influxdb_client import InfluxDBClient
from influxdb2.client.write.point import Point
from influxdb2.client.write_api import WriteOptions


def parse_row(row: OrderedDict):
    """Parse row of CSV file into Point with structure:

        financial-analysis,type=vix-daily close=18.47,high=19.82,low=18.28,open=19.82 1198195200000000000

    CSV format:
        Date,VIX Open,VIX High,VIX Low,VIX Close\n
        2004-01-02,17.96,18.68,17.54,18.22\n
        2004-01-05,18.45,18.49,17.44,17.49\n
        2004-01-06,17.66,17.67,16.19,16.73\n
        2004-01-07,16.72,16.75,15.5,15.5\n
        2004-01-08,15.42,15.68,15.32,15.61\n
        2004-01-09,16.15,16.88,15.57,16.75\n
        ...

    :param row: the row of CSV file
    :return: Parsed csv row to [Point]
    """
    return Point("financial-analysis") \
        .tag("type", "vix-daily") \
        .field("open", float(row['VIX Open'])) \
        .field("high", float(row['VIX High'])) \
        .field("low", float(row['VIX Low'])) \
        .field("close", float(row['VIX Close'])) \
        .time(datetime.strptime(row['Date'], '%Y-%m-%d'))


"""
Converts vix-daily.csv into sequence of data point
"""
data = rx.from_iterable(DictReader(open('vix-daily.csv', 'r'))) \
    .pipe(ops.map(lambda row: parse_row(row)))

client = InfluxDBClient(url="http://localhost:9999/api/v2", token="my-token-123", org="my-org", debug=True)

"""
Create client that writes data in batches with 500 items.
"""
write_api = client.write_api(write_options=WriteOptions(batch_size=500, jitter_interval=1_000))

"""
Write data into InfluxDB
"""
write_api.write(org="my-org", bucket="my-bucket", record=data)
write_api.__del__()

"""
Querying max value of CBOE Volatility Index
"""
query = 'from(bucket:"my-bucket")' \
        ' |> range(start: 0, stop: now())' \
        ' |> filter(fn: (r) => r._measurement == "financial-analysis")' \
        ' |> max()'
result = client.query_api().query(org="my-org", query=query)

"""
Processing results
"""
print()
print("=== results ===")
print()
for table in result:
    for record in table.records:
        print('max {0:5} = {1}'.format(record.get_field(), record.get_value()))

"""
Close client
"""
client.__del__()

```
