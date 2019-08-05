# influxdb-client-python

[![Build Status](https://travis-ci.org/bonitoo-io/influxdb-client-python.svg?branch=master)](https://travis-ci.org/bonitoo-io/influxdb-client-python)


InfluxDB 2.0 python client library. TODO...

## Requirements.

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

bucket = "test_bucket"

client = InfluxDBClient(url="http://localhost:9999/api/v2", token="my-token-123", org="my-org")

write_api = client.write_api()
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
