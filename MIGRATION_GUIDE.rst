Migration Guide
===============

This guide is meant to help you migrate your Python code from
`influxdb-python <https://github.com/influxdata/influxdb-python>`__ to
``influxdb-client-python`` by providing code examples that cover common
usages.

If there is something missing, please feel free to create a `new
request <https://github.com/influxdata/influxdb-client-python/issues/new?assignees=&labels=documentation&title=docs(migration%20guide):%20&template=feature_request.md>`__
for a guide enhancement.

Before You Start
----------------

Please take a moment to review the following client docs:

-  `User Guide <https://influxdb-client.readthedocs.io/en/stable/usage.html>`__, `README.rst <https://github.com/influxdata/influxdb-client-python#influxdb-client-python>`__
-  `Examples <https://github.com/influxdata/influxdb-client-python/tree/master/examples#examples>`__
-  `API Reference <https://influxdb-client.readthedocs.io/en/stable/api.html>`__
-  `CHANGELOG.md <https://github.com/influxdata/influxdb-client-python/blob/master/CHANGELOG.md>`__

Content
-------

-  `Initializing Client <#initializing-client>`__
-  `Creating Database/Bucket <#creating-databasebucket>`__
-  `Dropping Database/Bucket <#dropping-databasebucket>`__
-  Writes
    -  `LineProtocol <#writing-lineprotocol>`__
    -  `Dictionary-style object <#writing-dictionary-style-object>`__
    -  `Structured data <#writing-structured-data>`__
    -  `Pandas DataFrame <#writing-pandas-dataframe>`__
-  `Querying <#querying>`__

Initializing Client
-------------------

**influxdb-python**

.. code:: python

    from influxdb import InfluxDBClient

    client = InfluxDBClient(host='127.0.0.1', port=8086, username='root', password='root', database='dbname')

**influxdb-client-python**

.. code:: python

    from influxdb_client import InfluxDBClient

    with InfluxDBClient(url='http://localhost:8086', token='my-token', org='my-org') as client:
        pass

Creating Database/Bucket
------------------------

**influxdb-python**

.. code:: python

    from influxdb import InfluxDBClient

    client = InfluxDBClient(host='127.0.0.1', port=8086, username='root', password='root', database='dbname')

    dbname = 'example'
    client.create_database(dbname)
    client.create_retention_policy('awesome_policy', '60m', 3, database=dbname, default=True)

**influxdb-client-python**

.. code:: python

    from influxdb_client import InfluxDBClient, BucketRetentionRules

    org = 'my-org'

    with InfluxDBClient(url='http://localhost:8086', token='my-token', org=org) as client:
        buckets_api = client.buckets_api()

        # Create Bucket with retention policy set to 3600 seconds and name "bucket-by-python"
        retention_rules = BucketRetentionRules(type="expire", every_seconds=3600)
        created_bucket = buckets_api.create_bucket(bucket_name="bucket-by-python",
                                                   retention_rules=retention_rules,
                                                   org=org)

Dropping Database/Bucket
------------------------

**influxdb-python**

.. code:: python

    from influxdb import InfluxDBClient

    client = InfluxDBClient(host='127.0.0.1', port=8086, username='root', password='root', database='dbname')

    dbname = 'example'
    client.drop_database(dbname)

**influxdb-client-python**

.. code:: python

    from influxdb_client import InfluxDBClient

    with InfluxDBClient(url='http://localhost:8086', token='my-token', org='my-org') as client:
        buckets_api = client.buckets_api()

        bucket = buckets_api.find_bucket_by_name("my-bucket")
        buckets_api.delete_bucket(bucket)

Writing LineProtocol
--------------------

**influxdb-python**

.. code:: python

    from influxdb import InfluxDBClient

    client = InfluxDBClient(host='127.0.0.1', port=8086, username='root', password='root', database='dbname')

    client.write('h2o_feet,location=coyote_creek water_level=1.0 1', protocol='line')

**influxdb-client-python**

.. code:: python

    from influxdb_client import InfluxDBClient
    from influxdb_client.client.write_api import SYNCHRONOUS

    with InfluxDBClient(url='http://localhost:8086', token='my-token', org='my-org') as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)

        write_api.write(bucket='my-bucket', record='h2o_feet,location=coyote_creek water_level=1.0 1')

Writing Dictionary-style object
-------------------------------

**influxdb-python**

.. code:: python

    from influxdb import InfluxDBClient

    record = [
            {
                "measurement": "cpu_load_short",
                "tags": {
                    "host": "server01",
                    "region": "us-west"
                },
                "time": "2009-11-10T23:00:00Z",
                "fields": {
                    "Float_value": 0.64,
                    "Int_value": 3,
                    "String_value": "Text",
                    "Bool_value": True
                }
            }
        ]

    client = InfluxDBClient(host='127.0.0.1', port=8086, username='root', password='root', database='dbname')

    client.write_points(record)

**influxdb-client-python**

.. code:: python

    from influxdb_client import InfluxDBClient
    from influxdb_client.client.write_api import SYNCHRONOUS

    with InfluxDBClient(url='http://localhost:8086', token='my-token', org='my-org') as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)

        record = [
            {
                "measurement": "cpu_load_short",
                "tags": {
                    "host": "server01",
                    "region": "us-west"
                },
                "time": "2009-11-10T23:00:00Z",
                "fields": {
                    "Float_value": 0.64,
                    "Int_value": 3,
                    "String_value": "Text",
                    "Bool_value": True
                }
            }
        ]

        write_api.write(bucket='my-bucket', record=record)

Writing Structured Data
-----------------------

**influxdb-python**

.. code:: python

    from influxdb import InfluxDBClient
    from influxdb import SeriesHelper

    my_client = InfluxDBClient(host='127.0.0.1', port=8086, username='root', password='root', database='dbname')


    class MySeriesHelper(SeriesHelper):
        class Meta:
            client = my_client
            series_name = 'events.stats.{server_name}'
            fields = ['some_stat', 'other_stat']
            tags = ['server_name']
            bulk_size = 5
            autocommit = True


    MySeriesHelper(server_name='us.east-1', some_stat=159, other_stat=10)
    MySeriesHelper(server_name='us.east-1', some_stat=158, other_stat=20)

    MySeriesHelper.commit()


The ``influxdb-client-python`` doesn't have an equivalent implementation for ``MySeriesHelper``, but there is an option
to use Python `Data Classes <https://docs.python.org/3/library/dataclasses.html>`__ way:

**influxdb-client-python**

.. code:: python

    from dataclasses import dataclass

    from influxdb_client import InfluxDBClient
    from influxdb_client.client.write_api import SYNCHRONOUS


    @dataclass
    class Car:
        """
        DataClass structure - Car
        """
        engine: str
        type: str
        speed: float


    with InfluxDBClient(url='http://localhost:8086', token='my-token', org='my-org') as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)

        car = Car('12V-BT', 'sport-cars', 125.25)

        write_api.write(bucket="my-bucket",
                        record=car,
                        record_measurement_name="performance",
                        record_tag_keys=["engine", "type"],
                        record_field_keys=["speed"])

Writing Pandas DataFrame
------------------------

**influxdb-python**

.. code:: python

    import pandas as pd

    from influxdb import InfluxDBClient

    df = pd.DataFrame(data=list(range(30)),
                      index=pd.date_range(start='2014-11-16', periods=30, freq='H'),
                      columns=['0'])

    client = InfluxDBClient(host='127.0.0.1', port=8086, username='root', password='root', database='dbname')

    client.write_points(df, 'demo', protocol='line')

**influxdb-client-python**

.. code:: python

    import pandas as pd

    from influxdb_client import InfluxDBClient
    from influxdb_client.client.write_api import SYNCHRONOUS

    with InfluxDBClient(url='http://localhost:8086', token='my-token', org='my-org') as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)

        df = pd.DataFrame(data=list(range(30)),
                          index=pd.date_range(start='2014-11-16', periods=30, freq='H'),
                          columns=['0'])

        write_api.write(bucket='my-bucket', record=df, data_frame_measurement_name='demo')

Querying
--------

**influxdb-python**

.. code:: python

    from influxdb import InfluxDBClient

    client = InfluxDBClient(host='127.0.0.1', port=8086, username='root', password='root', database='dbname')

    points = client.query('SELECT * from cpu').get_points()
    for point in points:
        print(point)

**influxdb-client-python**

.. code:: python

    from influxdb_client import InfluxDBClient

    with InfluxDBClient(url='http://localhost:8086', token='my-token', org='my-org', debug=True) as client:
        query = '''from(bucket: "my-bucket")
      |> range(start: -10000d)
      |> filter(fn: (r) => r["_measurement"] == "cpu")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

        tables = client.query_api().query(query)
        for record in [record for table in tables for record in table.records]:
            print(record.values)

If you would like to omit boilerplate columns such as ``_result``, ``_table``, ``_start``, ... you can filter the record values by
following expression:

.. code:: python

    print({k: v for k, v in record.values.items() if k not in ['result', 'table', '_start', '_stop', '_measurement']})

For more info see `Flux Response Format <https://github.com/influxdata/flux/blob/master/docs/SPEC.md#response-format>`__.
