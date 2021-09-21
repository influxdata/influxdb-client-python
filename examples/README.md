# Examples

## Writes
- [import_data_set.py](import_data_set.py) - How to import CSV file
- [import_data_set_multiprocessing.py](import_data_set_multiprocessing.py) - How to large CSV file by Python Multiprocessing
- [ingest_dataframe_default_tags.py](ingest_dataframe_default_tags.py) - How to ingest DataFrame with default tags
- [ingest_large_dataframe.py](ingest_large_dataframe.py) - How to ingest large DataFrame
- [iot_sensor.py](iot_sensor.py) - How to write sensor data every minute by [RxPY](https://rxpy.readthedocs.io/en/latest/)
- [import_data_set_sync_batching.py](import_data_set_sync_batching.py) - How to use [RxPY](https://rxpy.readthedocs.io/en/latest/) to prepare batches for synchronous write into InfluxDB
- [write_structured_data.py](write_structured_data.py) - How to write structured data - [NamedTuple](https://docs.python.org/3/library/collections.html#collections.namedtuple)

## Queries
- [query.py](query.py) - How to query data into `FluxTable`s, `Stream` and `CSV`
- [query_from_file.py](query_from_file.py) - How to use a Flux query defined in a separate file
- [query_response_to_json.py](query_response_to_json.py) - How to serialize Query response to JSON


## Management API
- [buckets_management.py](buckets_management.py) - How to create, list and delete Buckets
- [monitoring_and_alerting.py](monitoring_and_alerting.py) - How to create the Check with Slack notification.

## Others
- [influx_cloud.py](influx_cloud.py) - How to connect to InfluxDB 2 Cloud
- [influxdb_18_example.py](influxdb_18_example.py) - How to connect to InfluxDB 1.8
- [nanosecond_precision.py](nanosecond_precision.py) - How to use nanoseconds precision
  