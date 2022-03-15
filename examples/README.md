# Examples

## Writes
- [import_data_set.py](import_data_set.py) - How to import CSV file
- [import_data_set_multiprocessing.py](import_data_set_multiprocessing.py) - How to large CSV file by Python Multiprocessing
- [ingest_dataframe_default_tags.py](ingest_dataframe_default_tags.py) - How to ingest DataFrame with default tags
- [ingest_large_dataframe.py](ingest_large_dataframe.py) - How to ingest large DataFrame
- [iot_sensor.py](iot_sensor.py) - How to write sensor data every minute by [RxPY](https://rxpy.readthedocs.io/en/latest/)
- [import_data_set_sync_batching.py](import_data_set_sync_batching.py) - How to use [RxPY](https://rxpy.readthedocs.io/en/latest/) to prepare batches for synchronous write into InfluxDB
- [write_api_callbacks.py](write_api_callbacks.py) - How to handle batch events
- [write_structured_data.py](write_structured_data.py) - How to write structured data - [NamedTuple](https://docs.python.org/3/library/collections.html#collections.namedtuple), [Data Classes](https://docs.python.org/3/library/dataclasses.html) - (_requires Python v3.8+_)
- [logging_handler.py](logging_handler.py) - How to set up a python native logging handler that writes to InfluxDB

## Queries
- [query.py](query.py) - How to query data into `FluxTable`s, `Stream` and `CSV`
- [query_from_file.py](query_from_file.py) - How to use a Flux query defined in a separate file
- [query_response_to_json.py](query_response_to_json.py) - How to serialize Query response to JSON
- [query_with_profilers.py](query_with_profilers.py) - How to process profilers output by callback


## Management API
- [buckets_management.py](buckets_management.py) - How to create, list and delete Buckets
- [monitoring_and_alerting.py](monitoring_and_alerting.py) - How to create the Check with Slack notification.
- [task_example.py](task_example.py) - How to create a Task by API
- [templates_management.py](templates_management.py) - How to use Templates and Stack API

## Others
- [influx_cloud.py](influx_cloud.py) - How to connect to InfluxDB 2 Cloud
- [influxdb_18_example.py](influxdb_18_example.py) - How to connect to InfluxDB 1.8
- [nanosecond_precision.py](nanosecond_precision.py) - How to use nanoseconds precision
- [invocable_scripts.py](invocable_scripts.py) - How to use Invocable scripts Cloud API to create custom endpoints that query data

## Asynchronous
- [asynchronous.py](asynchronous.py) - How to use Asyncio with InfluxDB client
- [asynchronous_management.py](asynchronous_management.py) - How to use asynchronous Management API
- [asynchronous_batching.py](asynchronous_batching.py) - HHow to use [RxPY](https://rxpy.readthedocs.io/en/latest/) to prepare batches for Asyncio client
  
