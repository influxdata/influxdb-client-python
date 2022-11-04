"""
This example is related to `InfluxDB Cloud <https://docs.influxdata.com/influxdb/cloud/>`_ and not available
on a local InfluxDB OSS instance.

How to manage explicit bucket schemas to enforce column names, tags, fields, and data types for your data.
"""
import datetime

from influxdb_client import InfluxDBClient, BucketSchemasService, PostBucketRequest, SchemaType, \
    MeasurementSchemaCreateRequest, MeasurementSchemaColumn, ColumnSemanticType, ColumnDataType, \
    MeasurementSchemaUpdateRequest

"""
Define credentials
"""
influx_cloud_url = 'https://us-west-2-1.aws.cloud2.influxdata.com'
influx_cloud_token = '...'
org_name = '...'

with InfluxDBClient(url=influx_cloud_url, token=influx_cloud_token, org=org_name, debug=False) as client:
    uniqueId = str(datetime.datetime.now())
    org_id = client.organizations_api().find_organizations(org=org_name)[0].id
    bucket_schemas_api = BucketSchemasService(api_client=client.api_client)

    """
    Create a bucket with the schema_type flag set to explicit
    """
    print("------- Create Bucket -------\n")
    created_bucket = client \
        .buckets_api() \
        .create_bucket(bucket=PostBucketRequest(name=f"my_schema_bucket_{uniqueId}",
                                                org_id=org_id,
                                                retention_rules=[],
                                                schema_type=SchemaType.EXPLICIT))
    print(created_bucket)

    """
    Sets the schema for a measurement: Usage CPU

    [
        {"name": "time", "type": "timestamp"},
        {"name": "service", "type": "tag"},
        {"name": "host", "type": "tag"},
        {"name": "usage_user", "type": "field", "dataType": "float"},
        {"name": "usage_system", "type": "field", "dataType": "float"}
    ]
    """
    print("------- Create Schema -------\n")
    columns = [
        MeasurementSchemaColumn(name="time",
                                type=ColumnSemanticType.TIMESTAMP),
        MeasurementSchemaColumn(name="service",
                                type=ColumnSemanticType.TAG),
        MeasurementSchemaColumn(name="host",
                                type=ColumnSemanticType.TAG),
        MeasurementSchemaColumn(name="usage_user",
                                type=ColumnSemanticType.FIELD,
                                data_type=ColumnDataType.FLOAT),
        MeasurementSchemaColumn(name="usage_system",
                                type=ColumnSemanticType.FIELD,
                                data_type=ColumnDataType.FLOAT)
    ]
    create_request = MeasurementSchemaCreateRequest(name="usage_cpu", columns=columns)
    created_schema = bucket_schemas_api.create_measurement_schema(bucket_id=created_bucket.id,
                                                                  org_id=org_id,
                                                                  measurement_schema_create_request=create_request)
    print(created_bucket)

    """
    Lists the Schemas
    """
    print("\n------- Lists the Schemas -------\n")
    measurement_schemas = bucket_schemas_api.get_measurement_schemas(bucket_id=created_bucket.id).measurement_schemas
    print("\n".join([f"---\n ID: {ms.id}\n Name: {ms.name}\n Columns: {ms.columns}" for ms in measurement_schemas]))
    print("---")

    """
    Update a bucket schema
    """
    print("------- Update a bucket schema -------\n")
    columns.append(MeasurementSchemaColumn(name="usage_total",
                                           type=ColumnSemanticType.FIELD,
                                           data_type=ColumnDataType.FLOAT))
    update_request = MeasurementSchemaUpdateRequest(columns=columns)
    updated_schema = bucket_schemas_api.update_measurement_schema(bucket_id=created_bucket.id,
                                                                  measurement_id=created_schema.id,
                                                                  measurement_schema_update_request=update_request)
    print(updated_schema)

    """
    Delete previously created bucket
    """
    print("------- Delete Bucket -------\n")
    client.buckets_api().delete_bucket(created_bucket)
    print(f" successfully deleted bucket: {created_bucket.name}")
