from influxdb_client import InfluxDBClient, BucketRetentionRules

org = 'my-org'

with InfluxDBClient(url='http://localhost:8086', token='my-token', org=org) as client:
    buckets_api = client.buckets_api()

    # Create Bucket with retention policy set to 3600 seconds and name "bucket-by-python"
    retention_rules = BucketRetentionRules(type="expire", every_seconds=3600)
    created_bucket = buckets_api.create_bucket(bucket_name="bucket-by-python",
                                               retention_rules=retention_rules,
                                               org=org)
    print(created_bucket)
