"""
How to create, list and delete Buckets.
"""

from influxdb_client import InfluxDBClient, BucketRetentionRules

"""
Define credentials
"""
url = "http://localhost:8086"
token = "my-token"
org = "my-org"

with InfluxDBClient(url=url, token=token) as client:
    buckets_api = client.buckets_api()

    """
    Create Bucket with retention policy set to 3600 seconds and name "bucket-by-python"
    """
    print(f"------- Create -------\n")
    retention_rules = BucketRetentionRules(type="expire", every_seconds=3600)
    created_bucket = buckets_api.create_bucket(bucket_name="bucket-by-python",
                                               retention_rules=retention_rules,
                                               org=org)
    print(created_bucket)

    """
    Update Bucket
    """
    print(f"------- Update -------\n")
    created_bucket.description = "Update description"
    created_bucket = buckets_api.update_bucket(bucket=created_bucket)
    print(created_bucket)

    """
    List all Buckets
    """
    print(f"\n------- List -------\n")
    buckets = buckets_api.find_buckets().buckets
    print("\n".join([f" ---\n ID: {bucket.id}\n Name: {bucket.name}\n Retention: {bucket.retention_rules}"
                     for bucket in buckets]))
    print("---")

    """
    Delete previously created bucket
    """
    print(f"------- Delete -------\n")
    buckets_api.delete_bucket(created_bucket)
    print(f" successfully deleted bucket: {created_bucket.name}")
