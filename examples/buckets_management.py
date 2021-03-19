"""
How to How to create, list and delete Buckets.
"""

from influxdb_client import InfluxDBClient, BucketRetentionRules

"""
Define credentials
"""
url = "http://localhost:8086"
token = "my-token"

client = InfluxDBClient(url=url, token=token)
buckets_api = client.buckets_api()

"""
The Bucket API uses as a parameter the Organization ID. We have to retrieve ID by Organization API.
"""
org_name = "my-org"
org = list(filter(lambda it: it.name == org_name, client.organizations_api().find_organizations()))[0]

"""
Create Bucket with retention policy set to 3600 seconds and name "bucket-by-python"
"""
print(f"------- Create -------\n")
retention_rules = BucketRetentionRules(type="expire", every_seconds=3600)
created_bucket = buckets_api.create_bucket(bucket_name="bucket-by-python",
                                           retention_rules=retention_rules,
                                           org_id=org.id)
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

client.close()
