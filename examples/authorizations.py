import os

from influxdb_client import InfluxDBClient, BucketRetentionRules, PermissionResource, Permission, Authorization, \
    WriteOptions
from influxdb_client.client.write_api import WriteType
from influxdb_client.rest import ApiException

HOST_URL =  os.environ.get("INFLUX_HOST") if os.environ.get("INFLUX_HOST") is not None else "http://localhost:8086"
TOKEN = os.environ.get("INFLUX_TOKEN") if os.environ.get("INFLUX_TOKEN") is not None else "my-token"
ORG = os.environ.get("INFLUX_ORG") if os.environ.get("INFLUX_ORG") is not None else "my-org"
SYS_BUCKET = os.environ.get("INFLUX_DB") if os.environ.get("INFLUX_DB") is not None else "my-bucket"
BUCKET = "special-bucket"


def create_auths():
    # Create authorizations with an initial client using all-access permissions
    with InfluxDBClient(url=HOST_URL, token=TOKEN, org=ORG, debug=False) as globalClient:
        bucket_rules = BucketRetentionRules(type="expire", every_seconds=3600)
        bucket = globalClient.buckets_api().create_bucket(bucket_name=BUCKET,
                                                      retention_rules=bucket_rules,
                                                      org=ORG)

        bucket_permission_resource_r = PermissionResource(org=ORG,
                                                      org_id=bucket.org_id,
                                                      type="buckets",
                                                      id=bucket.id)
        bucket_permission_resource_w = PermissionResource(org=ORG,
                                                      org_id=bucket.org_id,
                                                      type="buckets",
                                                      id=bucket.id)
        read_bucket = Permission(action="read", resource=bucket_permission_resource_r)
        write_bucket = Permission(action="write", resource=bucket_permission_resource_w)
        permissions = [read_bucket, write_bucket]
        auth_payload = Authorization(org_id=bucket.org_id,
                                 permissions=permissions,
                                 description="Shared bucket auth from Authorization object",
                                 id="auth1_base")
        auth_api = globalClient.authorizations_api()
        # use keyword arguments
        auth1 = auth_api.create_authorization(authorization=auth_payload)
        # or use positional arguments
        auth2 = auth_api.create_authorization(bucket.org_id, permissions)

    return auth1, auth2


def try_sys_bucket(client):
    print("starting to write")

    w_api = client.write_api(write_options=WriteOptions(write_type=WriteType.synchronous))
    try:
        w_api.write(bucket=SYS_BUCKET, record="cpu,host=r2d2 use=3.14")
    except ApiException as ae:
        print(f"Write to {SYS_BUCKET} failed (as expected) due to:")
        print(ae)


def try_restricted_bucket(client):
    print("starting to write")
    w_api = client.write_api(write_options=WriteOptions(write_type=WriteType.synchronous))

    w_api.write(bucket=BUCKET, record="cpu,host=r2d2 usage=3.14")
    print("written")
    print("now query")
    q_api = client.query_api()
    query = f'''
        from(bucket:"{BUCKET}")
            |> range(start: -5m) 
            |> filter(fn: (r) => r["_measurement"] == "cpu")'''

    tables = q_api.query(query=query, org=ORG)
    for table in tables:
        for record in table.records:
            print(record["_time"].isoformat(sep="T") + " | " + record["host"] + " | " + record["_field"] + "=" + str(record["_value"]))


def main():
    """
    a1 is generated using a local Authorization instance
    a2 is generated using local permissions and an internally created Authorization
    :return: void
    """
    print("=== Setting up authorizations ===")
    a1, a2 = create_auths()

    print("=== Using a1 authorization ===")
    client1 = InfluxDBClient(url=HOST_URL, token=a1.token, org=ORG, debug=False)
    print("   --- Try System Bucket ---")
    try_sys_bucket(client1)
    print("   --- Try Special Bucket ---")
    try_restricted_bucket(client1)
    print()

    print("=== Using a2 authorization ===")
    client2 = InfluxDBClient(url=HOST_URL, token=a2.token, org=ORG, debug=False)
    print("   --- Try System Bucket ---")
    try_sys_bucket(client2)
    print("   --- Try Special Bucket ---")
    try_restricted_bucket(client2)


if __name__ == "__main__":
    main()