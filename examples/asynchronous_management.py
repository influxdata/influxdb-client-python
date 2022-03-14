"""
How to use asynchronous management API.
"""
import asyncio
import datetime

from influxdb_client import OrganizationsService, BucketsService, PostBucketRequest, BucketRetentionRules
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

"""
Define credentials
"""
url = 'http://localhost:8086'
token = 'my-token'
bucket_name = 'my-bucket'
org_name = 'my-org'


async def main():
    async with InfluxDBClientAsync(url=url, token=token, org=org_name) as client:
        unique_id = str(datetime.datetime.now())
        """
        Initialize async OrganizationsService
        """
        organizations_service = OrganizationsService(api_client=client.api_client)

        """
        Find organization with name 'my-org'
        """
        print(f"\n------- Find organization with name 'my-org' -------\n")
        organizations = await organizations_service.get_orgs(org=org_name)
        for organization in organizations.orgs:
            print(f' > id: {organization.id}, name: {organization.name}')

        """
        Initialize async BucketsService
        """
        buckets_service = BucketsService(api_client=client.api_client)

        """
        Create new Bucket with retention policy 1h
        """
        print(f"\n------- Create new Bucket with retention policy 1h -------\n")
        retention_rules = [BucketRetentionRules(type="expire", every_seconds=3600)]
        bucket_request = PostBucketRequest(name=f"new_bucket_{unique_id}",
                                           retention_rules=retention_rules,
                                           description="Bucket created by async version of API.",
                                           org_id=organizations.orgs[0].id)
        created_bucket = await buckets_service.post_buckets(post_bucket_request=bucket_request)
        print(" > created bucket: ")
        print()
        print(created_bucket)


if __name__ == "__main__":
    asyncio.run(main())
