import asyncio

from influxdb_client import OrganizationsService
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync


async def main():
    async with InfluxDBClientAsync(url='http://localhost:8086', token='my-token', org='my-org') as client:
        # Initialize async OrganizationsService
        organizations_service = OrganizationsService(api_client=client.api_client)

        # Find organization with name 'my-org'
        organizations = await organizations_service.get_orgs_async(org='my-org')
        for organization in organizations.orgs:
            print(f'name: {organization.name}, id: {organization.id}')


if __name__ == "__main__":
    asyncio.run(main())
