from influxdb_client import OrganizationsService, UsersService, Organization


class OrganizationsApi(object):
    """
    The client of the InfluxDB 2.0 that implements Organizations HTTP API endpoint.
    """

    def __init__(self, influxdb_client):
        self._influxdb_client = influxdb_client
        self._organizations_service = OrganizationsService(influxdb_client.api_client)
        self._users_service = UsersService(influxdb_client.api_client)

    def me(self):
        user = self._users_service.get_me()
        return user

    def find_organization(self, org_id):
        return self._organizations_service.get_orgs_id(org_id=org_id)

    def find_organizations(self):
        return self._organizations_service.get_orgs().orgs

    def create_organization(self, name: str = None, organization: Organization = None) -> Organization:
        if organization is None:
            organization = Organization(name=name)
        return self._organizations_service.post_orgs(organization=organization)

    def delete_organization(self, org_id: str):
        return self._organizations_service.delete_orgs_id(org_id=org_id)
