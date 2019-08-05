from influxdb2 import OrganizationsService, UsersService, Organization


class OrganizationsApi(object):

    def __init__(self, influxdb_client):
        self._influxdb_client = influxdb_client
        self._organizations_service = OrganizationsService(influxdb_client.api_client)
        self._users_service = UsersService(influxdb_client.api_client)

    def me(self):
        user = self._users_service.get_me()
        return user

    def find_organization(self, id):
        return self._organizations_service.get_orgs_id(org_id=id)

    def find_organizations(self):
        return self._organizations_service.get_orgs()

    def create_organization(self, name=None, organization=None) -> Organization:
        if organization is None:
            organization = Organization(name=name)
        return self._organizations_service.post_orgs(organization)
