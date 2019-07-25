from influxdb2 import OrganizationsApi, Organization


class OrganizationsClient(object):

    def __init__(self, influxdb_client):
        self._influxdb_client = influxdb_client
        self._organizations_api = OrganizationsApi(influxdb_client.api_client)

    def me(self):
        user = self._users_api.get_me()
        return user

    def find_organization(self, id):
        return self._organizations_api.get_orgs_id(org_id=id)

    def find_organizations(self):
        return self._organizations_api.get_orgs()

    def create_organization(self, name=None, organization=None) -> Organization:
        if organization is None:
            organization = Organization(name=name)
        return self._organizations_api.post_orgs(organization)
