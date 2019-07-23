from influxdb2 import OrganizationsApi


class OrganizationsClient(object):

    def __init__(self, influxdb_client):
        self._influxdb_client = influxdb_client
        self._organizations_api = OrganizationsApi(influxdb_client.api_client)

    def me(self):
        user = self._users_api.get_me()
        return user
