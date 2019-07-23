from influxdb2 import UsersApi


class UsersClient(object):

    def __init__(self, influxdb_client):
        self._influxdb_client = influxdb_client
        self._users_api = UsersApi(influxdb_client.api_client)

    def me(self):
        user = self._users_api.get_me()
        return user
