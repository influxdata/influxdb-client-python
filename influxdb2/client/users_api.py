from influxdb2 import UsersService


class UsersApi(object):

    def __init__(self, influxdb_client):
        self._influxdb_client = influxdb_client
        self._users_api = UsersService(influxdb_client.api_client)

    def me(self):
        user = self._users_api.get_me()
        return user
