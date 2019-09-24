from influxdb_client import UsersService, User


class UsersApi(object):

    def __init__(self, influxdb_client):
        self._influxdb_client = influxdb_client
        self._service = UsersService(influxdb_client.api_client)

    def me(self) -> User:
        user = self._service.get_me()
        return user

    def create_user(self, name: str) -> User:
        user = User(name=name)

        return self._service.post_users(user=user)
