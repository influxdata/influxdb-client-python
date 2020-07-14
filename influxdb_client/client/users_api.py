"""
Users are those with access to InfluxDB.

To grant a user permission to access data, add them as a member of an organization
and provide them with an authentication token.
"""

from influxdb_client import UsersService, User


class UsersApi(object):
    """Implementation for '/api/v2/users' endpoint."""

    def __init__(self, influxdb_client):
        """Initialize defaults."""
        self._influxdb_client = influxdb_client
        self._service = UsersService(influxdb_client.api_client)

    def me(self) -> User:
        """Return the current authenticated user."""
        user = self._service.get_me()
        return user

    def create_user(self, name: str) -> User:
        """Create a user."""
        user = User(name=name)

        return self._service.post_users(user=user)
