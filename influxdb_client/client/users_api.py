"""
Users are those with access to InfluxDB.

To grant a user permission to access data, add them as a member of an organization
and provide them with an authentication token.
"""

from typing import Union
from influxdb_client import UsersService, User, Users, UserResponse, PasswordResetBody


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

    def update_user(self, user: User) -> UserResponse:
        """Update a user.

        :param user: User update to apply (required)
        :return: User
        """
        return self._service.patch_users_id(user_id=user.id, user=user)

    def update_password(self, user: Union[str, User, UserResponse], password: str) -> None:
        """Update a password.

        :param user: User to update password (required)
        :param password: New password (required)
        :return: None
        """
        user_id = self._user_id(user)

        return self._service.post_users_id_password(user_id=user_id, password_reset_body=PasswordResetBody(password))

    def delete_user(self, user: Union[str, User, UserResponse]) -> None:
        """Delete a user.

        :param user: user id or User
        :return: None
        """
        user_id = self._user_id(user)

        return self._service.delete_users_id(user_id=user_id)

    def find_users(self, **kwargs) -> Users:
        """List all users.

        :key int offset: The offset for pagination. The number of records to skip.
        :key int limit: Limits the number of records returned. Default is `20`.
        :key str after: The last resource ID from which to seek from (but not including).
                        This is to be used instead of `offset`.
        :key str name: The user name.
        :key str id: The user ID.
        :return: Buckets
        """
        return self._service.get_users(**kwargs)

    def _user_id(self, user: Union[str, User, UserResponse]):
        if isinstance(user, User):
            user_id = user.id
        elif isinstance(user, UserResponse):
            user_id = user.id
        else:
            user_id = user
        return user_id
