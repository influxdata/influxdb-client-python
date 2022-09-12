import pytest

from influxdb_client import UserResponse
from influxdb_client.rest import ApiException
from tests.base_test import BaseTest, generate_name


class UsersApiTests(BaseTest):

    def setUp(self) -> None:
        super(UsersApiTests, self).setUp()
        users_api = self.client.users_api()
        users = users_api.find_users()

        for user in users.users:
            if user.name.endswith("_IT"):
                print("Delete user: ", user.name)
                users_api.delete_user(user=user)

    def test_delete_user(self):
        users_api = self.client.users_api()

        user = users_api.create_user(name=generate_name(key='user'))
        users = users_api.find_users(id=user.id)
        self.assertEqual(1, len(users.users))
        self.assertEqual(user, users.users[0])

        users_api.delete_user(user)

        with pytest.raises(ApiException) as e:
            assert users_api.find_users(id=user.id)
        assert "user not found" in e.value.body

    def test_update_user(self):
        users_api = self.client.users_api()

        name = generate_name(key='user')
        user = users_api.create_user(name=name)
        self.assertEqual(name, user.name)

        user.name = "updated_" + name
        user = users_api.update_user(user=user)
        self.assertIsInstance(user, UserResponse)
        user = users_api.find_users(id=user.id).users[0]
        self.assertEqual("updated_" + name, user.name)

    def test_update_password(self):
        users_api = self.client.users_api()

        user = users_api.create_user(name=generate_name(key='user'))
        users_api.update_password(user, "my-password-2")
        users_api.update_password(user, "my-password-3")

