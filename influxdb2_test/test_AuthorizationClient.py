from influxdb2 import PermissionResource, Permission, Authorization
from influxdb2_test.base_test import BaseTest


class AuthorizationsClientTest(BaseTest):

    def setUp(self) -> None:
        super(AuthorizationsClientTest, self).setUp()
        self.user = self.users_client.me()
        self.organization = self.find_my_org()

    def test_createAuthorization(self):
        user_resource = PermissionResource(org_id=self.organization.id, type="users")
        read_users = Permission(action="read", resource=user_resource)

        org_resource = PermissionResource(org_id=self.organization.id, type="orgs")
        write_organizations = Permission(action="write", resource=org_resource)

        permissions = [read_users, write_organizations]
        authorization = self.authorizations_client.create_authorization(self.organization.id, permissions)

        self.log(authorization)

        self.assertIsNotNone(authorization)

        self.assertTrue(len(authorization.token) > 0)

        self.assertEqual(authorization.user_id, self.user.id)
        self.assertEqual(authorization.user, self.user.name)

        self.assertEqual(authorization.org_id, self.organization.id)

        self.assertEqual(authorization.org, self.organization.name)
        # todo
        # self.assertEqual(authorization.status, "active")

        self.assertTrue(len(authorization.permissions) == 2)

        self.assertEqual(authorization.permissions[0].resource.type, "users")

        self.assertEqual(authorization.permissions[0].resource.org_id, self.organization.id)
        self.assertEqual(authorization.permissions[0].action, "read")

        self.assertEqual(authorization.permissions[1].resource.type, "orgs")
        self.assertEqual(authorization.permissions[1].resource.org_id, self.organization.id)
        self.assertEqual(authorization.permissions[1].action, "write")

        self.assertTrue(authorization.links is not None)

        self.assertEqual(authorization.links["self"], "/api/v2/authorizations/" + authorization.id)

        self.assertEqual(authorization.links["user"], "/api/v2/users/" + self.user.id)

    def test_authorizationDescription(self):
        organization = self.my_organization

        resource = PermissionResource(org_id=organization.id, type="sources")
        createSource = Permission(action="write", resource=resource)

        permissions = [createSource]
        authorization = Authorization(org_id=organization.id, permissions=permissions)

        # todo
        # authorization.setStatus(Authorization.StatusEnum.ACTIVE);
        # authorization.setDescription("My description!");

        created = self.authorizations_client.create_authorization(authorization=authorization)

        self.assertIsNotNone(created)
        # todo
        # self.assertEquals(created.description, "My description")
