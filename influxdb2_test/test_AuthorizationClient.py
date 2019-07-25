import pytest

from influxdb2 import PermissionResource, Permission, Authorization
from influxdb2.rest import ApiException
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

    def test_createAuthorizationTask(self):
        resource = PermissionResource(org_id=self.organization.id, type="tasks")
        create_task = Permission(action="read", resource=resource)

        delete_task = Permission(action="write", resource=resource)

        permissions = [create_task, delete_task]
        authorization = self.authorizations_client.create_authorization(self.organization.id, permissions)

        self.assertTrue(len(authorization.permissions) == 2)

        self.assertEqual(authorization.permissions[0].resource.type, "tasks")
        self.assertEqual(authorization.permissions[0].resource.org_id, self.organization.id)
        self.assertEqual(authorization.permissions[0].action, "read")

        self.assertEqual(authorization.permissions[1].resource.type, "tasks")
        self.assertEqual(authorization.permissions[1].resource.org_id, self.organization.id)
        self.assertEqual(authorization.permissions[1].action, "write")

    def test_createAuthorizationBucket(self):
        organization = self.client.organizations_client().create_organization(self.generate_name("Auth Organization"))
        bucket = self.client.buckets_client().create_bucket(bucket_name=self.generate_name("Auth Bucket"),
                                                            retention_rules=BaseTest.retention_rule(),
                                                            org_id=self.organization.id)
        resource = PermissionResource(org_id=organization.id, type="buckets", id=bucket.id)
        create_bucket = Permission(action="read", resource=resource)
        delete_bucket = Permission(action="write", resource=resource)
        permissions = [create_bucket, delete_bucket]

        authorization = self.authorizations_client.create_authorization(organization.id, permissions)

        self.log(authorization)
        self.assertTrue(len(authorization.permissions) == 2)
        self.assertEqual(authorization.permissions[0].resource.id, bucket.id)
        self.assertEqual(authorization.permissions[0].resource.type, "buckets")
        self.assertEqual(authorization.permissions[0].action, "read")
        self.assertEqual(authorization.permissions[1].resource.id, bucket.id)
        self.assertEqual(authorization.permissions[1].resource.type, "buckets")
        self.assertEqual(authorization.permissions[1].action, "write")

    def test_findAuthorizationsByID(self):
        with pytest.raises(ApiException) as e:
            self.client.authorizations_client().find_authorization_by_id("020f755c3c082000")
        self.assertEquals(e.value.status, 404)
        self.assertIn("authorization not found", e.value.body)

    def test_findAuthorizations(self):
        authorizations = self.client.authorizations_client().find_authorizations()
        size = len(authorizations)
        self.client.authorizations_client().create_authorization(org_id=self.organization.id,
                                                                 permissions=self.new_permissions())

    def test_findAuthorizationsByUser(self):
        size = len(self.client.authorizations_client().find_authorizations_by_user(self.user))
        self.client.authorizations_client().create_authorization(org_id=self.organization.id,
                                                                 permissions=self.new_permissions())

        self.assertEquals(len(self.client.authorizations_client().find_authorizations_by_user(user=self.user)),
                          size + 1)

    def test_findAuthorizationsByUserName(self):
        size = len(self.client.authorizations_client().find_authorizations_by_user_name(self.user.name))
        self.client.authorizations_client().create_authorization(org_id=self.organization.id,
                                                                 permissions=self.new_permissions())

        self.assertEquals(
            len(self.client.authorizations_client().find_authorizations_by_user_name(user_name=self.user.name)),
            size + 1)

    def test_findAuthorizationsByOrg(self):
        size = len(self.client.authorizations_client().find_authorizations_by_org(self.organization))
        self.client.authorizations_client().create_authorization(org_id=self.organization.id,
                                                                 permissions=self.new_permissions())

        self.assertEquals(len(self.client.authorizations_client().find_authorizations_by_org(self.organization)),
                          size + 1)

    def test_findAuthorizationsByOrgNotFound(self):
        authorizationsByOrgID = self.client.authorizations_client().find_authorizations_by_org_id("ffffffffffffffff")
        self.assertEquals(len(authorizationsByOrgID), 0)

    def new_permissions(self):
        resource = PermissionResource(org_id=self.organization.id, type="users")
        permission = Permission(action="read", resource=resource)
        return [permission]
