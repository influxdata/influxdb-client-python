import pytest

from influxdb_client import PermissionResource, Permission, Authorization, AuthorizationsApi
from influxdb_client.rest import ApiException
from tests.base_test import BaseTest


class AuthorizationsClientTest(BaseTest):

    def setUp(self) -> None:
        super(AuthorizationsClientTest, self).setUp()
        self.user = self.users_api.me()
        self.organization = self.find_my_org()

    def test_createAuthorization(self):
        user_resource = PermissionResource(org_id=self.organization.id, type="users")
        read_users = Permission(action="read", resource=user_resource)

        org_resource = PermissionResource(org_id=self.organization.id, type="orgs")
        write_organizations = Permission(action="write", resource=org_resource)

        permissions = [read_users, write_organizations]
        authorization = self.authorizations_api.create_authorization(self.organization.id, permissions)

        self.log(authorization)

        self.assertIsNotNone(authorization)
        self.assertTrue(len(authorization.token) > 0)
        self.assertEqual(authorization.user_id, self.user.id)
        self.assertEqual(authorization.user, self.user.name)
        self.assertEqual(authorization.org_id, self.organization.id)
        self.assertEqual(authorization.org, self.organization.name)
        self.assertEqual(authorization.status, "active")
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

    def test_AuthorizationTypeAssert(self):
        self.assertRaisesRegex(TypeError, "org_id must be a string.", Authorization, org_id={})
        self.assertRaisesRegex(TypeError, "permissions must be a list.", Authorization, permissions={})

    def test_createAuthorizationWrongTypes(self):
        user_resource = PermissionResource(org_id=self.organization.id, type="users")
        read_users = Permission(action="read", resource=user_resource)

        org_resource = PermissionResource(org_id=self.organization.id, type="orgs")
        write_organizations = Permission(action="write", resource=org_resource)

        permissions = [read_users, write_organizations]
        self.assertRaisesRegex(TypeError, "org_id must be a string.",
                               self.authorizations_api.create_authorization, permissions)
        self.assertRaisesRegex(TypeError, "permissions must be a list",
                               self.authorizations_api.create_authorization, "123456789ABCDEF0", "Foo")
        self.assertRaisesRegex(TypeError, "Attempt to use non-Authorization value for authorization: Foo",
                               self.authorizations_api.create_authorization, "123456789ABCDEF0", permissions, "Foo")

    def test_authorizationDescription(self):
        organization = self.my_organization

        resource = PermissionResource(org_id=organization.id, type="sources")
        create_source = Permission(action="write", resource=resource)

        permissions = [create_source]
        authorization = Authorization(org_id=organization.id, permissions=permissions)
        authorization.status = "active"
        authorization.description = "My description!"
        created = self.authorizations_api.create_authorization(authorization=authorization)
        self.assertIsNotNone(created)
        self.assertEqual(created.description, "My description!")

    def test_createAuthorizationTask(self):
        resource = PermissionResource(org_id=self.organization.id, type="tasks")
        create_task = Permission(action="read", resource=resource)

        delete_task = Permission(action="write", resource=resource)

        permissions = [create_task, delete_task]
        authorization = self.authorizations_api.create_authorization(self.organization.id, permissions)

        self.assertTrue(len(authorization.permissions) == 2)

        self.assertEqual(authorization.permissions[0].resource.type, "tasks")
        self.assertEqual(authorization.permissions[0].resource.org_id, self.organization.id)
        self.assertEqual(authorization.permissions[0].action, "read")

        self.assertEqual(authorization.permissions[1].resource.type, "tasks")
        self.assertEqual(authorization.permissions[1].resource.org_id, self.organization.id)
        self.assertEqual(authorization.permissions[1].action, "write")

    def test_createAuthorizationBucket(self):
        organization = self.client.organizations_api().create_organization(self.generate_name("Auth Organization"))
        bucket = self.client.buckets_api().create_bucket(bucket_name=self.generate_name("Auth Bucket"),
                                                         retention_rules=BaseTest.retention_rule(),
                                                         org=self.organization)
        resource = PermissionResource(org_id=organization.id, type="buckets", id=bucket.id)
        create_bucket = Permission(action="read", resource=resource)
        delete_bucket = Permission(action="write", resource=resource)
        permissions = [create_bucket, delete_bucket]

        authorization = self.authorizations_api.create_authorization(organization.id, permissions)

        self.log(authorization)
        self.assertTrue(len(authorization.permissions) == 2)
        self.assertEqual(authorization.permissions[0].resource.id, bucket.id)
        self.assertEqual(authorization.permissions[0].resource.type, "buckets")
        self.assertEqual(authorization.permissions[0].action, "read")
        self.assertEqual(authorization.permissions[1].resource.id, bucket.id)
        self.assertEqual(authorization.permissions[1].resource.type, "buckets")
        self.assertEqual(authorization.permissions[1].action, "write")
        self.delete_test_bucket(bucket)

    def test_findAuthorizationsByID(self):
        with pytest.raises(ApiException) as e:
            self.authorization_api().find_authorization_by_id("020f755c3c082000")
        self.assertEqual(e.value.status, 404)
        self.assertIn("authorization not found", e.value.body)

    def test_findAuthorizations(self):
        authorizations = self.authorization_api().find_authorizations()
        size = len(authorizations)
        self.authorization_api().create_authorization(org_id=self.organization.id,
                                                      permissions=self.new_permissions())

    def test_findAuthorizationsByUser(self):
        size = len(self.authorization_api().find_authorizations_by_user(self.user))
        self.authorization_api().create_authorization(org_id=self.organization.id,
                                                      permissions=self.new_permissions())

        self.assertEqual(len(self.authorization_api().find_authorizations_by_user(user=self.user)),
                         size + 1)

    def test_findAuthorizationsByUserName(self):
        size = len(self.authorization_api().find_authorizations_by_user_name(self.user.name))
        self.authorization_api().create_authorization(org_id=self.organization.id,
                                                      permissions=self.new_permissions())

        self.assertEqual(
            len(self.authorization_api().find_authorizations_by_user_name(user_name=self.user.name)),
            size + 1)

    def test_findAuthorizationsByOrg(self):
        size = len(self.authorization_api().find_authorizations_by_org(self.organization))
        self.authorization_api().create_authorization(org_id=self.organization.id,
                                                      permissions=self.new_permissions())

        self.assertEqual(len(self.authorization_api().find_authorizations_by_org(self.organization)),
                         size + 1)

    def test_findAuthorizationsByOrgNotFound(self):
        authorizations_by_org_id = self.authorization_api().find_authorizations_by_org_id("ffffffffffffffff")
        self.assertEqual(len(authorizations_by_org_id), 0)

    def test_updateAuthorizationStatus(self):
        resource = PermissionResource(org_id=self.organization.id, type="users")
        read_user = Permission(action="read", resource=resource)
        permissions = [read_user]

        authorization = self.authorization_api().create_authorization(org_id=self.my_organization.id,
                                                                      permissions=permissions)
        self.assertEqual(authorization.status, "active")

        authorization.status = "inactive"
        authorization = self.authorization_api().update_authorization(authorization)

        self.assertEqual(authorization.status, "inactive")

        authorization.status = "active"
        authorization = self.authorization_api().update_authorization(authorization)
        self.assertEqual(authorization.status, "active")

    def test_deleteAuthorization(self):
        create_authorization = self.authorization_api().create_authorization(self.organization.id,
                                                                             self.new_permissions())
        self.assertIsNotNone(create_authorization)

        found_authorization = self.authorization_api().find_authorization_by_id(create_authorization.id)
        self.assertIsNotNone(found_authorization)

        self.authorization_api().delete_authorization(create_authorization)

        with pytest.raises(ApiException) as e:
            self.authorization_api().find_authorization_by_id(create_authorization.id)
        self.assertIn("authorization not found", e.value.body)

    def test_clone_authorization(self):
        source = self.authorization_api().create_authorization(self.organization.id, self.new_permissions())
        cloned = self.authorization_api().clone_authorization(source.id)

        self.assertIsNotNone(cloned.token)
        self.assertNotEqual(source.token, cloned.token)
        self.assertNotEqual(source.id, cloned.id)

        self.assertEqual(source.user_id, cloned.user_id)
        self.assertEqual(source.user, cloned.user)
        self.assertEqual(source.org_id, cloned.org_id)
        self.assertEqual(source.org, cloned.org)
        self.assertEqual(source.status, cloned.status)
        self.assertEqual(source.description, cloned.description)

        self.assertTrue(len(cloned.permissions), 1)
        self.assertEqual(cloned.permissions[0].action, "read")
        self.assertEqual(cloned.permissions[0].resource.type, "users")
        self.assertEqual(cloned.permissions[0].resource.org_id, self.organization.id)

    def test_cloneAuthorizationNotFound(self):
        with pytest.raises(ApiException) as e:
            self.authorization_api().find_authorization_by_id("020f755c3c082000")
        self.assertIn("authorization not found", e.value.body)

    def authorization_api(self) -> AuthorizationsApi:
        return self.client.authorizations_api()

    def new_permissions(self):
        resource = PermissionResource(org_id=self.organization.id, type="users")
        permission = Permission(action="read", resource=resource)
        return [permission]
