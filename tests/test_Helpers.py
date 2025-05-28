import pytest

from influxdb_client import InfluxDBClient, Organization, PermissionResource, Permission
# noinspection PyProtectedMember
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.util.helpers import get_org_query_param, _is_id
from tests.base_test import BaseTest


class HelpersTest(BaseTest):

    def test_is_id(self):
        self.assertTrue(_is_id("ffffffffffffffff"))
        self.assertTrue(_is_id("020f755c3c082000"))
        self.assertTrue(_is_id("ca55e77eca55e77e"))
        self.assertTrue(_is_id("02def021097c6000"))
        self.assertFalse(_is_id("gggggggggggggggg"))
        self.assertFalse(_is_id("abc"))
        self.assertFalse(_is_id("abcdabcdabcdabcd0"))
        self.assertFalse(_is_id("020f75"))
        self.assertFalse(_is_id("020f755c3c082000aaa"))
        self.assertFalse(_is_id(None))

    def test_organization_as_query_param(self):
        organization = Organization(id="org-id", name="org-name")
        org = get_org_query_param(organization, self.client)
        self.assertEqual("org-id", org)

    def test_required_id(self):
        org = get_org_query_param(None, self.client, required_id=True)
        self.assertEqual(self.my_organization.id, org)

    def test_required_id_not_exist(self):
        with pytest.raises(InfluxDBError) as e:
            get_org_query_param("not_exist_name", self.client, required_id=True)
        assert "The client cannot find organization with name: 'not_exist_name' to determine their ID." in f"{e.value} "

    def test_both_none(self):
        self.client.close()
        self.client = InfluxDBClient(url=self.client.url, token="my-token")
        org = get_org_query_param(None, self.client)
        self.assertIsNone(org)

    def test_not_permission_to_read_org(self):
        # Create Token without permission to read Organizations
        resource = PermissionResource(type="buckets", org_id=self.find_my_org().id)
        authorization = self.client \
            .authorizations_api() \
            .create_authorization(org_id=self.find_my_org().id,
                                  permissions=[Permission(resource=resource, action="read"),
                                               Permission(resource=resource, action="write")])
        self.client.close()

        # Initialize client without permission to read Organizations
        self.client = InfluxDBClient(url=self.client.url, token=authorization.token)

        with pytest.raises(InfluxDBError) as e:
            get_org_query_param("my-org", self.client, required_id=True)
        assert "The client cannot find organization with name: 'my-org' to determine their ID. Are you using token " \
               "with sufficient permission?" in f"{e.value} "
