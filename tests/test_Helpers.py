from influxdb_client import InfluxDBClient, Organization
# noinspection PyProtectedMember
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
        org = get_org_query_param("not_exist_name", self.client, required_id=True)
        self.assertIsNone(org)

    def test_both_none(self):
        self.client.close()
        self.client = InfluxDBClient(url=self.client.url, token="my-token")
        org = get_org_query_param(None, self.client)
        self.assertIsNone(org)
