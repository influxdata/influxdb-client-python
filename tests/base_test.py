import datetime
import os
import re
import time
import unittest

import influxdb_client
from influxdb_client import BucketRetentionRules, Organization, InfluxDBClient

current_milli_time = lambda: int(round(time.time() * 1000))


def generate_bucket_name():
    return "test_bucket_" + str(datetime.datetime.now().timestamp()) + "_IT"


class BaseTest(unittest.TestCase):

    def setUp(self) -> None:
        self.conf = influxdb_client.configuration.Configuration()

        self.host = os.getenv('INFLUXDB_V2_URL', "http://localhost:8086")
        self.debug = False
        self.auth_token = os.getenv('INFLUXDB_V2_TOKEN', "my-token")
        self.org = os.getenv('INFLUXDB_V2_ORG', "my-org")

        self.client = InfluxDBClient(url=self.host, token=self.auth_token, debug=self.debug, org=self.org)
        self.api_client = self.client.api_client

        self.query_api = self.client.query_api()
        self.buckets_api = self.client.buckets_api()
        self.users_api = self.client.users_api()
        self.organizations_api = self.client.organizations_api()
        self.authorizations_api = self.client.authorizations_api()
        self.labels_api = self.client.labels_api()

        self.my_organization = self.find_my_org()

    def tearDown(self) -> None:
        self.client.close()

    def create_test_bucket(self):
        bucket_name = generate_bucket_name()
        bucket = self.buckets_api.create_bucket(bucket_name=bucket_name, org_id=self.my_organization.id,
                                                description=bucket_name + "description")
        return bucket

    def delete_test_bucket(self, bucket):
        return self.buckets_api.delete_bucket(bucket)

    def find_my_org(self) -> Organization:
        org_api = influxdb_client.service.organizations_service.OrganizationsService(self.api_client)
        orgs = org_api.get_orgs()
        for org in orgs.orgs:
            if org.name == self.org:
                return org

    @staticmethod
    def log(args):
        print(">>>", args)

    @staticmethod
    def generate_name(prefix):
        assert prefix != "" or prefix is not None
        return prefix + str(datetime.datetime.now().timestamp()) + "-IT"

    @classmethod
    def retention_rule(cls) -> BucketRetentionRules:
        return BucketRetentionRules(type='expire', every_seconds=3600)

    def assertEqualIgnoringWhitespace(self, first, second, msg=None) -> None:
        whitespace_pattern = re.compile(r"\s+")
        self.assertEqual(whitespace_pattern.sub("", first), whitespace_pattern.sub("", second), msg=msg)
