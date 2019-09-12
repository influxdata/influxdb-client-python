import datetime
import os
import re
import time
import unittest

import influxdb2
from influxdb2 import BucketRetentionRules, Organization
from influxdb2.client.influxdb_client import InfluxDBClient

current_milli_time = lambda: int(round(time.time() * 1000))


def generate_bucket_name():
    return "test_bucket_" + str(datetime.datetime.now().timestamp()) + "_IT"


class BaseTest(unittest.TestCase):

    def setUp(self) -> None:
        self.conf = influxdb2.configuration.Configuration()
        self.host = "http://{0}:9999/api/v2".format(os.getenv("INFLUXDB_IP", "localhost"))
        self.debug = False
        self.auth_token = "my-token"
        self.org = "my-org"
        self.bucket = "test_bucket"

        self.client = InfluxDBClient(url=self.host, token=self.auth_token, debug=self.conf.debug, org=self.org)
        self.api_client = self.client.api_client

        self.query_api = self.client.query_api()
        self.buckets_api = self.client.buckets_api()
        self.users_api = self.client.users_api()
        self.organizations_api = self.client.organizations_api()
        self.authorizations_api = self.client.authorizations_api()
        self.labels_api = self.client.labels_api()

        self.my_organization = self.find_my_org()

    def tearDown(self) -> None:
        self.client.__del__()

    def create_test_bucket(self):
        bucket_name = generate_bucket_name()
        bucket = self.buckets_api.create_bucket(bucket_name=bucket_name, org_id=self.my_organization.id,
                                                description=bucket_name + "description")
        return bucket

    def delete_test_bucket(self, bucket):
        return self.buckets_api.delete_bucket(bucket)

    def find_my_org(self) -> Organization:
        org_api = influxdb2.service.organizations_service.OrganizationsService(self.api_client)
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
