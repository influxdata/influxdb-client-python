import datetime
import time
import unittest

import influxdb2
from influxdb2 import BucketRetentionRules
from influxdb2.client.influxdb_client import InfluxDBClient

current_milli_time = lambda: int(round(time.time() * 1000))


def generate_bucket_name():
    return "test_bucket_" + str(datetime.datetime.now().timestamp()) + "_IT"


class BaseTest(unittest.TestCase):

    def setUp(self) -> None:
        conf = influxdb2.configuration.Configuration()
        conf.host = "http://localhost:9999/api/v2"
        conf.debug = True
        auth_token = "my-token-123"
        self.org = "my-org"
        self.bucket = "test_bucket"

        self.client = InfluxDBClient(conf.host, auth_token, debug=conf.debug, org="my-org")
        self.api_client = self.client.api_client

        self.write_api = influxdb2.api.write_api.WriteApi(self.api_client)
        self.write_client = self.client.write_client()

        self.query_client = self.client.query_client()
        self.buckets_client = self.client.buckets_client()
        self.my_organization = self.find_my_org()
        self.users_client = self.client.users_client()
        self.organizations_client = self.client.organizations_client()
        self.authorizations_client = self.client.authorizations_client()

    def tearDown(self) -> None:
        self.client.__del__()

    def create_test_bucket(self):
        bucket_name = generate_bucket_name()
        bucket = self.buckets_client.create_bucket(bucket_name=bucket_name, org_id=self.my_organization.id,
                                                   description=bucket_name + "description")
        return bucket

    def delete_test_bucket(self, bucket):
        return self.buckets_client.delete_bucket(bucket)

    def find_my_org(self):
        org_api = influxdb2.api.organizations_api.OrganizationsApi(self.api_client)
        orgs = org_api.get_orgs()
        for org in orgs.orgs:
            if org.name == self.org:
                return org

        return None

    @staticmethod
    def log(args):
        print(">>>", args)

    @staticmethod
    def generate_name(prefix):
        assert prefix != "" or prefix is not None
        return prefix + str(current_milli_time) + "-IT"

    @classmethod
    def retention_rule(cls) -> BucketRetentionRules:
        return BucketRetentionRules(type='expire', every_seconds=3600)
