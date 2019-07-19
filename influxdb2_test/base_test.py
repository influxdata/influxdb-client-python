import unittest

import datetime

import influxdb2
from influxdb2.client.influxdb_client import InfluxDBClient


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
        self.my_organization = self.client.find_my_org()

    def tearDown(self) -> None:
        self.client.__del__()

    def create_test_bucket(self):
        bucket_name = generate_bucket_name()
        bucket = self.buckets_client.create_bucket(bucket_name=bucket_name, org_id=self.my_organization.id,
                                                   description=bucket_name + "description")
        return bucket

    def delete_test_bucket(self, bucket):
        return self.buckets_client.delete_bucket(bucket)
