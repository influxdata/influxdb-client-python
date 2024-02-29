import json
import unittest

import httpretty
import pytest

from influxdb_client import InfluxDBClient, BucketSchemasService
from influxdb_client.client.warnings import CloudOnlyWarning


class Warnings(unittest.TestCase):

    def setUp(self) -> None:
        httpretty.enable()
        httpretty.reset()

    def tearDown(self) -> None:
        httpretty.disable()

    def test_cloud_only_warning(self):
        httpretty.register_uri(httpretty.GET, uri="http://localhost/ping",
                               status=200, body="{}", adding_headers={'X-Influxdb-Build': 'OSS'})
        httpretty.register_uri(httpretty.GET, uri="http://localhost/api/v2/buckets/01010101/schema/measurements",
                               status=200, body=json.dumps({'measurementSchemas': []}))

        with pytest.warns(CloudOnlyWarning) as warnings:
            with InfluxDBClient(url="http://localhost", token="my-token", org="my-org") as client:
                service = BucketSchemasService(api_client=client.api_client)
                service.get_measurement_schemas(bucket_id="01010101")
        warnings = [w for w in warnings if w.category == CloudOnlyWarning]
        self.assertEqual(1, len(warnings))
