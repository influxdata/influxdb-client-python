import unittest

import pytest

from influxdb_client import InfluxDBClient, BucketSchemasService
from influxdb_client.client.warnings import CloudOnlyWarning


class PointWarnings(unittest.TestCase):

    def test_cloud_only_warning(self):
        with pytest.warns(CloudOnlyWarning) as warnings:
            with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org") as client:
                BucketSchemasService(api_client=client.api_client)
        self.assertEqual(1, len(warnings))
