import json

import httpretty

from influxdb_client import InfluxDBClient, ChecksService
from tests.base_test import BaseTest


class ThresholdsClientTest(BaseTest):

    def setUp(self) -> None:
        super(ThresholdsClientTest, self).setUp()

        httpretty.enable()
        httpretty.reset()

    def tearDown(self) -> None:
        self.client.close()
        httpretty.disable()

    def test_threshold(self):
        dictionary = {
            "id": "01",
            "orgID": "org_id",
            "name": "name",
            "type": "threshold",
            "query": "query",
            "thresholds": [{
                "allValues": False,
                "level": "CRIT",
                "value": 10.5,
                "type": "greater"
            }],
        }
        httpretty.register_uri(httpretty.GET, uri="http://localhost/api/v2/checks/01", status=200,
                               body=json.dumps(dictionary, indent=2),
                               adding_headers={'Content-Type': 'application/json'})
        self.client = InfluxDBClient("http://localhost", "my-token", org="my-org", debug=True)
        checks_service = ChecksService(api_client=self.client.api_client)
        check = checks_service.get_checks_id(check_id="01")
        self.assertEqual(1, len(check.thresholds))
        self.assertEqual(False, check.thresholds[0].all_values)
        self.assertEqual(10.5, check.thresholds[0].value)
        self.assertEqual("CRIT", check.thresholds[0].level)
        self.assertEqual("greater", check.thresholds[0].type)
