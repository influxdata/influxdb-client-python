import json
import unittest

import httpretty

from influxdb_client import InfluxDBClient, ChecksService
from influxdb_client.domain.check_status_level import CheckStatusLevel
from influxdb_client.domain.dashboard_query import DashboardQuery
from influxdb_client.domain.greater_threshold import GreaterThreshold
from influxdb_client.domain.lesser_threshold import LesserThreshold
from influxdb_client.domain.query_edit_mode import QueryEditMode
from influxdb_client.domain.range_threshold import RangeThreshold
from influxdb_client.domain.task_status_type import TaskStatusType
from influxdb_client.domain.threshold_check import ThresholdCheck
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


class ThresholdZeroValueTest(unittest.TestCase):
    """Unit tests: threshold bounds must distinguish 0 from None (#681)."""

    def test_range_threshold_accepts_int_zero_min_and_max(self):
        threshold = RangeThreshold(
            level=CheckStatusLevel.OK, min=0, max=0, within=True
        )
        self.assertEqual(0, threshold.min)
        self.assertEqual(0, threshold.max)
        self.assertIs(True, threshold.within)

    def test_range_threshold_accepts_float_zero_min(self):
        threshold = RangeThreshold(
            level=CheckStatusLevel.CRIT, min=0.0, max=10.0, within=False
        )
        self.assertEqual(0.0, threshold.min)
        self.assertEqual(10.0, threshold.max)
        self.assertIs(False, threshold.within)

    def test_range_threshold_rejects_none_min(self):
        with self.assertRaises(ValueError) as ctx:
            RangeThreshold(level=CheckStatusLevel.OK, min=None, max=10, within=True)
        self.assertIn("`min`", str(ctx.exception))
        self.assertIn("None", str(ctx.exception))

    def test_range_threshold_rejects_none_max(self):
        with self.assertRaises(ValueError) as ctx:
            RangeThreshold(level=CheckStatusLevel.OK, min=0, max=None, within=True)
        self.assertIn("`max`", str(ctx.exception))

    def test_range_threshold_rejects_none_within(self):
        with self.assertRaises(ValueError) as ctx:
            RangeThreshold(level=CheckStatusLevel.OK, min=0, max=10, within=None)
        self.assertIn("`within`", str(ctx.exception))

    def test_range_threshold_serialization_keeps_zero(self):
        from influxdb_client._sync.api_client import ApiClient

        threshold = RangeThreshold(
            level=CheckStatusLevel.OK, min=0, max=10, within=False, all_values=False
        )
        payload = ApiClient().sanitize_for_serialization(threshold)
        self.assertEqual(0, payload["min"])
        self.assertEqual(10, payload["max"])
        self.assertIs(False, payload["within"])
        self.assertIs(False, payload["allValues"])
        self.assertIn("min", payload)

    def test_range_threshold_deserialization_keeps_zero(self):
        from influxdb_client._sync.api_client import ApiClient

        data = {
            "type": "range",
            "min": 0,
            "max": 10,
            "within": True,
            "level": "OK",
        }
        obj = ApiClient()._ApiClient__deserialize(data, "RangeThreshold")
        self.assertIsInstance(obj, RangeThreshold)
        self.assertEqual(0.0, obj.min)
        self.assertEqual(10.0, obj.max)

    def test_greater_and_lesser_accept_zero_value(self):
        greater = GreaterThreshold(level=CheckStatusLevel.CRIT, value=0)
        lesser = LesserThreshold(level=CheckStatusLevel.WARN, value=0.0)
        self.assertEqual(0, greater.value)
        self.assertEqual(0.0, lesser.value)

        from influxdb_client._sync.api_client import ApiClient

        ac = ApiClient()
        self.assertEqual(0, ac.sanitize_for_serialization(greater)["value"])
        self.assertEqual(0.0, ac.sanitize_for_serialization(lesser)["value"])

    def test_greater_rejects_none_value(self):
        with self.assertRaises(ValueError) as ctx:
            GreaterThreshold(level=CheckStatusLevel.CRIT, value=None)
        self.assertIn("`value`", str(ctx.exception))

    def test_create_check_serializes_zero_min(self):
        """Issue #681 reproduction: create_check payload must keep min=0."""
        httpretty.enable()
        httpretty.reset()
        try:
            created = {
                "id": "check-01",
                "orgID": "org_id",
                "name": "zero-bound-check",
                "type": "threshold",
                "query": {"text": "from(bucket:\"b\")", "editMode": "advanced"},
                "thresholds": [{
                    "type": "range",
                    "min": 0,
                    "max": 10,
                    "within": True,
                    "level": "OK",
                }],
                "status": "active",
            }
            httpretty.register_uri(
                httpretty.POST,
                uri="http://localhost/api/v2/checks",
                status=201,
                body=json.dumps(created),
                adding_headers={"Content-Type": "application/json"},
            )
            client = InfluxDBClient("http://localhost", "my-token", org="my-org", debug=False)
            try:
                threshold = RangeThreshold(
                    level=CheckStatusLevel.OK, min=0, max=10, within=True
                )
                check = ThresholdCheck(
                    name="zero-bound-check",
                    status_message_template="The value is on: ${ r._level } level!",
                    every="5s",
                    offset="0s",
                    query=DashboardQuery(
                        edit_mode=QueryEditMode.ADVANCED, text='from(bucket:"b")'
                    ),
                    thresholds=[threshold],
                    org_id="org_id",
                    status=TaskStatusType.ACTIVE,
                )
                result = ChecksService(api_client=client.api_client).create_check(check)
                self.assertEqual("check-01", result.id)
                request_body = json.loads(httpretty.last_request().body.decode("utf-8"))
                self.assertEqual(0, request_body["thresholds"][0]["min"])
                self.assertEqual(10, request_body["thresholds"][0]["max"])
                self.assertTrue(request_body["thresholds"][0]["within"])
                self.assertIn("min", request_body["thresholds"][0])
            finally:
                client.close()
        finally:
            httpretty.disable()
