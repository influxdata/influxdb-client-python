from datetime import datetime, timezone

from influxdb_client import PermissionResource, Permission, InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from tests.base_test import BaseTest


class DeleteApiTest(BaseTest):

    def setUp(self) -> None:
        super(DeleteApiTest, self).setUp()
        response = self.buckets_api.find_buckets()

        for bucket in response.buckets:
            if bucket.name.endswith("_IT"):
                self.buckets_api.delete_bucket(bucket)

        self.bucket = self.create_test_bucket()
        self.organization = self.find_my_org()

        resource = PermissionResource(type="buckets", org_id=self.organization.id, id=self.bucket.id)
        read_bucket = Permission(resource=resource, action="read")
        write_bucket = Permission(resource=resource, action="write")

        authorization = self.client.authorizations_api().create_authorization(org_id=self.organization.id,
                                                                              permissions=[read_bucket, write_bucket])
        self.auth_token = authorization.token
        self.client.close()
        self.client = InfluxDBClient(url=self.host, token=self.auth_token, org=self.org)
        self.delete_api = self.client.delete_api()

    def test_delete_buckets(self):

        self._write_data()

        q = f'from(bucket:\"{self.bucket.name}\") |> range(start: 1970-01-01T00:00:00.000000001Z)'
        flux_tables = self.client.query_api().query(query=q, org=self.organization.id)
        self.assertEqual(len(flux_tables), 1)
        self.assertEqual(len(flux_tables[0].records), 12)

        start = "1970-01-01T00:00:00.000000001Z"
        stop = "1970-01-01T00:00:00.000000012Z"
        self.delete_api.delete(start, stop, "", bucket=self.bucket.id, org=self.organization.id)

        flux_tables2 = self.client.query_api().query(
            f'from(bucket:"{self.bucket.name}") |> range(start: 1970-01-01T00:00:00.000000001Z)',
            org=self.organization.id)
        self.assertEqual(len(flux_tables2), 0)

    def test_delete_buckets_by_name(self):

        self._write_data()

        q = f'from(bucket:\"{self.bucket.name}\") |> range(start: 1970-01-01T00:00:00.000000001Z)'
        flux_tables = self.client.query_api().query(query=q, org=self.organization.id)
        self.assertEqual(len(flux_tables), 1)
        self.assertEqual(len(flux_tables[0].records), 12)

        start = "1970-01-01T00:00:00.000000001Z"
        stop = "1970-01-01T00:00:00.000000012Z"
        self._delete_and_verify(start, stop, self.organization.name)

    def test_delete_org_parameters_types(self):

        orgs = [
            self.organization,
            self.organization.id,
            self.organization.name,
            None
        ]

        for org in orgs:
            self._write_data()
            self._delete_and_verify("1970-01-01T00:00:00.000000001Z", "1970-01-01T00:00:00.000000012Z", org)

    def test_start_stop_types(self):
        starts_stops = [
            ("1970-01-01T00:00:00.000000001Z", "1970-01-01T00:00:00.000000012Z"),
            (datetime(1970, 1, 1, 0, 0, 0, 0, timezone.utc), datetime(1970, 1, 1, 0, 0, 0, 1, timezone.utc)),
            (datetime(1970, 1, 1, 0, 0, 0, 0), datetime(1970, 1, 1, 0, 0, 0, 1))
        ]
        for start_stop in starts_stops:
            self._write_data()
            self._delete_and_verify(start_stop[0], start_stop[1], self.organization.name)

    def _delete_and_verify(self, start, stop, org):
        self.delete_api.delete(start, stop, "", bucket=self.bucket.name, org=org)
        flux_tables = self.client.query_api().query(
            f'from(bucket:"{self.bucket.name}") |> range(start: 1970-01-01T00:00:00.000000001Z)',
            org=self.organization.id)
        self.assertEqual(len(flux_tables), 0)

    def _write_data(self):

        write_api = self.client.write_api(write_options=SYNCHRONOUS)
        p1 = Point(measurement_name="h2o").tag("location", "coyote_creek").field("watter_level", 7.0).time(1)
        write_api.write(bucket=self.bucket.name, org=self.organization.name, record=p1)

        p2 = Point(measurement_name="h2o").tag("location", "coyote_creek").field("watter_level", 8.0).time(2)
        write_api.write(bucket=self.bucket.name, org=self.organization.name, record=p2)

        p3 = Point(measurement_name="h2o").tag("location", "coyote_creek").field("watter_level", 9.0).time(3)
        p4 = Point(measurement_name="h2o").tag("location", "coyote_creek").field("watter_level", 10.0).time(4)
        write_api.write(bucket=self.bucket.name, org=self.organization.name, record=[p3, p4])

        p5 = Point(measurement_name="h2o").tag("location", "coyote_creek").field("watter_level", 11.0).time(5)
        p6 = Point(measurement_name="h2o").tag("location", "coyote_creek").field("watter_level", 12.0).time(6)
        write_api.write(bucket=self.bucket.name, org=self.organization.name, record=[p5, p6])

        p7 = Point(measurement_name="h2o").tag("location", "coyote_creek").field("watter_level", 8.0).time(7)
        write_api.write(bucket=self.bucket.name, org=self.organization.name, record=p7)
        p8 = Point(measurement_name="h2o").tag("location", "coyote_creek").field("watter_level", 9.0).time(8)
        write_api.write(bucket=self.bucket.name, org=self.organization.name, record=p8)

        p9 = Point(measurement_name="h2o").tag("location", "coyote_creek").field("watter_level", 9.0).time(9)
        p10 = Point(measurement_name="h2o").tag("location", "coyote_creek").field("watter_level", 11.0).time(10)
        write_api.write(bucket=self.bucket.name, org=self.organization.name, record=[p9, p10])

        p11 = Point(measurement_name="h2o").tag("location", "coyote_creek").field("watter_level", 11.0).time(11)
        p12 = Point(measurement_name="h2o").tag("location", "coyote_creek").field("watter_level", 13.0).time(12)
        write_api.write(bucket=self.bucket.name, org=self.organization.name, record=[p11, p12])
