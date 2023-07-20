import gzip

import httpretty

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from tests.base_test import BaseTest


class GzipSupportTest(BaseTest):

    def setUp(self) -> None:
        super(GzipSupportTest, self).setUp()

        httpretty.enable()
        httpretty.reset()

    def tearDown(self) -> None:
        self.client.close()
        httpretty.disable()

    def test_gzip_disabled(self):
        query_response = \
            "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string\n" \
            "#group,false,false,false,false,false,false,false,false,false,true\n#default,_result,,,,,,,,\n" \
            ",result,table,_start,_stop,_time,_value,_field,_measurement,host\n" \
            ",,0,1970-01-01T00:00:10Z,1970-01-01T00:00:20Z,1970-01-01T00:00:10Z,121,free,mem,A\n" \
            ",,0,1970-01-01T00:00:10Z,1970-01-01T00:00:20Z,1970-01-01T00:00:10Z,122,free,mem,A\n" \
            ",,0,1970-01-01T00:00:10Z,1970-01-01T00:00:20Z,1970-01-01T00:00:10Z,123,free,mem,A\n" \
            ",,0,1970-01-01T00:00:10Z,1970-01-01T00:00:20Z,1970-01-01T00:00:10Z,124,free,mem,A\n"
        httpretty.register_uri(httpretty.GET, uri="http://localhost/api/v2/me", status=200, body="{\"name\":\"Tom\"}",
                               adding_headers={'Content-Type': 'application/json'})
        httpretty.register_uri(httpretty.POST, uri="http://localhost/api/v2/write", status=204)
        httpretty.register_uri(httpretty.POST, uri="http://localhost/api/v2/query", status=200, body=query_response)

        self.client = InfluxDBClient("http://localhost", "my-token", org="my-org", enable_gzip=False)

        _user = self.client.users_api().me()
        self.assertEqual("Tom", _user._name)

        _response = self.client.write_api(write_options=SYNCHRONOUS) \
            .write("my-bucket", "my-org", "h2o_feet,location=coyote_creek water_level=1 1")
        self.assertEqual(None, _response)

        _tables = self.client.query_api() \
            .query('from(bucket:"my-bucket") |> range(start: 1970-01-01T00:00:00.000000001Z) |> last()', "my-org")
        self.assertEqual(1, len(_tables))
        self.assertEqual(4, len(_tables[0].records))
        self.assertEqual(121, _tables[0].records[0].get_value())
        self.assertEqual(122, _tables[0].records[1].get_value())
        self.assertEqual(123, _tables[0].records[2].get_value())
        self.assertEqual(124, _tables[0].records[3].get_value())

        _requests = httpretty.httpretty.latest_requests
        self.assertEqual(3, len(_requests))

        # Unsupported
        self.assertEqual("/api/v2/me", _requests[0].path)
        self.assertEqual(None, _requests[0].headers['Content-Encoding'])
        self.assertEqual("identity", _requests[0].headers['Accept-Encoding'])
        # Write
        self.assertEqual("/api/v2/write?org=my-org&bucket=my-bucket&precision=ns", _requests[1].path)
        self.assertEqual("identity", _requests[1].headers['Accept-Encoding'])
        self.assertEqual("h2o_feet,location=coyote_creek water_level=1 1", _requests[1].parsed_body)
        # Query
        self.assertEqual("/api/v2/query?org=my-org", _requests[2].path)
        self.assertEqual(None, _requests[2].headers['Content-Encoding'])
        self.assertEqual("identity", _requests[2].headers['Accept-Encoding'])
        self.assertTrue('from(bucket:"my-bucket") |> range(start: 1970-01-01T00:00:00.000000001Z) |> last()' in str(
            _requests[2].parsed_body))

    def test_gzip_enabled(self):
        query_response = \
            "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string\n" \
            "#group,false,false,false,false,false,false,false,false,false,true\n#default,_result,,,,,,,,\n" \
            ",result,table,_start,_stop,_time,_value,_field,_measurement,host\n" \
            ",,0,1970-01-01T00:00:10Z,1970-01-01T00:00:20Z,1970-01-01T00:00:10Z,121,free,mem,A\n" \
            ",,0,1970-01-01T00:00:10Z,1970-01-01T00:00:20Z,1970-01-01T00:00:10Z,122,free,mem,A\n" \
            ",,0,1970-01-01T00:00:10Z,1970-01-01T00:00:20Z,1970-01-01T00:00:10Z,123,free,mem,A\n" \
            ",,0,1970-01-01T00:00:10Z,1970-01-01T00:00:20Z,1970-01-01T00:00:10Z,124,free,mem,A\n"
        httpretty.register_uri(httpretty.GET, uri="http://localhost/api/v2/me", status=200, body="{\"name\":\"Tom\"}",
                               adding_headers={'Content-Type': 'application/json'})
        httpretty.register_uri(httpretty.POST, uri="http://localhost/api/v2/write", status=204)
        httpretty.register_uri(httpretty.POST, uri="http://localhost/api/v2/query", status=200,
                               body=gzip.compress(bytes(query_response, "utf-8")),
                               adding_headers={'Content-Encoding': 'gzip'})

        self.client = InfluxDBClient("http://localhost", "my-token", org="my-org", enable_gzip=True)
        _user = self.client.users_api().me()
        self.assertEqual("Tom", _user._name)

        _response = self.client.write_api(write_options=SYNCHRONOUS) \
            .write("my-bucket", "my-org", "h2o_feet,location=coyote_creek water_level=1 1")
        self.assertEqual(None, _response)

        _tables = self.client.query_api() \
            .query('from(bucket:"my-bucket") |> range(start: 1970-01-01T00:00:00.000000001Z) |> last()', "my-org")
        self.assertEqual(1, len(_tables))
        self.assertEqual(4, len(_tables[0].records))
        self.assertEqual(121, _tables[0].records[0].get_value())
        self.assertEqual(122, _tables[0].records[1].get_value())
        self.assertEqual(123, _tables[0].records[2].get_value())
        self.assertEqual(124, _tables[0].records[3].get_value())

        _requests = httpretty.httpretty.latest_requests
        self.assertEqual(3, len(_requests))

        # Unsupported
        self.assertEqual("/api/v2/me", _requests[0].path)
        self.assertEqual(None, _requests[0].headers['Content-Encoding'])
        self.assertEqual("identity", _requests[0].headers['Accept-Encoding'])
        # Write
        self.assertEqual("/api/v2/write?org=my-org&bucket=my-bucket&precision=ns", _requests[1].path)
        self.assertEqual("gzip", _requests[1].headers['Content-Encoding'])
        self.assertEqual("identity", _requests[1].headers['Accept-Encoding'])
        self.assertNotEqual("h2o_feet,location=coyote_creek water_level=1 1", _requests[1].parsed_body)
        # Query
        self.assertEqual("/api/v2/query?org=my-org", _requests[2].path)
        self.assertEqual(None, _requests[2].headers['Content-Encoding'])
        self.assertEqual("gzip", _requests[2].headers['Accept-Encoding'])
        self.assertTrue('from(bucket:"my-bucket") |> range(start: 1970-01-01T00:00:00.000000001Z) |> last()' in str(
            _requests[2].parsed_body))

    def test_write_query_gzip(self):
        httpretty.disable()

        self.client = InfluxDBClient(self.host, token="my-token", org="my-org", debug=False,
                                     enable_gzip=True)
        self.api_client = self.client.api_client
        self.buckets_client = self.client.buckets_api()
        self.query_client = self.client.query_api()
        self.org = "my-org"
        self.my_organization = self.find_my_org()

        _bucket = self.create_test_bucket()

        self.client.write_api(write_options=SYNCHRONOUS) \
            .write(_bucket.name, self.org, "h2o_feet,location=coyote_creek water_level=111 1")

        _result = self.query_client.query(
            f"from(bucket:\"{_bucket.name}\") |> range(start: 1970-01-01T00:00:00.000000001Z)", self.org)

        self.assertEqual(len(_result), 1)
        self.assertEqual(_result[0].records[0].get_measurement(), "h2o_feet")
        self.assertEqual(_result[0].records[0].get_value(), 111.0)
        self.assertEqual(_result[0].records[0].get_field(), "water_level")
