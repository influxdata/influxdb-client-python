import asyncio
import dateutil.parser
import logging
import math
import re
import time
import unittest
import os
from datetime import datetime, timezone
from io import StringIO

import pandas
import pytest
import warnings
from aioresponses import aioresponses

from influxdb_client import Point, WritePrecision, BucketsService, OrganizationsService, Organizations
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client.client.query_api import QueryOptions
from influxdb_client.client.warnings import MissingPivotFunction
from influxdb_client.client.write.retry import WritesRetry
from tests.base_test import generate_name


def async_test(coro):
    def wrapper(*args, **kwargs):
        return asyncio.get_event_loop().run_until_complete(coro(*args, **kwargs))

    return wrapper


# noinspection PyMethodMayBeStatic
class InfluxDBClientAsyncTest(unittest.TestCase):

    @async_test
    async def setUp(self) -> None:
        self.client = InfluxDBClientAsync(url="http://localhost:8086", token="my-token", org="my-org")

    @async_test
    async def tearDown(self) -> None:
        if self.client:
            await self.client.close()

    def test_use_async_context_manager(self):
        self.assertIsNotNone(self.client)

    @async_test
    async def test_ping(self):
        ping = await self.client.ping()
        self.assertTrue(ping)

    @async_test
    async def test_version(self):
        version = await self.client.version()
        self.assertTrue(len(version) > 0)

    @async_test
    async def test_build(self):
        build = await self.client.build()
        self.assertEqual('oss', build.lower())

    def test_create_query_api(self):
        query_api = self.client.query_api()
        self.assertIsNotNone(query_api)

    def test_create_write_api(self):
        write_api = self.client.write_api()
        self.assertIsNotNone(write_api)

    def test_create_delete_api(self):
        delete_api = self.client.delete_api()
        self.assertIsNotNone(delete_api)

    @async_test
    async def test_query_tables(self):
        measurement = generate_name("measurement")
        await self._prepare_data(measurement)
        query = f'''
                    from(bucket:"my-bucket") 
                        |> range(start: -10m)
                        |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                '''
        query_api = self.client.query_api()

        tables = await query_api.query(query)
        self.assertEqual(2, len(tables))
        self.assertEqual(1, len(tables[0].records))
        self.assertEqual("New York", tables[0].records[0]['location'])
        self.assertEqual(24.3, tables[0].records[0]['_value'])
        self.assertEqual(1, len(tables[1].records))
        self.assertEqual("Prague", tables[1].records[0]['location'])
        self.assertEqual(25.3, tables[1].records[0]['_value'])

    @async_test
    async def test_query_raw(self):
        measurement = generate_name("measurement")
        await self._prepare_data(measurement)
        query = f'''
                    from(bucket:"my-bucket") 
                        |> range(start: -10m)
                        |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                        |> drop(columns: ["_start", "_stop", "_time", "_measurement"])
                '''
        query_api = self.client.query_api()

        raw = await query_api.query_raw(query)
        self.assertEqual(7, len(raw.splitlines()))
        self.assertEqual(',,0,24.3,temperature,New York', raw.splitlines()[4])
        self.assertEqual(',,1,25.3,temperature,Prague', raw.splitlines()[5])

    @async_test
    async def test_query_stream_records(self):
        measurement = generate_name("measurement")
        await self._prepare_data(measurement)
        query = f'''
                    from(bucket:"my-bucket") 
                        |> range(start: -10m)
                        |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                '''
        query_api = self.client.query_api()

        records = []
        async for record in await query_api.query_stream(query):
            records.append(record)

        self.assertEqual(2, len(records))
        self.assertEqual("New York", records[0]['location'])
        self.assertEqual(24.3, records[0]['_value'])
        self.assertEqual("Prague", records[1]['location'])
        self.assertEqual(25.3, records[1]['_value'])

    @async_test
    async def test_query_data_frame(self):
        measurement = generate_name("measurement")
        await self._prepare_data(measurement)
        query = f'''
                    from(bucket:"my-bucket") 
                        |> range(start: -10m)
                        |> filter(fn: (r) => r["_measurement"] == "{measurement}") 
                        |> group()
                '''
        query_api = self.client.query_api()

        dataframe = await query_api.query_data_frame(query)
        self.assertIsNotNone(dataframe)
        self.assertEqual(2, len(dataframe))
        self.assertEqual(24.3, dataframe['_value'][0])
        self.assertEqual(25.3, dataframe['_value'][1])
        self.assertEqual('temperature', dataframe['_field'][0])
        self.assertEqual('temperature', dataframe['_field'][1])
        self.assertEqual(measurement, dataframe['_measurement'][0])
        self.assertEqual(measurement, dataframe['_measurement'][1])
        self.assertEqual('New York', dataframe['location'][0])
        self.assertEqual('Prague', dataframe['location'][1])

    @async_test
    async def test_query_data_frame_with_warning(self):
        measurement = generate_name("measurement")
        await self._prepare_data(measurement)
        query = f'''
                    from(bucket:"my-bucket") 
                        |> range(start: -10m)
                        |> filter(fn: (r) => r["_measurement"] == "{measurement}") 
                '''
        query_api = self.client.query_api()

        with pytest.warns(MissingPivotFunction) as warnings:
            dataframe = await query_api.query_data_frame(query)
            self.assertIsNotNone(dataframe)
        self.assertEqual(1, len(warnings))

    @async_test
    async def test_query_data_frame_without_warning(self):
        measurement = generate_name("measurement")
        await self._prepare_data(measurement)
        query = f'''
                    from(bucket:"my-bucket") 
                        |> range(start: -10m)
                        |> filter(fn: (r) => r["_measurement"] == "{measurement}") 
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                '''
        query_api = self.client.query_api()

        with warnings.catch_warnings(record=True) as warns:
            dataframe = await query_api.query_data_frame(query)
            self.assertIsNotNone(dataframe)
        self.assertEqual(0, len(warns))

    @async_test
    async def test_write_response_type(self):
        measurement = generate_name("measurement")
        point = Point(measurement).tag("location", "Prague").field("temperature", 25.3)
        response = await self.client.write_api().write(bucket="my-bucket", record=point)

        self.assertEqual(True, response)

    @async_test
    async def test_write_empty_data(self):
        measurement = generate_name("measurement")
        point = Point(measurement).tag("location", "Prague")
        response = await self.client.write_api().write(bucket="my-bucket", record=point)

        self.assertEqual(True, response)

    def gen_fractional_utc(self, nano, precision) -> str:
        raw_sec = nano / 1_000_000_000
        if precision == WritePrecision.NS:
            rem = f"{nano % 1_000_000_000}".rjust(9,"0").rstrip("0")
            return (datetime.fromtimestamp(math.floor(raw_sec), tz=timezone.utc)
                    .isoformat()
                    .replace("+00:00", "") + f".{rem}Z")
                    #f".{rem}Z"))
        elif precision == WritePrecision.US:
            # rem = f"{round(nano / 1_000) % 1_000_000}"#.ljust(6,"0")
            return (datetime.fromtimestamp(round(raw_sec,6), tz=timezone.utc)
                    .isoformat()
                    .replace("+00:00","")
                    .strip("0") + "Z"
                    )
        elif precision == WritePrecision.MS:
            #rem = f"{round(nano / 1_000_000) % 1_000}".rjust(3, "0")
            return (datetime.fromtimestamp(round(raw_sec,3), tz=timezone.utc)
                    .isoformat()
                    .replace("+00:00","")
                    .strip("0") + "Z"
                    )
        elif precision == WritePrecision.S:
            return (datetime.fromtimestamp(round(raw_sec), tz=timezone.utc)
                    .isoformat()
                    .replace("+00:00","Z"))
        else:
            raise ValueError(f"Unknown precision: {precision}")


    @async_test
    async def test_write_points_different_precision(self):
        now_ns = time.time_ns()
        now_us = now_ns / 1_000
        now_ms = now_us / 1_000
        now_s = now_ms / 1_000

        now_date_s = self.gen_fractional_utc(now_ns, WritePrecision.S)
        now_date_ms = self.gen_fractional_utc(now_ns, WritePrecision.MS)
        now_date_us = self.gen_fractional_utc(now_ns, WritePrecision.US)
        now_date_ns = self.gen_fractional_utc(now_ns, WritePrecision.NS)

        points = {
            WritePrecision.S: [],
            WritePrecision.MS: [],
            WritePrecision.US: [],
            WritePrecision.NS: []
        }

        expected = {}

        measurement = generate_name("measurement")
        # basic date-time value
        points[WritePrecision.S].append(Point(measurement).tag("method", "SecDateTime").field("temperature", 25.3) \
            .time(datetime.fromtimestamp(round(now_s), tz=timezone.utc), write_precision=WritePrecision.S))
        expected['SecDateTime'] = now_date_s
        points[WritePrecision.MS].append(Point(measurement).tag("method", "MilDateTime").field("temperature", 24.3) \
            .time(datetime.fromtimestamp(round(now_s,3), tz=timezone.utc), write_precision=WritePrecision.MS))
        expected['MilDateTime'] = now_date_ms
        points[WritePrecision.US].append(Point(measurement).tag("method", "MicDateTime").field("temperature", 24.3) \
            .time(datetime.fromtimestamp(round(now_s,6), tz=timezone.utc), write_precision=WritePrecision.US))
        expected['MicDateTime'] = now_date_us
        # N.B. datetime does not handle nanoseconds
#        points[WritePrecision.NS].append(Point(measurement).tag("method", "NanDateTime").field("temperature", 24.3) \
#            .time(datetime.fromtimestamp(now_s, tz=timezone.utc), write_precision=WritePrecision.NS))

        # long timestamps based on POSIX time
        points[WritePrecision.S].append(Point(measurement).tag("method", "SecPosix").field("temperature", 24.3) \
            .time(round(now_s), write_precision=WritePrecision.S))
        expected['SecPosix'] = now_date_s
        points[WritePrecision.MS].append(Point(measurement).tag("method", "MilPosix").field("temperature", 24.3) \
            .time(round(now_ms), write_precision=WritePrecision.MS))
        expected['MilPosix'] = now_date_ms
        points[WritePrecision.US].append(Point(measurement).tag("method", "MicPosix").field("temperature", 24.3) \
            .time(round(now_us), write_precision=WritePrecision.US))
        expected['MicPosix'] = now_date_us
        points[WritePrecision.NS].append(Point(measurement).tag("method", "NanPosix").field("temperature", 24.3) \
            .time(now_ns, write_precision=WritePrecision.NS))
        expected['NanPosix'] = now_date_ns

        # ISO Zulu datetime with ms, us and ns e.g. "2024-09-27T13:17:16.412399728Z"
        points[WritePrecision.S].append(Point(measurement).tag("method", "SecDTZulu").field("temperature", 24.3) \
            .time(now_date_s, write_precision=WritePrecision.S))
        expected['SecDTZulu'] = now_date_s
        points[WritePrecision.MS].append(Point(measurement).tag("method", "MilDTZulu").field("temperature", 24.3) \
            .time(now_date_ms, write_precision=WritePrecision.MS))
        expected['MilDTZulu'] = now_date_ms
        points[WritePrecision.US].append(Point(measurement).tag("method", "MicDTZulu").field("temperature", 24.3) \
            .time(now_date_us, write_precision=WritePrecision.US))
        expected['MicDTZulu'] = now_date_us
        # This keeps resulting in micro second resolution in response
#        points[WritePrecision.NS].append(Point(measurement).tag("method", "NanDTZulu").field("temperature", 24.3) \
#            .time(now_date_ns, write_precision=WritePrecision.NS))

        recs = [x for x in [v for v in points.values()]]

        await self.client.write_api().write(bucket="my-bucket", record=recs,
                                            write_precision=WritePrecision.NS)
        query = f'''
                    from(bucket:"my-bucket") 
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "{measurement}") 
                        |> keep(columns: ["method","_time"])
                '''
        query_api = self.client.query_api()

        # ensure calls fully processed on server
        await asyncio.sleep(1)

        raw = await query_api.query_raw(query)
        linesRaw = raw.splitlines()[4:]

        lines = []
        for lnr in linesRaw:
            lines.append(lnr[2:].split(","))

        def get_time_for_method(lines, method):
            for l in lines:
                if l[2] == method:
                    return l[1]
            return ""

        self.assertEqual(15, len(raw.splitlines()))

        for key in expected:
            t = get_time_for_method(lines,key)
            comp_time = dateutil.parser.isoparse(get_time_for_method(lines,key))
            target_time = dateutil.parser.isoparse(expected[key])
            self.assertEqual(target_time.date(), comp_time.date())
            self.assertEqual(target_time.hour, comp_time.hour)
            self.assertEqual(target_time.second,comp_time.second)
            dif = abs(target_time.microsecond - comp_time.microsecond)
            if key[:3] == "Sec":
                # Already tested
                pass
            elif key[:3] == "Mil":
                # may be slight rounding differences
                self.assertLess(dif, 1500, f"failed to match timestamp for {key} {target_time} != {comp_time}")
            elif key[:3] == "Mic":
                # may be slight rounding differences
                self.assertLess(dif, 150, f"failed to match timestamp for {key} {target_time} != {comp_time}")
            elif key[:3] == "Nan":
                self.assertEqual(expected[key], get_time_for_method(lines, key))
            else:
                raise Exception(f"Unhandled key {key}")

    @async_test
    async def test_delete_api(self):
        measurement = generate_name("measurement")
        await self._prepare_data(measurement)

        successfully = await self.client.delete_api().delete(start=datetime.fromtimestamp(0),
                                                             stop=datetime.now(tz=timezone.utc),
                                                             predicate="location = \"Prague\"", bucket="my-bucket")
        self.assertEqual(True, successfully)
        query = f'''
                        from(bucket:"my-bucket") 
                            |> range(start: -10m)
                            |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                    '''
        query_api = self.client.query_api()
        tables = await query_api.query(query)
        self.assertEqual(1, len(tables))
        self.assertEqual(1, len(tables[0].records))
        self.assertEqual("New York", tables[0].records[0]['location'])
        self.assertEqual(24.3, tables[0].records[0]['_value'])

    @async_test
    async def test_init_from_ini_file(self):
        client_from_config = InfluxDBClientAsync.from_config_file(f'{os.path.dirname(__file__)}/config.ini')
        self.assertEqual("http://localhost:8086", client_from_config.url)
        self.assertEqual("my-org", client_from_config.org)
        self.assertEqual("my-token", client_from_config.token)
        self.assertEqual(6000, client_from_config.api_client.configuration.timeout)
        self.assertEqual(3, len(client_from_config.default_tags))
        self.assertEqual("132-987-655", client_from_config.default_tags["id"])
        self.assertEqual("California Miner", client_from_config.default_tags["customer"])
        self.assertEqual("${env.data_center}", client_from_config.default_tags["data_center"])

        await client_from_config.close()

    @async_test
    async def test_init_from_file_kwargs(self):
        retry = WritesRetry(total=1, retry_interval=2, exponential_base=3)
        client_from_config = InfluxDBClientAsync.from_config_file(f'{os.path.dirname(__file__)}/config.ini',
                                                                  retries=retry)
        self.assertEqual(client_from_config.retries, retry)

        await client_from_config.close()

    @async_test
    async def test_init_from_env(self):
        os.environ["INFLUXDB_V2_URL"] = "http://localhost:8086"
        os.environ["INFLUXDB_V2_ORG"] = "my-org"
        os.environ["INFLUXDB_V2_TOKEN"] = "my-token"
        os.environ["INFLUXDB_V2_TIMEOUT"] = "5500"
        client_from_envs = InfluxDBClientAsync.from_env_properties()
        self.assertEqual("http://localhost:8086", client_from_envs.url)
        self.assertEqual("my-org", client_from_envs.org)
        self.assertEqual("my-token", client_from_envs.token)
        self.assertEqual(5500, client_from_envs.api_client.configuration.timeout)

        await client_from_envs.close()

    @async_test
    async def test_init_from_kwargs(self):
        retry = WritesRetry(total=1, retry_interval=2, exponential_base=3)
        client_from_envs = InfluxDBClientAsync.from_env_properties(retries=retry)

        self.assertEqual(client_from_envs.retries, retry)

        await client_from_envs.close()

    def test_initialize_out_side_async_context(self):
        with pytest.raises(InfluxDBError) as e:
            InfluxDBClientAsync(url="http://localhost:8086", token="my-token", org="my-org")
        self.assertEqual("The async client should be initialised inside async coroutine "
                         "otherwise there can be unexpected behaviour.", e.value.message)

    @async_test
    async def test_username_password_authorization(self):
        await self.client.close()
        self.client = InfluxDBClientAsync(url="http://localhost:8086", username="my-user", password="my-password",
                                          debug=True)
        await self.client.query_api().query("buckets()", "my-org")

    @async_test
    @aioresponses()
    async def test_init_without_token(self, mocked):
        mocked.post('http://localhost/api/v2/query?org=my-org', status=200, body='')
        await self.client.close()
        self.client = InfluxDBClientAsync("http://localhost")
        await self.client.query_api().query("buckets()", "my-org")

    @async_test
    async def test_redacted_auth_header(self):
        await self.client.close()
        self.client = InfluxDBClientAsync(url="http://localhost:8086", token="my-token", org="my-org", debug=True)

        log_stream = StringIO()
        logger = logging.getLogger("influxdb_client.client.http")
        logger.addHandler(logging.StreamHandler(log_stream))

        await self.client.query_api().query("buckets()", "my-org")

        self.assertIn("Authorization: ***", log_stream.getvalue())

    @async_test
    async def test_query_and_debug(self):
        await self.client.close()
        self.client = InfluxDBClientAsync(url="http://localhost:8086", token="my-token", debug=True)
        # Query
        results = await self.client.query_api().query("buckets()", "my-org")
        self.assertIn("my-bucket", list(map(lambda record: record["name"], results[0].records)))
        # Query RAW
        results = await self.client.query_api().query_raw("buckets()", "my-org")
        self.assertIn("my-bucket", results)
        # Bucket API
        buckets_service = BucketsService(api_client=self.client.api_client)
        results = await buckets_service.get_buckets_async()
        self.assertIn("my-bucket", list(map(lambda bucket: bucket.name, results.buckets)))

    @async_test
    @aioresponses()
    async def test_parse_csv_with_new_lines_in_column(self, mocked):
        await self.client.close()
        self.client = InfluxDBClientAsync("http://localhost")
        mocked.post('http://localhost/api/v2/query?org=my-org', status=200, body='''#datatype,string,long,dateTime:RFC3339
#group,false,false,false
#default,_result,,
,result,table,_time
,,0,2022-09-09T10:22:13.744147091Z

#datatype,string,long,string,long,long,long,long,long,long,long,long,long,string,long,long,string
#group,false,false,true,false,false,false,false,false,false,false,false,false,false,false,false,false
#default,_profiler,,,,,,,,,,,,,,,
,result,table,_measurement,TotalDuration,CompileDuration,QueueDuration,PlanDuration,RequeueDuration,ExecuteDuration,Concurrency,MaxAllocated,TotalAllocated,RuntimeErrors,influxdb/scanned-bytes,influxdb/scanned-values,flux/query-plan
,,0,profiler/query,17305459,6292042,116958,0,0,10758125,0,448,0,,0,0,"digraph {
  ""ReadRange4""
  ""keep2""
  ""limit3""

  ""ReadRange4"" -> ""keep2""
  ""keep2"" -> ""limit3""
}

"

#datatype,string,long,string,string,string,long,long,long,long,double
#group,false,false,true,false,false,false,false,false,false,false
#default,_profiler,,,,,,,,,
,result,table,_measurement,Type,Label,Count,MinDuration,MaxDuration,DurationSum,MeanDuration
,,1,profiler/operator,*influxdb.readFilterSource,ReadRange4,1,888209,888209,888209,888209
,,1,profiler/operator,*universe.schemaMutationTransformation,keep2,4,1875,42042,64209,16052.25
,,1,profiler/operator,*universe.limitTransformation,limit3,3,1333,38750,47874,15958''')

        records = []
        await self.client\
            .query_api(QueryOptions(profilers=["operator", "query"],
                                    profiler_callback=lambda record: records.append(record))) \
            .query("buckets()", "my-org")

        self.assertEqual(4, len(records))

    @async_test
    async def test_query_exception_propagation(self):
        await self.client.close()
        self.client = InfluxDBClientAsync(url="http://localhost:8086", token="wrong", org="my-org")

        with pytest.raises(InfluxDBError) as e:
            await self.client.query_api().query("buckets()", "my-org")
        self.assertEqual("unauthorized access", e.value.message)

    @async_test
    async def test_write_exception_propagation(self):
        await self.client.close()
        self.client = InfluxDBClientAsync(url="http://localhost:8086", token="wrong", org="my-org")

        with pytest.raises(InfluxDBError) as e:
            await self.client.write_api().write(bucket="my_bucket",
                                                record="temperature,location=hic cels=")
        self.assertEqual("unauthorized access", e.value.message)
        headers = e.value.headers
        self.assertIsNotNone(headers)
        self.assertIsNotNone(headers.get("Content-Length"))
        self.assertIsNotNone(headers.get("Date"))
        self.assertIsNotNone(headers.get("X-Platform-Error-Code"))
        self.assertIn("application/json", headers.get("Content-Type"))
        self.assertTrue(re.compile("^v.*").match(headers.get("X-Influxdb-Version")))
        self.assertEqual("OSS", headers.get("X-Influxdb-Build"))

    @async_test
    @aioresponses()
    async def test_parse_utf8_two_bytes_character(self, mocked):
        await self.client.close()
        self.client = InfluxDBClientAsync("http://localhost")

        body = '''#group,false,false,false,false,true,true,true
#datatype,string,long,dateTime:RFC3339,string,string,string,string
#default,_result,,,,,,
,result,table,_time,_value,_field,_measurement,type
'''
        for i in range(1000):
            body += f",,0,2022-10-13T12:28:31.{i}Z,ÂÂÂ,value,async,error\n"

        mocked.post('http://localhost/api/v2/query?org=my-org', status=200, body=body)

        data_frame = await self.client.query_api().query_data_frame("from()", "my-org")
        self.assertEqual(1000, len(data_frame))

    @async_test
    async def test_management_apis(self):
        service = OrganizationsService(api_client=self.client.api_client)
        results = await service.get_orgs_async()
        self.assertIsInstance(results, Organizations)
        self.assertIn("my-org", list(map(lambda org: org.name, results.orgs)))

    @async_test
    async def test_trust_env_default(self):
        self.assertFalse(self.client.api_client.rest_client.pool_manager.trust_env)

        await self.client.close()
        self.client = InfluxDBClientAsync(url="http://localhost:8086", token="wrong", org="my-org",
                                          client_session_kwargs={'trust_env': True})
        self.assertTrue(self.client.api_client.rest_client.pool_manager.trust_env)

    async def _prepare_data(self, measurement: str):
        _point1 = Point(measurement).tag("location", "Prague").field("temperature", 25.3)
        _point2 = Point(measurement).tag("location", "New York").field("temperature", 24.3)
        await self.client.write_api().write(bucket="my-bucket", record=[_point1, _point2])
