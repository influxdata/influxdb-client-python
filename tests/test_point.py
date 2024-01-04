# -*- coding: utf-8 -*-

import unittest
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import pytest
from dateutil import tz

from influxdb_client import Point, WritePrecision


class PointTest(unittest.TestCase):

    def test_ToStr(self):
        point = Point.measurement("h2o").tag("location", "europe").field("level", 2.2)
        expected_str = point.to_line_protocol()
        self.assertEqual(expected_str, str(point))

    def test_MeasurementEscape(self):
        point = Point.measurement("h2 o").tag("location", "europe").tag("", "warn").field("level", 2)
        self.assertEqual(point.to_line_protocol(), "h2\\ o,location=europe level=2i")

        point = Point.measurement("h2,o").tag("location", "europe").tag("", "warn").field("level", 2)
        self.assertEqual(point.to_line_protocol(), "h2\\,o,location=europe level=2i")
        pass

    def test_TagEmptyKey(self):
        point = Point.measurement("h2o") \
            .tag("location", "europe") \
            .tag("", "warn") \
            .field("level", 2)

        self.assertEqual("h2o,location=europe level=2i", point.to_line_protocol())

    def test_TagEmptyValue(self):
        point = Point.measurement("h2o") \
            .tag("location", "europe") \
            .tag("log", "") \
            .field("level", 2)

        self.assertEqual("h2o,location=europe level=2i", point.to_line_protocol())

    def test_TagEscapingKeyAndValue(self):
        point = Point.measurement("h\n2\ro\t_data") \
            .tag("new\nline", "new\nline") \
            .tag("carriage\rreturn", "carriage\nreturn") \
            .tag("t\tab", "t\tab") \
            .field("level", 2)

        self.assertEqual(
            "h\\n2\\ro\\t_data,carriage\\rreturn=carriage\\nreturn,new\\nline=new\\nline,t\\tab=t\\tab level=2i",
            point.to_line_protocol())

    def test_EqualSignEscaping(self):
        point = Point.measurement("h=2o") \
            .tag("l=ocation", "e=urope") \
            .field("l=evel", 2)

        self.assertEqual("h=2o,l\\=ocation=e\\=urope l\\=evel=2i", point.to_line_protocol())

    def test_OverrideTagField(self):
        point = Point.measurement("h2o") \
            .tag("location", "europe") \
            .tag("location", "europe2") \
            .field("level", 2) \
            .field("level", 3)

        self.assertEqual("h2o,location=europe2 level=3i", point.to_line_protocol())

    def test_FieldTypes(self):
        point = Point.measurement("h2o") \
            .tag("location", "europe") \
            .field("long", 1) \
            .field("double", 250.69) \
            .field("float", 35.0) \
            .field("integer", 7) \
            .field("short", 8) \
            .field("byte", 9) \
            .field("ulong", 10) \
            .field("uint", 11) \
            .field("sbyte", 12) \
            .field("ushort", 13) \
            .field("point", 13.3) \
            .field("decimal", 25.6) \
            .field("decimal-object", Decimal('0.142857')) \
            .field("boolean", False) \
            .field("string", "string value")

        expected = "h2o,location=europe boolean=false,byte=9i,decimal=25.6,decimal-object=0.142857,double=250.69," \
                   "float=35,integer=7i,long=1i,point=13.3,sbyte=12i,short=8i,string=\"string value\"," \
                   "uint=11i,ulong=10i,ushort=13i"

        self.assertEqual(expected, point.to_line_protocol())

    def test_FieldNullValue(self):
        point = Point.measurement("h2o") \
            .tag("location", "europe") \
            .field("level", 2) \
            .field("warning", None)

        self.assertEqual("h2o,location=europe level=2i", point.to_line_protocol())

    def test_FieldEscape(self):
        point = Point.measurement("h2o") \
            .tag("location", "europe") \
            .field("level", "string esc\\ape value")

        self.assertEqual("h2o,location=europe level=\"string esc\\\\ape value\"", point.to_line_protocol())

        point = Point.measurement("h2o") \
            .tag("location", "europe") \
            .field("level", "string esc\"ape value")

        self.assertEqual("h2o,location=europe level=\"string esc\\\"ape value\"", point.to_line_protocol())

    def test_Time(self):
        point = Point.measurement("h2o") \
            .tag("location", "europe") \
            .field("level", 2) \
            .time(123, WritePrecision.S)

        self.assertEqual("h2o,location=europe level=2i 123", point.to_line_protocol())

    def test_TimePrecisionDefault(self):
        point = Point.measurement("h2o") \
            .tag("location", "europe") \
            .field("level", 2)

        self.assertEqual(WritePrecision.NS, point.write_precision)

    def test_TimeSpanFormatting(self):
        point = Point.measurement("h2o") \
            .tag("location", "europe") \
            .field("level", 2) \
            .time(timedelta(days=1), WritePrecision.NS)

        self.assertEqual("h2o,location=europe level=2i 86400000000000", point.to_line_protocol())

        point = Point.measurement("h2o") \
            .tag("location", "europe") \
            .field("level", 2) \
            .time(timedelta(hours=356), WritePrecision.US)

        self.assertEqual("h2o,location=europe level=2i 1281600000000", point.to_line_protocol())

        point = Point.measurement("h2o") \
            .tag("location", "europe") \
            .field("level", 2) \
            .time(timedelta(seconds=156), WritePrecision.MS)

        self.assertEqual("h2o,location=europe level=2i 156000", point.to_line_protocol())

        point = Point.measurement("h2o") \
            .tag("location", "europe") \
            .field("level", 2) \
            .time(timedelta(seconds=123), WritePrecision.S)

        self.assertEqual("h2o,location=europe level=2i 123", point.to_line_protocol())

        point = Point.measurement("h2o") \
            .tag("location", "europe") \
            .field("level", 2) \
            .time(timedelta(microseconds=876), WritePrecision.NS)

        self.assertEqual("h2o,location=europe level=2i 876000", point.to_line_protocol())

        point = Point.measurement("h2o") \
            .tag("location", "europe") \
            .field("level", 2) \
            .time(timedelta(milliseconds=954), WritePrecision.NS)

        self.assertEqual("h2o,location=europe level=2i 954000000", point.to_line_protocol())

    def test_DateTimeFormatting(self):
        date_time = datetime(2015, 10, 15, 8, 20, 15)

        point = Point.measurement("h2o") \
            .tag("location", "europe") \
            .field("level", 2) \
            .time(date_time, WritePrecision.MS)

        self.assertEqual("h2o,location=europe level=2i 1444897215000", point.to_line_protocol())

        date_time = datetime(2015, 10, 15, 8, 20, 15, 750, timezone.utc)

        point = Point.measurement("h2o") \
            .tag("location", "europe") \
            .field("level", False) \
            .time(date_time, WritePrecision.S)

        self.assertEqual("h2o,location=europe level=false 1444897215", point.to_line_protocol())

        point = Point.measurement("h2o") \
            .tag("location", "europe") \
            .field("level", False) \
            .time(date_time, WritePrecision.MS)

        self.assertEqual("h2o,location=europe level=false 1444897215000", point.to_line_protocol())

        point = Point.measurement("h2o") \
            .tag("location", "europe") \
            .field("level", False) \
            .time(date_time, WritePrecision.US)

        self.assertEqual("h2o,location=europe level=false 1444897215000750", point.to_line_protocol())

        point = Point.measurement("h2o") \
            .tag("location", "europe") \
            .field("level", False) \
            .time(date_time, WritePrecision.NS)

        self.assertEqual("h2o,location=europe level=false 1444897215000750000", point.to_line_protocol())

        point = Point.measurement("h2o") \
            .tag("location", "europe") \
            .field("level", True) \
            .time(datetime.now(timezone.utc), WritePrecision.S)

        line_protocol = point.to_line_protocol()
        self.assertTrue("." not in line_protocol)

        point = Point.measurement("h2o") \
            .tag("location", "europe") \
            .field("level", True) \
            .time(datetime.now(timezone.utc), WritePrecision.NS)

        line_protocol = point.to_line_protocol()
        self.assertTrue("." not in line_protocol)

    def test_DateTimeUtc(self):
        date_time = datetime(2015, 10, 15, 8, 20, 15)

        point = Point.measurement("h2o") \
            .tag("location", "europe") \
            .field("level", 2) \
            .time(date_time)

        self.assertEqual("h2o,location=europe level=2i 1444897215000000000", point.to_line_protocol())

    def test_InstantFormatting(self):
        instant = "1970-01-01T00:00:45.808680869Z"

        point = Point.measurement("h2o") \
            .tag("location", "europe") \
            .field("level", 2) \
            .time(instant, WritePrecision.S)

        self.assertEqual("h2o,location=europe level=2i 45", point.to_line_protocol())

    def test_point_protocol(self):
        dt = datetime(year=2009, month=11, day=10, hour=23, minute=0, second=0, microsecond=123456)

        point = Point.measurement('weather').time(dt, WritePrecision.MS) \
            .tag("location", "Přerov") \
            .tag("sid", "12345") \
            .field("temperature", 30.1) \
            .field("int_field", 2) \
            .field("float_field", 0)

        self.assertEqual(point.to_line_protocol(),
                         "weather,location=Přerov,sid=12345 float_field=0i,int_field=2i,temperature=30.1 1257894000123")

        point = Point.measurement('weather').time(dt, WritePrecision.MS) \
            .field("temperature", 30.1) \
            .field("float_field", 0)

        print(point.to_line_protocol())
        self.assertEqual(point.to_line_protocol(), "weather float_field=0i,temperature=30.1 1257894000123")

    def test_lineprotocol_encode(self):
        point = Point.measurement('test')
        point._tags = {
            "empty_tag": "",
            "none_tag": None,
            "backslash_tag": "C:\\\\",
            "integer_tag": 2,
            "string_tag": "hello"
        }
        point._fields = {
            "string_val": "hello!",
            "int_val": 1,
            "float_val": 1.1,
            "none_field": None,
            "bool_val": True,
        }

        self.assertEqual(
            point.to_line_protocol(),
            'test,backslash_tag=C:\\\\ ,integer_tag=2,string_tag=hello '
            'bool_val=true,float_val=1.1,int_val=1i,string_val="hello!"'
        )

    def test_timestamp(self):
        """Test timezone in TestLineProtocol object."""
        dt = datetime(2009, 11, 10, 23, 0, 0, 123456)
        utc = dt.replace(tzinfo=timezone.utc)
        berlin = dt.replace(tzinfo=tz.gettz('Europe/Berlin'))
        eastern = berlin.astimezone(tz.gettz('US/Eastern'))

        exp_utc = 'A val=1i 1257894000123456000'
        exp_est = 'A val=1i 1257890400123456000'

        point = Point.measurement("A").field("val", 1).time(dt)

        self.assertEqual(point.to_line_protocol(), exp_utc)
        self.assertEqual(point.time(utc).to_line_protocol(), exp_utc)
        self.assertEqual(point.time(berlin).to_line_protocol(), exp_est)
        self.assertEqual(point.time(eastern).to_line_protocol(), exp_est)

    def test_infinity_values(self):
        _point = Point.measurement("h2o") \
            .tag("location", "europe") \
            .field("decimal-infinity-positive", Decimal('Infinity')) \
            .field("decimal-infinity-negative", Decimal('-Infinity')) \
            .field("decimal-nan", Decimal('NaN')) \
            .field("flout-infinity-positive", float('inf')) \
            .field("flout-infinity-negative", float('-inf')) \
            .field("flout-nan", float('nan')) \
            .field("level", 2)

        self.assertEqual("h2o,location=europe level=2i", _point.to_line_protocol())

    def test_only_infinity_values(self):
        _point = Point.measurement("h2o") \
            .tag("location", "europe") \
            .field("decimal-infinity-positive", Decimal('Infinity')) \
            .field("decimal-infinity-negative", Decimal('-Infinity')) \
            .field("decimal-nan", Decimal('NaN')) \
            .field("flout-infinity-positive", float('inf')) \
            .field("flout-infinity-negative", float('-inf')) \
            .field("flout-nan", float('nan'))

        self.assertEqual("", _point.to_line_protocol())

    def test_timezone(self):
        """Test timezone in TestLineProtocol object."""
        dt = datetime(2009, 11, 10, 23, 0, 0, 123456)
        utc = dt.replace(tzinfo=timezone.utc)
        berlin = dt.replace(tzinfo=tz.gettz('Europe/Berlin'))
        eastern = berlin.astimezone(tz.gettz('US/Eastern'))

        self.assertEqual("h2o val=1i 0", Point.measurement("h2o").field("val", 1).time(0).to_line_protocol())
        self.assertEqual("h2o val=1i 1257894000123456000", Point.measurement("h2o").field("val", 1).time(
            "2009-11-10T23:00:00.123456Z").to_line_protocol())
        self.assertEqual("h2o val=1i 1257894000123456000",
                         Point.measurement("h2o").field("val", 1).time(utc).to_line_protocol())
        self.assertEqual("h2o val=1i 1257894000123456000",
                         Point.measurement("h2o").field("val", 1).time(dt).to_line_protocol())
        self.assertEqual("h2o val=1i 1257894000123456000",
                         Point.measurement("h2o").field("val", 1).time(1257894000123456000,
                                                                       write_precision=WritePrecision.NS).to_line_protocol())
        self.assertEqual("h2o val=1i 1257890400123456000",
                         Point.measurement("h2o").field("val", 1).time(eastern).to_line_protocol())
        self.assertEqual("h2o val=1i 1257890400123456000",
                         Point.measurement("h2o").field("val", 1).time(berlin).to_line_protocol())

    def test_from_dict_without_timestamp(self):
        json = {"measurement": "my-org", "tags": {"tag1": "tag1", "tag2": "tag2"}, "fields": {'field1': 1, "field2": 2}}

        point = Point.from_dict(json)
        self.assertEqual("my-org,tag1=tag1,tag2=tag2 field1=1i,field2=2i", point.to_line_protocol())

    def test_from_dict_without_tags(self):
        json = {"measurement": "my-org", "fields": {'field1': 1, "field2": 2}}

        point = Point.from_dict(json)
        self.assertEqual("my-org field1=1i,field2=2i", point.to_line_protocol())

    def test_points_from_different_timezones(self):
        time_in_utc = datetime(2020, 7, 4, 0, 0, 0, 123456).replace(tzinfo=timezone.utc)
        time_in_hk = datetime(2020, 7, 4, 8, 0, 0, 123456).replace(tzinfo=tz.gettz('Asia/Hong_Kong'))  # +08:00

        point_utc = Point.measurement("h2o").field("val", 1).time(time_in_utc)
        point_hk = Point.measurement("h2o").field("val", 1).time(time_in_hk)
        self.assertEqual(point_utc.to_line_protocol(), point_hk.to_line_protocol())

    def test_unsupported_field_type(self):
        with self.assertRaises(ValueError) as ve:
            Point.measurement("h2o") \
                .tag("location", "europe") \
                .field("level", timezone.utc) \
                .to_line_protocol()
        exception = ve.exception

        self.assertEqual('Type: "<class \'datetime.timezone\'>" of field: "level" is not supported.', f'{exception}')

    def test_backslash(self):
        point = Point.from_dict({"measurement": "test",
                                 "tags": {"tag1": "value1", "tag2": "value\2", "tag3": "value\\3",
                                          "tag4": r"value\4", "tag5": r"value\\5"}, "time": 1624989000000000000,
                                 "fields": {"value": 10}}, write_precision=WritePrecision.NS)
        self.assertEqual(
            "test,tag1=value1,tag2=value\2,tag3=value\\3,tag4=value\\4,tag5=value\\\\5 value=10i 1624989000000000000",
            point.to_line_protocol())

    def test_numpy_types(self):
        from influxdb_client.extras import np

        point = Point.measurement("h2o") \
            .tag("location", "europe") \
            .field("np.float2", np.float16(2.123)) \
            .field("np.float3", np.float32(3.123)) \
            .field("np.float4", np.float64(4.123)) \
            .field("np.int1", np.int8(1)) \
            .field("np.int2", np.int16(2)) \
            .field("np.int3", np.int32(3)) \
            .field("np.int4", np.int64(4)) \
            .field("np.uint1", np.uint8(5)) \
            .field("np.uint2", np.uint16(6)) \
            .field("np.uint3", np.uint32(7)) \
            .field("np.uint4", np.uint64(8))

        self.assertEqual(
            "h2o,location=europe np.float2=2.123,np.float3=3.123,np.float4=4.123,np.int1=1i,np.int2=2i,np.int3=3i,np.int4=4i,np.uint1=5i,np.uint2=6i,np.uint3=7i,np.uint4=8i",
            point.to_line_protocol())

    def test_from_dictionary_custom_measurement(self):
        dictionary = {
            "name": "test",
            "tags": {"tag": "a"},
            "fields": {"value": 1},
            "time": 1,
        }
        point = Point.from_dict(dictionary, record_measurement_key="name")
        self.assertEqual("test,tag=a value=1i 1", point.to_line_protocol())

    def test_from_dictionary_custom_time(self):
        dictionary = {
            "name": "test",
            "tags": {"tag": "a"},
            "fields": {"value": 1},
            "created": 100250,
        }
        point = Point.from_dict(dictionary,
                                record_measurement_key="name",
                                record_time_key="created")
        self.assertEqual("test,tag=a value=1i 100250", point.to_line_protocol())

    def test_from_dictionary_custom_tags(self):
        dictionary = {
            "name": "test",
            "tag_a": "a",
            "tag_b": "b",
            "fields": {"value": 1},
            "time": 1,
        }
        point = Point.from_dict(dictionary,
                                record_measurement_key="name",
                                record_tag_keys=["tag_a", "tag_b"])
        self.assertEqual("test,tag_a=a,tag_b=b value=1i 1", point.to_line_protocol())

    def test_from_dictionary_custom_fields(self):
        dictionary = {
            "name": "sensor_pt859",
            "location": "warehouse_125",
            "version": "2021.06.05.5874",
            "pressure": 125,
            "temperature": 10,
            "time": 1632208639,
        }
        point = Point.from_dict(dictionary,
                                write_precision=WritePrecision.S,
                                record_measurement_key="name",
                                record_tag_keys=["location", "version"],
                                record_field_keys=["pressure", "temperature"])
        self.assertEqual(
            "sensor_pt859,location=warehouse_125,version=2021.06.05.5874 pressure=125i,temperature=10i 1632208639",
            point.to_line_protocol())

    def test_from_dictionary_tolerant_to_missing_tags_and_fields(self):
        dictionary = {
            "name": "sensor_pt859",
            "location": "warehouse_125",
            "pressure": 125
        }
        point = Point.from_dict(dictionary,
                                write_precision=WritePrecision.S,
                                record_measurement_key="name",
                                record_tag_keys=["location", "version"],
                                record_field_keys=["pressure", "temperature"])
        self.assertEqual("sensor_pt859,location=warehouse_125 pressure=125i", point.to_line_protocol())

    def test_from_dictionary_uint(self):
        dict_structure = {
            "measurement": "h2o_feet",
            "tags": {"location": "coyote_creek"},
            "fields": {
                "water_level": 1.0,
                "some_counter": 108913123234
            },
            "time": 1
        }
        point = Point.from_dict(dict_structure, field_types={"some_counter": "uint"})
        self.assertEqual("h2o_feet,location=coyote_creek some_counter=108913123234u,water_level=1 1",
                         point.to_line_protocol())

    def test_from_dictionary_int(self):
        dict_structure = {
            "measurement": "h2o_feet",
            "tags": {"location": "coyote_creek"},
            "fields": {
                "water_level": 1.0,
                "some_counter": 108913123234
            },
            "time": 1
        }
        point = Point.from_dict(dict_structure, field_types={"some_counter": "int"})
        self.assertEqual("h2o_feet,location=coyote_creek some_counter=108913123234i,water_level=1 1",
                         point.to_line_protocol())

    def test_from_dictionary_float(self):
        dict_structure = {
            "measurement": "h2o_feet",
            "tags": {"location": "coyote_creek"},
            "fields": {
                "water_level": 1.0,
                "some_counter": 108913123234
            },
            "time": 1
        }
        point = Point.from_dict(dict_structure, field_types={"some_counter": "float"})
        self.assertEqual("h2o_feet,location=coyote_creek some_counter=108913123234,water_level=1 1",
                         point.to_line_protocol())

    def test_from_dictionary_float_from_dict(self):
        dict_structure = {
            "measurement": "h2o_feet",
            "tags": {"location": "coyote_creek"},
            "fields": {
                "water_level": 1.0,
                "some_counter": 108913123234
            },
            "field_types": {"some_counter": "float"},
            "time": 1
        }
        point = Point.from_dict(dict_structure)
        self.assertEqual("h2o_feet,location=coyote_creek some_counter=108913123234,water_level=1 1",
                         point.to_line_protocol())

    def test_static_measurement_name(self):
        dictionary = {
            "name": "sensor_pt859",
            "location": "warehouse_125",
            "pressure": 125
        }
        point = Point.from_dict(dictionary,
                                write_precision=WritePrecision.S,
                                record_measurement_name="custom_sensor_id",
                                record_tag_keys=["location", "version"],
                                record_field_keys=["pressure", "temperature"])
        self.assertEqual("custom_sensor_id,location=warehouse_125 pressure=125i", point.to_line_protocol())

    def test_name_start_with_hash(self):
        point = Point.measurement("#hash_start").tag("location", "europe").field("level", 2.2)
        with pytest.warns(SyntaxWarning) as warnings:
            self.assertEqual('#hash_start,location=europe level=2.2', point.to_line_protocol())
        self.assertEqual(1, len(warnings))

    def test_equality_from_dict(self):
        point_dict = {
            "measurement": "h2o_feet",
            "tags": {"location": "coyote_creek"},
            "fields": {
                "water_level": 1.0,
                "some_counter": 108913123234
            },
            "field_types": {"some_counter": "float"},
            "time": 1
        }
        point_a = Point.from_dict(point_dict)
        point_b = Point.from_dict(point_dict)
        self.assertEqual(point_a, point_b)

    def test_equality(self):
        # https://github.com/influxdata/influxdb-client-python/issues/623#issue-2048573579
        point_a = (
            Point("asd")
            .tag("foo", "bar")
            .field("value", 123.45)
            .time(datetime(2023, 12, 19, 13, 27, 42, 215000, tzinfo=timezone.utc))
        )

        point_b = (
            Point("asd")
            .tag("foo", "bar")
            .field("value", 123.45)
            .time(datetime(2023, 12, 19, 13, 27, 42, 215000, tzinfo=timezone.utc))
        )
        self.assertEqual(point_a, point_b)

    def test_not_equal_if_tags_differ(self):
        point_a = (
            Point("asd")
            .tag("foo", "bar")
            .field("value", 123.45)
            .time(datetime(2023, 12, 19, 13, 27, 42, 215000, tzinfo=timezone.utc))
        )

        point_b = (
            Point("asd")
            .tag("foo", "baz")  # not "bar"
            .field("value", 123.45)
            .time(datetime(2023, 12, 19, 13, 27, 42, 215000, tzinfo=timezone.utc))
        )
        self.assertNotEqual(point_a, point_b)

    def test_not_equal_if_fields_differ(self):
        point_a = (
            Point("asd")
            .tag("foo", "bar")
            .field("value", 123.45)
            .time(datetime(2023, 12, 19, 13, 27, 42, 215000, tzinfo=timezone.utc))
        )

        point_b = (
            Point("asd")
            .tag("foo", "bar")
            .field("value", 678.90)  # not 123.45
            .time(datetime(2023, 12, 19, 13, 27, 42, 215000, tzinfo=timezone.utc))
        )
        self.assertNotEqual(point_a, point_b)

    def test_not_equal_if_measurements_differ(self):
        point_a = (
            Point("asd")
            .tag("foo", "bar")
            .field("value", 123.45)
            .time(datetime(2023, 12, 19, 13, 27, 42, 215000, tzinfo=timezone.utc))
        )

        point_b = (
            Point("fgh")  # not "asd"
            .tag("foo", "bar")
            .field("value", 123.45)
            .time(datetime(2023, 12, 19, 13, 27, 42, 215000, tzinfo=timezone.utc))
        )
        self.assertNotEqual(point_a, point_b)

    def test_not_equal_if_times_differ(self):
        point_a = (
            Point("asd")
            .tag("foo", "bar")
            .field("value", 123.45)
            .time(datetime(2023, 12, 19, 13, 27, 42, 215000, tzinfo=timezone.utc))
        )

        point_b = (
            Point("asd")
            .tag("foo", "bar")
            .field("value", 123.45)
            .time(datetime(2024, 12, 19, 13, 27, 42, 215000, tzinfo=timezone.utc))
        )
        self.assertNotEqual(point_a, point_b)
    def test_not_equal_if_other_is_no_point(self):
        point_a = (
            Point("asd")
            .tag("foo", "bar")
            .field("value", 123.45)
            .time(datetime(2023, 12, 19, 13, 27, 42, 215000, tzinfo=timezone.utc))
        )
        not_a_point = "not a point but a string"
        self.assertNotEqual(point_a, not_a_point)

if __name__ == '__main__':
    unittest.main()
