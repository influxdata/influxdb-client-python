# -*- coding: utf-8 -*-

import unittest
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from pytz import UTC, timezone

from influxdb_client import Point, WritePrecision


class PointTest(unittest.TestCase):

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

        self.assertEqual("h\\n2\\ro\\t_data,carriage\\rreturn=carriage\\nreturn,new\\nline=new\\nline,t\\tab=t\\tab level=2i", point.to_line_protocol())

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
                   "float=35.0,integer=7i,long=1i,point=13.3,sbyte=12i,short=8i,string=\"string value\"," \
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

        date_time = datetime(2015, 10, 15, 8, 20, 15, 750, UTC)

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
            .time(datetime.now(UTC), WritePrecision.S)

        line_protocol = point.to_line_protocol()
        self.assertTrue("." not in line_protocol)

        point = Point.measurement("h2o") \
            .tag("location", "europe") \
            .field("level", True) \
            .time(datetime.now(UTC), WritePrecision.NS)

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
        instant = "1970-01-01T00:00:45.999999999Z"

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
            "backslash_tag": "C:\\",
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
        utc = UTC.localize(dt)
        berlin = timezone('Europe/Berlin').localize(dt)
        eastern = berlin.astimezone(timezone('US/Eastern'))

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
        utc = UTC.localize(dt)
        berlin = timezone('Europe/Berlin').localize(dt)
        eastern = berlin.astimezone(timezone('US/Eastern'))

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
        time_in_utc = UTC.localize(datetime(2020, 7, 4, 0, 0, 0, 123456))
        time_in_hk = timezone('Asia/Hong_Kong').localize(datetime(2020, 7, 4, 8, 0, 0, 123456))  # +08:00

        point_utc = Point.measurement("h2o").field("val", 1).time(time_in_utc)
        point_hk = Point.measurement("h2o").field("val", 1).time(time_in_hk)
        self.assertEqual(point_utc.to_line_protocol(), point_hk.to_line_protocol())


if __name__ == '__main__':
    unittest.main()
