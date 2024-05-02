import unittest
from datetime import datetime, timedelta, timezone

from influxdb_client.client.util.date_utils_pandas import PandasDateTimeHelper


class PandasDateTimeHelperTest(unittest.TestCase):

    def setUp(self) -> None:
        self.helper = PandasDateTimeHelper()

    def test_parse_date(self):
        date = self.helper.parse_date('2020-08-07T06:21:57.331249158Z')

        self.assertEqual(date.year, 2020)
        self.assertEqual(date.month, 8)
        self.assertEqual(date.day, 7)
        self.assertEqual(date.hour, 6)
        self.assertEqual(date.minute, 21)
        self.assertEqual(date.second, 57)
        self.assertEqual(date.microsecond, 331249)
        self.assertEqual(date.nanosecond, 158)

    def test_to_nanoseconds(self):
        date = self.helper.parse_date('2020-08-07T06:21:57.331249158Z').replace(tzinfo=timezone.utc)
        nanoseconds = self.helper.to_nanoseconds(date - datetime.fromtimestamp(0, tz=timezone.utc))

        self.assertEqual(nanoseconds, 1596781317331249158)

    def test_to_nanoseconds_buildin_timedelta(self):
        nanoseconds = self.helper.to_nanoseconds(timedelta(days=1))

        self.assertEqual(nanoseconds, 86400000000000)
