# -*- coding: utf-8 -*-

import unittest
from datetime import datetime, timezone

from dateutil import tz

from influxdb_client.client.util.date_utils import DateHelper


class DateHelperTest(unittest.TestCase):

    def test_to_utc(self):
        date = DateHelper().to_utc(datetime(2021, 4, 29, 20, 30, 10, 0))
        self.assertEqual(datetime(2021, 4, 29, 20, 30, 10, 0, timezone.utc), date)

    def test_to_utc_different_timezone(self):
        date = DateHelper(timezone=tz.gettz('ETC/GMT+2')).to_utc(datetime(2021, 4, 29, 20, 30, 10, 0))
        self.assertEqual(datetime(2021, 4, 29, 22, 30, 10, 0, timezone.utc), date)


if __name__ == '__main__':
    unittest.main()
