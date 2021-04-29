# -*- coding: utf-8 -*-

import unittest
from datetime import datetime, timezone

from pytz import UTC, timezone

from influxdb_client.client.util.date_utils import DateHelper


class DateHelperTest(unittest.TestCase):

    def test_to_utc(self):
        date = DateHelper().to_utc(datetime(2021, 4, 29, 20, 30, 10, 0))
        self.assertEqual(datetime(2021, 4, 29, 20, 30, 10, 0, UTC), date)

    def test_to_utc_different_timezone(self):
        date = DateHelper(timezone=timezone('ETC/GMT+2')).to_utc(datetime(2021, 4, 29, 20, 30, 10, 0))
        self.assertEqual(datetime(2021, 4, 29, 22, 30, 10, 0, UTC), date)


if __name__ == '__main__':
    unittest.main()
