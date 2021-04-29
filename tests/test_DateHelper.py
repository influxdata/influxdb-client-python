# -*- coding: utf-8 -*-

import unittest
from datetime import datetime, timezone

from pytz import UTC, timezone

from influxdb_client.client.util import date_utils
from influxdb_client.client.util.date_utils import DateHelper, get_date_helper


class DateHelperTest(unittest.TestCase):

    def setUp(self) -> None:
        date_utils.date_helper = DateHelper()

    def test_to_utc(self):
        date = get_date_helper().to_utc(datetime(2021, 4, 29, 20, 30, 10, 0))
        self.assertEqual(datetime(2021, 4, 29, 20, 30, 10, 0, UTC), date)

    def test_to_utc_different_timezone(self):
        date_utils.date_helper = DateHelper(timezone=timezone('ETC/GMT+2'))
        date = get_date_helper().to_utc(datetime(2021, 4, 29, 20, 30, 10, 0))
        self.assertEqual(datetime(2021, 4, 29, 22, 30, 10, 0, UTC), date)


if __name__ == '__main__':
    unittest.main()
