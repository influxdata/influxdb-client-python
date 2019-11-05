import datetime

from influxdb_client.client.flux_table import FluxTable, FluxColumn, FluxRecord
from tests.base_test import BaseTest


class FluxObjectTest(BaseTest):

    def test_create_structure(self):
        _time = datetime.datetime(1970, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)

        table = FluxTable()
        table.columns = [FluxColumn(index=0, label='result', data_type='string', group=False, default_value='_result'),
                         FluxColumn(index=1, label='table', data_type='long', group=False, default_value=''),
                         FluxColumn(index=2, label='_start', data_type='dateTime:RFC3339', group=True,
                                    default_value=''),
                         FluxColumn(index=3, label='_stop', data_type='dateTime:RFC3339', group=True, default_value=''),
                         FluxColumn(index=4, label='_time', data_type='dateTime:RFC3339', group=False,
                                    default_value=''),
                         FluxColumn(index=5, label='_value', data_type='double', group=False, default_value=''),
                         FluxColumn(index=6, label='_field', data_type='string', group=True, default_value=''),
                         FluxColumn(index=7, label='_measurement', data_type='string', group=True, default_value=''),
                         FluxColumn(index=8, label='location', data_type='string', group=True, default_value='')]

        record1 = FluxRecord(table=0)
        record1["table"] = 0
        record1["_start"] = _time
        record1["_stop"] = _time
        record1["_time"] = _time
        record1["_value"] = 1.0
        record1["_field"] = 'water level'
        record1["_measurement"] = 'h2o'
        record1["location"] = 'coyote_creek'

        record2 = FluxRecord(table=0)
        record2["table"] = 0
        record2["_start"] = _time
        record2["_stop"] = _time
        record2["_time"] = _time + datetime.timedelta(days=1)
        record2["_value"] = 2.0
        record2["_field"] = 'water level'
        record2["_measurement"] = 'h2o'
        record2["location"] = 'coyote_creek'

        table.records = [record1, record2]

        self.assertEqual(9, table.columns.__len__())
        self.assertEqual(2, table.records.__len__())

        # record 1
        self.assertEqual(_time, table.records[0].get_start())
        self.assertEqual(_time, table.records[0].get_stop())
        self.assertEqual(_time, table.records[0].get_time())
        self.assertEqual(1.0, table.records[0].get_value())
        self.assertEqual(1.0, table.records[0]["_value"])
        self.assertEqual('water level', table.records[0].get_field())
        self.assertEqual('water level', table.records[0]["_field"])
        self.assertEqual('h2o', table.records[0].get_measurement())
        self.assertEqual('h2o', table.records[0]["_measurement"])
        self.assertEqual('coyote_creek', table.records[0].values['location'])
        self.assertEqual('coyote_creek', table.records[0]['location'])

        # record 2
        self.assertEqual(_time, table.records[1].get_start())
        self.assertEqual(_time, table.records[1].get_stop())
        self.assertEqual(_time + datetime.timedelta(days=1), table.records[1].get_time())
        self.assertEqual(2.0, table.records[1].get_value())
        self.assertEqual(2.0, table.records[1]["_value"])
        self.assertEqual('water level', table.records[1].get_field())
        self.assertEqual('water level', table.records[1]["_field"])
        self.assertEqual('h2o', table.records[1].get_measurement())
        self.assertEqual('h2o', table.records[1]["_measurement"])
        self.assertEqual('coyote_creek', table.records[1].values['location'])
        self.assertEqual('coyote_creek', table.records[1]['location'])
