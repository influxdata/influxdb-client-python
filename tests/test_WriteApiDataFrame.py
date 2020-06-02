import csv
import os
import time
import unittest
from datetime import timedelta

from influxdb_client import InfluxDBClient, WriteOptions, WriteApi
from influxdb_client.client.write_api import SYNCHRONOUS
from tests.base_test import BaseTest


class DataFrameWriteTest(BaseTest):

    def setUp(self) -> None:
        super().setUp()
        self.influxDb_client = InfluxDBClient(url="http://localhost:9999", token="my-token", debug=False)

        self.write_options = WriteOptions(batch_size=10_000, flush_interval=5_000, retry_interval=3_000)
        self._write_client = WriteApi(influxdb_client=self.influxDb_client, write_options=self.write_options)

    def tearDown(self) -> None:
        super().tearDown()
        self._write_client.__del__()

    @unittest.skip('Test big file')
    def test_write_data_frame(self):
        import random
        from influxdb_client.extras import pd

        if not os.path.isfile("data_frame_file.csv"):
            with open('data_frame_file.csv', mode='w+') as csv_file:
                _writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                _writer.writerow(['time', 'col1', 'col2', 'col3', 'col4', 'col5', 'col6', 'col7', 'col8'])

                for i in range(1, 1500000):
                    choice = ['test_a', 'test_b', 'test_c']
                    _writer.writerow([i, random.choice(choice), 'test', random.random(), random.random(),
                                      random.random(), random.random(), random.random(), random.random()])

            csv_file.close()

        with open('data_frame_file.csv', mode='rb') as csv_file:

            data_frame = pd.read_csv(csv_file, index_col='time')
            print(data_frame)

            print('Writing...')

            start = time.time()

            self._write_client.write("my-bucket", "my-org", record=data_frame,
                                     data_frame_measurement_name='h2o_feet',
                                     data_frame_tag_columns=['location'])

            self._write_client.__del__()

            print("Time elapsed: ", (time.time() - start))

        csv_file.close()

    def test_write_num_py(self):
        from influxdb_client.extras import pd, np

        bucket = self.create_test_bucket()

        now = pd.Timestamp('2020-04-05 00:00+00:00')

        data_frame = pd.DataFrame(data=[["coyote_creek", np.int64(100.5)], ["coyote_creek", np.int64(200)]],
                                  index=[now + timedelta(hours=1), now + timedelta(hours=2)],
                                  columns=["location", "water_level"])

        write_api = self.client.write_api(write_options=SYNCHRONOUS)
        write_api.write(bucket.name, record=data_frame, data_frame_measurement_name='h2o_feet',
                        data_frame_tag_columns=['location'])

        write_api.__del__()

        result = self.query_api.query(
            "from(bucket:\"" + bucket.name + "\") |> range(start: 1970-01-01T00:00:00.000000001Z)",
            self.my_organization.id)

        self.assertEqual(1, len(result))
        self.assertEqual(2, len(result[0].records))

        self.assertEqual(result[0].records[0].get_value(), 100.0)
        self.assertEqual(result[0].records[1].get_value(), 200.0)

        pass
