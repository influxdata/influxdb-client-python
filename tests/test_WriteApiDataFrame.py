import csv
import os
import time
import unittest

from influxdb_client import InfluxDBClient, WriteOptions, WriteApi, Point
from tests.base_test import BaseTest


class DataFrameWriteTest(BaseTest):

    def setUp(self) -> None:
        self.influxDb_client = InfluxDBClient(url="http://localhost:9999", token="my-token")

        self.write_options = WriteOptions(batch_size=10_000, flush_interval=5_000, retry_interval=3_000)
        self._write_client = WriteApi(influxdb_client=self.influxDb_client, write_options=self.write_options)

    def tearDown(self) -> None:
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
