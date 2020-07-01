import csv
import os
import time
import unittest
from datetime import timedelta

from influxdb_client import InfluxDBClient, WriteOptions, WriteApi
from influxdb_client.client.write.dataframe_serializer import data_frame_to_list_of_points
from influxdb_client.client.write_api import SYNCHRONOUS, PointSettings
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

    def test_write_nan(self):
        from influxdb_client.extras import pd, np

        now = pd.Timestamp('2020-04-05 00:00+00:00')

        data_frame = pd.DataFrame(data=[[3.1955, np.nan, 20.514305, np.nan],
                                        [5.7310, np.nan, 23.328710, np.nan],
                                        [np.nan, 3.138664, np.nan, 20.755026],
                                        [5.7310, 5.139563, 23.328710, 19.791240]],
                                  index=[now, now + timedelta(minutes=30), now + timedelta(minutes=60),
                                         now + timedelta(minutes=90)],
                                  columns=["actual_kw_price", "forecast_kw_price", "actual_general_use",
                                           "forecast_general_use"])

        points = data_frame_to_list_of_points(data_frame=data_frame, point_settings=PointSettings(),
                                              data_frame_measurement_name='measurement')

        self.assertEqual(4, len(points))
        self.assertEqual("measurement actual_kw_price=3.1955,actual_general_use=20.514305 1586044800000000000",
                         points[0])
        self.assertEqual("measurement actual_kw_price=5.731,actual_general_use=23.32871 1586046600000000000",
                         points[1])
        self.assertEqual("measurement forecast_kw_price=3.138664,forecast_general_use=20.755026 1586048400000000000",
                         points[2])
        self.assertEqual("measurement actual_kw_price=5.731,forecast_kw_price=5.139563,actual_general_use=23.32871,"
                         "forecast_general_use=19.79124 1586050200000000000",
                         points[3])

    def test_write_tag_nan(self):
        from influxdb_client.extras import pd, np

        now = pd.Timestamp('2020-04-05 00:00+00:00')

        data_frame = pd.DataFrame(data=[["", 3.1955, 20.514305],
                                        ['', 5.7310, 23.328710],
                                        [np.nan, 5.7310, 23.328710],
                                        ["tag", 3.138664, 20.755026]],
                                  index=[now, now + timedelta(minutes=30),
                                         now + timedelta(minutes=60), now + timedelta(minutes=90)],
                                  columns=["tag", "actual_kw_price", "forecast_kw_price"])

        write_api = self.client.write_api(write_options=SYNCHRONOUS, point_settings=PointSettings())

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              point_settings=PointSettings(),
                                              data_frame_measurement_name='measurement',
                                              data_frame_tag_columns={"tag"})

        self.assertEqual(4, len(points))
        self.assertEqual("measurement actual_kw_price=3.1955,forecast_kw_price=20.514305 1586044800000000000",
                         points[0])
        self.assertEqual("measurement actual_kw_price=5.731,forecast_kw_price=23.32871 1586046600000000000",
                         points[1])
        self.assertEqual("measurement actual_kw_price=5.731,forecast_kw_price=23.32871 1586048400000000000",
                         points[2])
        self.assertEqual("measurement,tag=tag actual_kw_price=3.138664,forecast_kw_price=20.755026 1586050200000000000",
                         points[3])

        write_api.__del__()

    def test_escaping_measurement(self):
        from influxdb_client.extras import pd, np

        now = pd.Timestamp('2020-04-05 00:00+00:00')

        data_frame = pd.DataFrame(data=[["coyote_creek", np.int64(100.5)], ["coyote_creek", np.int64(200)]],
                                  index=[now + timedelta(hours=1), now + timedelta(hours=2)],
                                  columns=["location", "water_level"])

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              point_settings=PointSettings(),
                                              data_frame_measurement_name='measu rement',
                                              data_frame_tag_columns={"tag"})

        self.assertEqual(2, len(points))
        self.assertEqual("measu\\ rement location=\"coyote_creek\",water_level=100i 1586048400000000000",
                         points[0])
        self.assertEqual("measu\\ rement location=\"coyote_creek\",water_level=200i 1586052000000000000",
                         points[1])

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              point_settings=PointSettings(),
                                              data_frame_measurement_name='measu\nrement2',
                                              data_frame_tag_columns={"tag"})

        self.assertEqual(2, len(points))
        self.assertEqual("measu\\nrement2 location=\"coyote_creek\",water_level=100i 1586048400000000000",
                         points[0])
        self.assertEqual("measu\\nrement2 location=\"coyote_creek\",water_level=200i 1586052000000000000",
                         points[1])

    def test_tag_escaping_key_and_value(self):
        from influxdb_client.extras import pd, np

        now = pd.Timestamp('2020-04-05 00:00+00:00')

        data_frame = pd.DataFrame(data=[["carriage\nreturn", "new\nline", "t\tab", np.int64(2)], ],
                                  index=[now + timedelta(hours=1), ],
                                  columns=["carriage\rreturn", "new\nline", "t\tab", "l\ne\rv\tel"])

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              point_settings=PointSettings(),
                                              data_frame_measurement_name='h\n2\ro\t_data',
                                              data_frame_tag_columns={"new\nline", "carriage\rreturn", "t\tab"})

        self.assertEqual(1, len(points))
        self.assertEqual(
            "h\\n2\\ro\\t_data,carriage\\rreturn=carriage\\nreturn,new\\nline=new\\nline,t\\tab=t\\tab l\\ne\\rv\\tel=2i 1586048400000000000",
            points[0])

    def test_tags_order(self):
        from influxdb_client.extras import pd, np

        now = pd.Timestamp('2020-04-05 00:00+00:00')

        data_frame = pd.DataFrame(data=[["c", "a", "b", np.int64(2)], ],
                                  index=[now + timedelta(hours=1), ],
                                  columns=["c", "a", "b", "level"])

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              point_settings=PointSettings(),
                                              data_frame_measurement_name='h2o',
                                              data_frame_tag_columns={"c", "a", "b"})

        self.assertEqual(1, len(points))
        self.assertEqual("h2o,a=a,b=b,c=c level=2i 1586048400000000000", points[0])
