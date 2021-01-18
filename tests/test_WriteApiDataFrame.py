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
        self.influxDb_client = InfluxDBClient(url="http://localhost:8086", token="my-token", debug=False)

        self.write_options = WriteOptions(batch_size=10_000, flush_interval=5_000, retry_interval=3_000)
        self._write_client = WriteApi(influxdb_client=self.influxDb_client, write_options=self.write_options)

    def tearDown(self) -> None:
        super().tearDown()
        self._write_client.__del__()

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


class DataSerializerTest(unittest.TestCase):
    @unittest.skip('Test big data')
    def test_convert_data_frame(self):
        from influxdb_client.extras import pd, np

        num_rows=1500000
        col_data={
            'time': np.arange(0, num_rows, 1, dtype=int),
            'col1': np.random.choice(['test_a', 'test_b', 'test_c'], size=(num_rows,)),
        }
        for n in range(2, 9):
            col_data[f'col{n}'] = np.random.rand(num_rows)

        data_frame = pd.DataFrame(data=col_data)
        print(data_frame)

        start = time.time()
        data_frame_to_list_of_points(data_frame, PointSettings(),
            data_frame_measurement_name='h2o_feet',
            data_frame_tag_columns=['location'])

        print("Time elapsed: ", (time.time() - start))

    def test_write_nan(self):
        from influxdb_client.extras import pd, np

        now = pd.Timestamp('2020-04-05 00:00+00:00')

        data_frame = pd.DataFrame(data=[
                                        [3.1955, np.nan, 20.514305, np.nan],
                                        [5.7310, np.nan, 23.328710, np.nan],
                                        [np.nan, 3.138664, np.nan, 20.755026],
                                        [5.7310, 5.139563, 23.328710, 19.791240],
                                        [np.nan, np.nan, np.nan, np.nan],
                                  ],
                                  index=[now, now + timedelta(minutes=30), now + timedelta(minutes=60),
                                         now + timedelta(minutes=90), now + timedelta(minutes=120)],
                                  columns=["actual_kw_price", "forecast_kw_price", "actual_general_use",
                                           "forecast_general_use"])

        points = data_frame_to_list_of_points(data_frame=data_frame, point_settings=PointSettings(),
                                              data_frame_measurement_name='measurement')

        self.assertEqual(4, len(points))
        self.assertEqual("measurement actual_general_use=20.514305,actual_kw_price=3.1955 1586044800000000000",
                         points[0])
        self.assertEqual("measurement actual_general_use=23.32871,actual_kw_price=5.731 1586046600000000000",
                         points[1])
        self.assertEqual("measurement forecast_general_use=20.755026,forecast_kw_price=3.138664 1586048400000000000",
                         points[2])
        self.assertEqual("measurement actual_general_use=23.32871,actual_kw_price=5.731,forecast_general_use=19.79124"
                         ",forecast_kw_price=5.139563 1586050200000000000",
                         points[3])

    def test_write_tag_nan(self):
        from influxdb_client.extras import pd, np

        now = pd.Timestamp('2020-04-05 00:00+00:00')

        data_frame = pd.DataFrame(data=[
                                        ["", 3.1955, 20.514305],
                                        ['', 5.7310, 23.328710],
                                        [np.nan, 5.7310, 23.328710],
                                        ["tag", 3.138664, 20.755026],
                                  ],
                                  index=[now, now + timedelta(minutes=30),
                                         now + timedelta(minutes=60), now + timedelta(minutes=90)],
                                  columns=["tag", "actual_kw_price", "forecast_kw_price"])

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

    def test_write_object_field_nan(self):
        from influxdb_client.extras import pd, np

        now = pd.Timestamp('2020-04-05 00:00+00:00')

        data_frame = pd.DataFrame(data=[
                                        ["foo", 1],
                                        [np.nan, 2],
                                  ],
                                  index=[now, now + timedelta(minutes=30)],
                                  columns=["obj", "val"])

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              point_settings=PointSettings(),
                                              data_frame_measurement_name='measurement')

        self.assertEqual(2, len(points))
        self.assertEqual("measurement obj=\"foo\",val=1i 1586044800000000000",
                         points[0])
        self.assertEqual("measurement val=2i 1586046600000000000",
                         points[1])

    def test_write_field_bool(self):
        from influxdb_client.extras import pd, np

        now = pd.Timestamp('2020-04-05 00:00+00:00')

        data_frame = pd.DataFrame(data=[
                                        [True],
                                        [False],
                                  ],
                                  index=[now, now + timedelta(minutes=30)],
                                  columns=["status"])

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              point_settings=PointSettings(),
                                              data_frame_measurement_name='measurement')

        self.assertEqual(2, len(points))
        self.assertEqual("measurement status=True 1586044800000000000",
                         points[0])
        self.assertEqual("measurement status=False 1586046600000000000",
                         points[1])

    def test_escaping_measurement(self):
        from influxdb_client.extras import pd, np

        now = pd.Timestamp('2020-04-05 00:00+00:00')

        data_frame = pd.DataFrame(data=[
                                        ["coyote_creek", np.int64(100.5)],
                                        ["coyote_creek", np.int64(200)],
                                  ],
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

    def test_escape_text_value(self):
        from influxdb_client.extras import pd, np

        now = pd.Timestamp('2020-04-05 00:00+00:00')
        an_hour_ago = now - timedelta(hours=1)

        test = [{'a': an_hour_ago, 'b': 'hello world', 'c': 1, 'd': 'foo bar'},
                {'a': now, 'b': 'goodbye cruel world', 'c': 2, 'd': 'bar foo'}]

        data_frame = pd.DataFrame(test)
        data_frame = data_frame.set_index('a')

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              point_settings=PointSettings(),
                                              data_frame_measurement_name='test',
                                              data_frame_tag_columns=['d'])

        self.assertEqual(2, len(points))
        self.assertEqual("test,d=foo\\ bar b=\"hello world\",c=1i 1586041200000000000", points[0])
        self.assertEqual("test,d=bar\\ foo b=\"goodbye cruel world\",c=2i 1586044800000000000", points[1])

    def test_with_default_tags(self):
        from influxdb_client.extras import pd, np
        now = pd.Timestamp('2020-04-05 00:00+00:00')
        data_frame = pd.DataFrame(data={
                                      'value': [1, 2],
                                      't1': ['a1', 'a2'],
                                      't3': ['c1', 'c2'],
                                },
                                index=[now + timedelta(hours=1), now + timedelta(hours=2)])
        original_data = data_frame.copy()

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              point_settings=PointSettings(t2='every'),
                                              data_frame_measurement_name='h2o',
                                              data_frame_tag_columns={"t1", "t3"})

        self.assertEqual(2, len(points))
        self.assertEqual("h2o,t1=a1,t2=every,t3=c1 value=1i 1586048400000000000", points[0])
        self.assertEqual("h2o,t1=a2,t2=every,t3=c2 value=2i 1586052000000000000", points[1])

        # Check that the data frame hasn't been changed (an earlier version did change it)
        self.assertEqual(True, (data_frame == original_data).all(axis = None), f'data changed; old:\n{original_data}\nnew:\n{data_frame}')

        # Check that the default tags won't override actual column data.
        # This is arguably incorrect behavior, but it's how it works currently.
        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              point_settings=PointSettings(t1='every'),
                                              data_frame_measurement_name='h2o',
                                              data_frame_tag_columns={"t1", "t3"})

        self.assertEqual(2, len(points))
        self.assertEqual("h2o,t1=a1,t3=c1 value=1i 1586048400000000000", points[0])
        self.assertEqual("h2o,t1=a2,t3=c2 value=2i 1586052000000000000", points[1])

        self.assertEqual(True, (data_frame == original_data).all(axis = None), f'data changed; old:\n{original_data}\nnew:\n{data_frame}')

    def test_with_period_index(self):
        from influxdb_client.extras import pd, np
        now = pd.Timestamp('2020-04-05 00:00+00:00')
        data_frame = pd.DataFrame(data={
                                      'value': [1, 2],
                                },
                                index=pd.period_range(start='2020-04-05 01:00+00:00', freq='H', periods=2))

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              point_settings=PointSettings(),
                                              data_frame_measurement_name='h2o')

        self.assertEqual(2, len(points))
        self.assertEqual("h2o value=1i 1586048400000000000", points[0])
        self.assertEqual("h2o value=2i 1586052000000000000", points[1])
