import time
import unittest
from datetime import timedelta
from io import StringIO

from influxdb_client import InfluxDBClient, WriteOptions, WriteApi, WritePrecision
from influxdb_client.client.write.dataframe_serializer import data_frame_to_list_of_points, DataframeSerializer
from influxdb_client.client.write.point import DEFAULT_WRITE_PRECISION
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
        self._write_client.close()

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

        write_api.close()

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

        num_rows = 1500000
        col_data = {
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

    def test_write_missing_values(self):
        from influxdb_client.extras import pd

        data_frame = pd.DataFrame({
            "a_bool": [True, None, False],
            "b_int": [None, 1, 2],
            "c_float": [1.0, 2.0, None],
            "d_str": ["a", "b", None],
        })

        data_frame['a_bool'] = data_frame['a_bool'].astype(pd.BooleanDtype())
        data_frame['b_int'] = data_frame['b_int'].astype(pd.Int64Dtype())
        data_frame['c_float'] = data_frame['c_float'].astype(pd.Float64Dtype())
        data_frame['d_str'] = data_frame['d_str'].astype(pd.StringDtype())

        print(data_frame)
        points = data_frame_to_list_of_points(
            data_frame=data_frame,
            point_settings=PointSettings(),
            data_frame_measurement_name='measurement')

        self.assertEqual(3, len(points))
        self.assertEqual("measurement a_bool=True,c_float=1.0,d_str=\"a\" 0", points[0])
        self.assertEqual("measurement b_int=1i,c_float=2.0,d_str=\"b\" 1", points[1])
        self.assertEqual("measurement a_bool=False,b_int=2i 2", points[2])

    def test_write_field_bool(self):
        from influxdb_client.extras import pd

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
        from influxdb_client.extras import pd

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
        from influxdb_client.extras import pd
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
        self.assertEqual(True, (data_frame == original_data).all(axis=None), f'data changed; old:\n{original_data}\nnew:\n{data_frame}')

        # Check that the default tags won't override actual column data.
        # This is arguably incorrect behavior, but it's how it works currently.
        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              point_settings=PointSettings(t1='every'),
                                              data_frame_measurement_name='h2o',
                                              data_frame_tag_columns={"t1", "t3"})

        self.assertEqual(2, len(points))
        self.assertEqual("h2o,t1=a1,t3=c1 value=1i 1586048400000000000", points[0])
        self.assertEqual("h2o,t1=a2,t3=c2 value=2i 1586052000000000000", points[1])

        self.assertEqual(True, (data_frame == original_data).all(axis=None), f'data changed; old:\n{original_data}\nnew:\n{data_frame}')

    def test_with_period_index(self):
        from influxdb_client.extras import pd
        data_frame = pd.DataFrame(data={
                                      'value': [1, 2],
                                },
                                index=pd.period_range(start='2020-04-05 01:00', freq='h', periods=2))

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              point_settings=PointSettings(),
                                              data_frame_measurement_name='h2o')

        self.assertEqual(2, len(points))
        self.assertEqual("h2o value=1i 1586048400000000000", points[0])
        self.assertEqual("h2o value=2i 1586052000000000000", points[1])

    def test_write_num_py_floats(self):
        from influxdb_client.extras import pd, np
        now = pd.Timestamp('2020-04-05 00:00+00:00')

        float_types = [np.float16, np.float32, np.float64]
        if hasattr(np, 'float128'):
            float_types.append(np.float128)
        for np_float_type in float_types:
            data_frame = pd.DataFrame([15.5], index=[now], columns=['level']).astype(np_float_type)
            points = data_frame_to_list_of_points(data_frame=data_frame,
                                                  data_frame_measurement_name='h2o',
                                                  point_settings=PointSettings())
            self.assertEqual(1, len(points))
            self.assertEqual("h2o level=15.5 1586044800000000000", points[0], msg=f'Current type: {np_float_type}')

    def test_write_precision(self):
        from influxdb_client.extras import pd
        now = pd.Timestamp('2020-04-05 00:00+00:00')
        precisions = [
            (WritePrecision.NS, 1586044800000000000),
            (WritePrecision.US, 1586044800000000),
            (WritePrecision.MS, 1586044800000),
            (WritePrecision.S, 1586044800),
            (None, 1586044800000000000)
        ]

        for precision in precisions:
            data_frame = pd.DataFrame([15], index=[now], columns=['level'])
            points = data_frame_to_list_of_points(data_frame=data_frame,
                                                  data_frame_measurement_name='h2o',
                                                  point_settings=PointSettings(),
                                                  precision=precision[0])
            self.assertEqual(1, len(points))
            self.assertEqual(f"h2o level=15i {precision[1]}", points[0])

    def test_index_not_periodIndex_respect_write_precision(self):
        from influxdb_client.extras import pd

        precisions = [
            (WritePrecision.NS, 1586044800000000000),
            (WritePrecision.US, 1586044800000000),
            (WritePrecision.MS, 1586044800000),
            (WritePrecision.S, 1586044800),
            (None, 1586044800000000000)
        ]

        for precision in precisions:
            data_frame = pd.DataFrame([15], index=[precision[1]], columns=['level'])
            points = data_frame_to_list_of_points(data_frame=data_frame,
                                                  data_frame_measurement_name='h2o',
                                                  point_settings=PointSettings(),
                                                  precision=precision[0])
            self.assertEqual(1, len(points))
            self.assertEqual(f"h2o level=15i {precision[1]}", points[0])

    def test_serialize_strings_with_commas(self):
        from influxdb_client.extras import pd

        csv = StringIO("""sep=;
Date;Entry Type;Value;Currencs;Category;Person;Account;Counter Account;Group;Note;Recurring;
"01.10.2018";"Expense";"-1,00";"EUR";"Testcategory";"";"Testaccount";"";"";"This, works";"no";
"02.10.2018";"Expense";"-1,00";"EUR";"Testcategory";"";"Testaccount";"";"";"This , works not";"no";
""")
        data_frame = pd.read_csv(csv, sep=";", skiprows=1, decimal=",", encoding="utf-8")
        data_frame['Date'] = pd.to_datetime(data_frame['Date'], format="%d.%m.%Y")
        data_frame.set_index('Date', inplace=True)

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              data_frame_measurement_name="bookings",
                                              data_frame_tag_columns=['Entry Type', 'Category', 'Person', 'Account'],
                                              point_settings=PointSettings())

        self.assertEqual(2, len(points))
        self.assertEqual("bookings,Account=Testaccount,Category=Testcategory,Entry\\ Type=Expense Currencs=\"EUR\",Note=\"This, works\",Recurring=\"no\",Value=-1.0 1538352000000000000", points[0])
        self.assertEqual("bookings,Account=Testaccount,Category=Testcategory,Entry\\ Type=Expense Currencs=\"EUR\",Note=\"This , works not\",Recurring=\"no\",Value=-1.0 1538438400000000000", points[1])

    def test_without_tags_and_fields_with_nan(self):
        from influxdb_client.extras import pd, np

        df = pd.DataFrame({
            'a': np.arange(0., 3.),
            'b': [0., np.nan, 1.],
        }).set_index(pd.to_datetime(['2021-01-01 0:00', '2021-01-01 0:01', '2021-01-01 0:02']))

        points = data_frame_to_list_of_points(data_frame=df,
                                              data_frame_measurement_name="test",
                                              point_settings=PointSettings())

        self.assertEqual(3, len(points))
        self.assertEqual("test a=0.0,b=0.0 1609459200000000000", points[0])
        self.assertEqual("test a=1.0 1609459260000000000", points[1])
        self.assertEqual("test a=2.0,b=1.0 1609459320000000000", points[2])

    def test_use_timestamp_from_specified_column(self):
        from influxdb_client.extras import pd
        data_frame = pd.DataFrame(data={
            'column_time': ['2020-04-05', '2020-05-05'],
            'value1': [10, 20],
            'value2': [30, 40],
        }, index=['A', 'B'])

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              data_frame_measurement_name="test",
                                              data_frame_timestamp_column="column_time",
                                              point_settings=PointSettings())

        self.assertEqual(2, len(points))
        self.assertEqual('test value1=10i,value2=30i 1586044800000000000', points[0])
        self.assertEqual('test value1=20i,value2=40i 1588636800000000000', points[1])

    def test_str_format_for_timestamp(self):
        from influxdb_client.extras import pd

        time_formats = [
            ('2018-10-26', 'test value1=10i,value2=20i 1540512000000000000'),
            ('2018-10-26 10:00', 'test value1=10i,value2=20i 1540548000000000000'),
            ('2018-10-26 10:00:00-05:00', 'test value1=10i,value2=20i 1540566000000000000'),
            ('2018-10-26T11:00:00+00:00', 'test value1=10i,value2=20i 1540551600000000000'),
            ('2018-10-26 12:00:00+00:00', 'test value1=10i,value2=20i 1540555200000000000'),
            ('2018-10-26T16:00:00-01:00', 'test value1=10i,value2=20i 1540573200000000000'),
        ]

        for time_format in time_formats:
            data_frame = pd.DataFrame(data={
                'column_time': [time_format[0]],
                'value1': [10],
                'value2': [20],
            }, index=['A'])
            points = data_frame_to_list_of_points(data_frame=data_frame,
                                                  data_frame_measurement_name="test",
                                                  data_frame_timestamp_column="column_time",
                                                  point_settings=PointSettings())

            self.assertEqual(1, len(points))
            self.assertEqual(time_format[1], points[0])

    def test_specify_timezone(self):
        from influxdb_client.extras import pd
        data_frame = pd.DataFrame(data={
            'column_time': ['2020-05-24 10:00', '2020-05-24 01:00'],
            'value1': [10, 20],
            'value2': [30, 40],
        }, index=['A', 'B'])

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              data_frame_measurement_name="test",
                                              data_frame_timestamp_column="column_time",
                                              data_frame_timestamp_timezone="Europe/Berlin",
                                              point_settings=PointSettings())

        self.assertEqual(2, len(points))
        self.assertEqual('test value1=10i,value2=30i 1590307200000000000', points[0])
        self.assertEqual('test value1=20i,value2=40i 1590274800000000000', points[1])

    def test_specify_timezone_date_time_index(self):
        from influxdb_client.extras import pd
        data_frame = pd.DataFrame(data={
            'value1': [10, 20],
            'value2': [30, 40],
        }, index=[pd.Timestamp('2020-05-24 10:00'), pd.Timestamp('2020-05-24 01:00')])

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              data_frame_measurement_name="test",
                                              data_frame_timestamp_timezone="Europe/Berlin",
                                              point_settings=PointSettings())

        self.assertEqual(2, len(points))
        self.assertEqual('test value1=10i,value2=30i 1590307200000000000', points[0])
        self.assertEqual('test value1=20i,value2=40i 1590274800000000000', points[1])

    def test_specify_timezone_period_time_index(self):
        from influxdb_client.extras import pd
        data_frame = pd.DataFrame(data={
            'value1': [10, 20],
            'value2': [30, 40],
        }, index=pd.period_range(start='2020-05-24 10:00', freq='h', periods=2))

        print(data_frame.to_string())

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              data_frame_measurement_name="test",
                                              data_frame_timestamp_timezone="Europe/Berlin",
                                              point_settings=PointSettings())

        self.assertEqual(2, len(points))
        self.assertEqual('test value1=10i,value2=30i 1590307200000000000', points[0])
        self.assertEqual('test value1=20i,value2=40i 1590310800000000000', points[1])

    def test_serialization_for_nan_in_columns_starting_with_digits(self):
        from influxdb_client.extras import pd
        from influxdb_client.extras import np
        data_frame = pd.DataFrame(data={
            '1value': [np.nan, 30.0, np.nan, 30.0, np.nan],
            '2value': [30.0, np.nan, np.nan, np.nan, np.nan],
            '3value': [30.0, 30.0, 30.0, np.nan, np.nan],
            'avalue': [30.0, 30.0, 30.0, 30.0, 30.0]
        }, index=pd.period_range('2020-05-24 10:00', freq='h', periods=5))

        points = data_frame_to_list_of_points(data_frame,
                                              PointSettings(),
                                              data_frame_measurement_name='test')

        self.assertEqual(5, len(points))
        self.assertEqual('test 2value=30.0,3value=30.0,avalue=30.0 1590314400000000000', points[0])
        self.assertEqual('test 1value=30.0,3value=30.0,avalue=30.0 1590318000000000000', points[1])
        self.assertEqual('test 3value=30.0,avalue=30.0 1590321600000000000', points[2])
        self.assertEqual('test 1value=30.0,avalue=30.0 1590325200000000000', points[3])
        self.assertEqual('test avalue=30.0 1590328800000000000', points[4])

        data_frame = pd.DataFrame(data={
            '1value': [np.nan],
            'avalue': [30.0],
            'bvalue': [30.0]
        }, index=pd.period_range('2020-05-24 10:00', freq='h', periods=1))

        points = data_frame_to_list_of_points(data_frame,
                                              PointSettings(),
                                              data_frame_measurement_name='test')
        self.assertEqual(1, len(points))
        self.assertEqual('test avalue=30.0,bvalue=30.0 1590314400000000000', points[0])


class DataSerializerChunksTest(unittest.TestCase):
    def test_chunks(self):
        from influxdb_client.extras import pd
        data_frame = pd.DataFrame(
            data=[
                ["a", 1, 2],
                ["b", 3, 4],
                ["c", 5, 6],
                ["d", 7, 8],
            ],
            index=[1, 2, 3, 4],
            columns=["tag", "field1", "field2"])

        #
        # Batch size = 2
        #
        serializer = DataframeSerializer(data_frame, PointSettings(), DEFAULT_WRITE_PRECISION, 2,
                                         data_frame_measurement_name='m', data_frame_tag_columns={"tag"})
        self.assertEqual(2, serializer.number_of_chunks)
        self.assertEqual(['m,tag=a field1=1i,field2=2i 1',
                          'm,tag=b field1=3i,field2=4i 2'], serializer.serialize(chunk_idx=0))
        self.assertEqual(['m,tag=c field1=5i,field2=6i 3',
                          'm,tag=d field1=7i,field2=8i 4'], serializer.serialize(chunk_idx=1))

        #
        # Batch size = 10
        #
        serializer = DataframeSerializer(data_frame, PointSettings(), DEFAULT_WRITE_PRECISION, 10,
                                         data_frame_measurement_name='m', data_frame_tag_columns={"tag"})
        self.assertEqual(1, serializer.number_of_chunks)
        self.assertEqual(['m,tag=a field1=1i,field2=2i 1',
                          'm,tag=b field1=3i,field2=4i 2',
                          'm,tag=c field1=5i,field2=6i 3',
                          'm,tag=d field1=7i,field2=8i 4'
                          ], serializer.serialize(chunk_idx=0))

        #
        # Batch size = 3
        #
        serializer = DataframeSerializer(data_frame, PointSettings(), DEFAULT_WRITE_PRECISION, 3,
                                         data_frame_measurement_name='m', data_frame_tag_columns={"tag"})
        self.assertEqual(2, serializer.number_of_chunks)
        self.assertEqual(['m,tag=a field1=1i,field2=2i 1',
                          'm,tag=b field1=3i,field2=4i 2',
                          'm,tag=c field1=5i,field2=6i 3'
                          ], serializer.serialize(chunk_idx=0))
        self.assertEqual(['m,tag=d field1=7i,field2=8i 4'], serializer.serialize(chunk_idx=1))
