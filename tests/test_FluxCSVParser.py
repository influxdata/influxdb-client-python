import math
import unittest
from io import BytesIO

from urllib3 import HTTPResponse

from influxdb_client.client.flux_csv_parser import FluxCsvParser, FluxSerializationMode, FluxQueryException, \
    FluxResponseMetadataMode
from influxdb_client.client.flux_table import FluxStructureEncoder


class FluxCsvParserTest(unittest.TestCase):

    def test_one_table(self):
        data = "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,string,string,string,string,long,long,string\n" \
               "#group,false,false,true,true,true,true,true,true,false,false,false\n" \
               "#default,_result,,,,,,,,,,\n" \
               ",result,table,_start,_stop,_field,_measurement,host,region,_value2,value1,value_str\n" \
               ",,0,1677-09-21T00:12:43.145224192Z,2018-07-16T11:21:02.547596934Z,free,mem,A,west,121,11,test\n"

        tables = self._parse_to_tables(data=data)
        self.assertEqual(1, tables.__len__())
        self.assertEqual(11, tables[0].columns.__len__())
        self.assertEqual(1, tables[0].records.__len__())

    def test_more_tables(self):
        data = "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,string,string,string,string,long,long,string\n" \
               "#group,false,false,true,true,true,true,true,true,false,false,false\n" \
               "#default,_result,,,,,,,,,,\n" \
               ",result,table,_start,_stop,_field,_measurement,host,region,_value2,value1,value_str\n" \
               ",,0,1677-09-21T00:12:43.145224192Z,2018-07-16T11:21:02.547596934Z,free,mem,A,west,121,11,test\n" \
               ",,1,1677-09-21T00:12:43.145224192Z,2018-07-16T11:21:02.547596934Z,free,mem,B,west,484,22,test\n" \
               ",,2,1677-09-21T00:12:43.145224192Z,2018-07-16T11:21:02.547596934Z,usage_system,cpu,A,west,1444,38,test\n" \
               ",,3,1677-09-21T00:12:43.145224192Z,2018-07-16T11:21:02.547596934Z,user_usage,cpu,A,west,2401,49,test"

        tables = self._parse_to_tables(data=data)
        self.assertEqual(4, tables.__len__())
        self.assertEqual(11, tables[0].columns.__len__())
        self.assertEqual(1, tables[0].records.__len__())
        self.assertEqual(11, tables[1].columns.__len__())
        self.assertEqual(1, tables[1].records.__len__())
        self.assertEqual(11, tables[2].columns.__len__())
        self.assertEqual(1, tables[2].records.__len__())
        self.assertEqual(11, tables[3].columns.__len__())
        self.assertEqual(1, tables[3].records.__len__())

    def test_multiple_queries(self):
        data = "#datatype,string,long,string,string,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string\n" \
               "#group,false,false,true,true,true,true,false,false,true\n" \
               "#default,t1,,,,,,,,\n" \
               ",result,table,_field,_measurement,_start,_stop,_time,_value,tag\n" \
               ",,0,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:20:00Z,2,test1\n" \
               ",,0,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:21:40Z,2,test1\n" \
               ",,0,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:23:20Z,2,test1\n" \
               ",,0,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:25:00Z,2,test1\n" \
               ",,0,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:26:40Z,2,test1\n" \
               ",,0,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:28:20Z,2,test1\n" \
               ",,0,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:30:00Z,2,test1\n" \
               ",,1,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:20:00Z,2,test2\n" \
               ",,1,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:21:40Z,2,test2\n" \
               ",,1,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:23:20Z,2,test2\n" \
               ",,1,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:25:00Z,2,test2\n" \
               ",,1,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:26:40Z,2,test2\n" \
               ",,1,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:28:20Z,2,test2\n" \
               ",,1,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:30:00Z,2,test2\n" \
               "\n" \
               "#datatype,string,long,string,string,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string\n" \
               "#group,false,false,true,true,true,true,false,false,true\n" \
               "#default,t2,,,,,,,,\n" \
               ",result,table,_field,_measurement,_start,_stop,_time,_value,tag\n" \
               ",,0,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:20:00Z,2,test1\n" \
               ",,0,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:21:40Z,2,test1\n" \
               ",,0,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:23:20Z,2,test1\n" \
               ",,0,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:25:00Z,2,test1\n" \
               ",,0,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:26:40Z,2,test1\n" \
               ",,0,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:28:20Z,2,test1\n" \
               ",,0,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:30:00Z,2,test1\n" \
               ",,1,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:20:00Z,2,test2\n" \
               ",,1,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:21:40Z,2,test2\n" \
               ",,1,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:23:20Z,2,test2\n" \
               ",,1,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:25:00Z,2,test2\n" \
               ",,1,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:26:40Z,2,test2\n" \
               ",,1,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:28:20Z,2,test2\n" \
               ",,1,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:30:00Z,2,test2"

        tables = self._parse_to_tables(data=data)
        self.assertEqual(4, tables.__len__())
        self.assertEqual(9, tables[0].columns.__len__())
        self.assertEqual(7, tables[0].records.__len__())
        self.assertEqual(9, tables[1].columns.__len__())
        self.assertEqual(7, tables[1].records.__len__())
        self.assertEqual(9, tables[2].columns.__len__())
        self.assertEqual(7, tables[2].records.__len__())
        self.assertEqual(9, tables[3].columns.__len__())
        self.assertEqual(7, tables[3].records.__len__())

    def test_table_index_not_start_at_zero(self):
        data = "#datatype,string,long,string,string,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string\n" \
               "#group,false,false,true,true,true,true,false,false,true\n" \
               "#default,t1,,,,,,,,\n" \
               ",result,table,_field,_measurement,_start,_stop,_time,_value,tag\n" \
               ",,1,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:20:00Z,2,test1\n" \
               ",,1,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:21:40Z,2,test1\n" \
               ",,1,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:23:20Z,2,test1\n" \
               ",,1,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:25:00Z,2,test1\n" \
               ",,1,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:26:40Z,2,test1\n" \
               ",,1,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:28:20Z,2,test1\n" \
               ",,1,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:30:00Z,2,test1\n" \
               ",,2,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:20:00Z,2,test2\n" \
               ",,2,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:21:40Z,2,test2\n" \
               ",,2,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:23:20Z,2,test2\n" \
               ",,2,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:25:00Z,2,test2\n" \
               ",,2,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:26:40Z,2,test2\n" \
               ",,2,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:28:20Z,2,test2\n" \
               ",,2,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:30:00Z,2,test2\n"

        tables = self._parse_to_tables(data=data)
        self.assertEqual(2, tables.__len__())
        self.assertEqual(9, tables[0].columns.__len__())
        self.assertEqual(7, tables[0].records.__len__())
        self.assertEqual(9, tables[1].columns.__len__())
        self.assertEqual(7, tables[1].records.__len__())

    def test_response_with_error(self):
        data = "#datatype,string,long,string,string,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string\n" \
                 "#group,false,false,true,true,true,true,false,false,true\n" \
                 "#default,t1,,,,,,,,\n" \
                 ",result,table,_field,_measurement,_start,_stop,_time,_value,tag\n" \
                 ",,0,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:20:00Z,2,test1\n" \
                 ",,0,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:21:40Z,2,test1\n" \
                 ",,0,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:23:20Z,2,test1\n" \
                 ",,0,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:25:00Z,2,test1\n" \
                 "\n" \
                 "#datatype,string,string\n" \
                 "#group,true,true\n" \
                 "#default,,\n" \
                 ",error,reference\n" \
                 ",\"engine: unknown field type for value: xyz\","

        with self.assertRaises(FluxQueryException) as cm:
            self._parse_to_tables(data=data)
        exception = cm.exception

        self.assertEqual('engine: unknown field type for value: xyz', exception.message)
        self.assertEqual('', exception.reference)

    def test_ParseExportFromUserInterface(self):

        data = "#group,false,false,true,true,true,true,true,true,false,false\n" \
           + "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,string,string,string,string,double,dateTime:RFC3339\n" \
           + "#default,mean,,,,,,,,,\n" \
           + ",result,table,_start,_stop,_field,_measurement,city,location,_value,_time\n" \
           + ",,0,1754-06-26T11:30:27.613654848Z,2040-10-27T12:13:46.485Z,temperatureC,weather,London,us-midwest,30,1975-09-01T16:59:54.5Z\n" \
           + ",,1,1754-06-26T11:30:27.613654848Z,2040-10-27T12:13:46.485Z,temperatureF,weather,London,us-midwest,86,1975-09-01T16:59:54.5Z\n";

        tables = self._parse_to_tables(data=data)
        self.assertEqual(2, tables.__len__())
        self.assertEqual(1, tables[0].records.__len__())
        self.assertEqual(1, tables[1].records.__len__())
        self.assertFalse(tables[1].columns[0].group)
        self.assertFalse(tables[1].columns[1].group)
        self.assertTrue(tables[1].columns[2].group)

    def test_ParseInf(self):

        data = """#group,false,false,true,true,true,true,true,true,true,true,false,false
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,string,string,string,string,string,string,double,double
#default,_result,,,,,,,,,,,
,result,table,_start,_stop,_field,_measurement,language,license,name,owner,le,_value
,,0,2021-06-23T06:50:11.897825012Z,2021-06-25T06:50:11.897825012Z,stars,github_repository,C#,MIT License,influxdb-client-csharp,influxdata,0,0
,,0,2021-06-23T06:50:11.897825012Z,2021-06-25T06:50:11.897825012Z,stars,github_repository,C#,MIT License,influxdb-client-csharp,influxdata,10,0
,,0,2021-06-23T06:50:11.897825012Z,2021-06-25T06:50:11.897825012Z,stars,github_repository,C#,MIT License,influxdb-client-csharp,influxdata,20,0
,,0,2021-06-23T06:50:11.897825012Z,2021-06-25T06:50:11.897825012Z,stars,github_repository,C#,MIT License,influxdb-client-csharp,influxdata,30,0
,,0,2021-06-23T06:50:11.897825012Z,2021-06-25T06:50:11.897825012Z,stars,github_repository,C#,MIT License,influxdb-client-csharp,influxdata,40,0
,,0,2021-06-23T06:50:11.897825012Z,2021-06-25T06:50:11.897825012Z,stars,github_repository,C#,MIT License,influxdb-client-csharp,influxdata,50,0
,,0,2021-06-23T06:50:11.897825012Z,2021-06-25T06:50:11.897825012Z,stars,github_repository,C#,MIT License,influxdb-client-csharp,influxdata,60,0
,,0,2021-06-23T06:50:11.897825012Z,2021-06-25T06:50:11.897825012Z,stars,github_repository,C#,MIT License,influxdb-client-csharp,influxdata,70,0
,,0,2021-06-23T06:50:11.897825012Z,2021-06-25T06:50:11.897825012Z,stars,github_repository,C#,MIT License,influxdb-client-csharp,influxdata,80,0
,,0,2021-06-23T06:50:11.897825012Z,2021-06-25T06:50:11.897825012Z,stars,github_repository,C#,MIT License,influxdb-client-csharp,influxdata,90,0
,,0,2021-06-23T06:50:11.897825012Z,2021-06-25T06:50:11.897825012Z,stars,github_repository,C#,MIT License,influxdb-client-csharp,influxdata,+Inf,15
,,0,2021-06-23T06:50:11.897825012Z,2021-06-25T06:50:11.897825012Z,stars,github_repository,C#,MIT License,influxdb-client-csharp,influxdata,-Inf,15

"""
        tables = self._parse_to_tables(data=data)
        self.assertEqual(1, tables.__len__())
        self.assertEqual(12, tables[0].records.__len__())
        self.assertEqual(math.inf, tables[0].records[10]["le"])
        self.assertEqual(-math.inf, tables[0].records[11]["le"])

    def test_to_json(self):
        data = "#datatype,string,long,string,string,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string\n" \
               "#group,false,false,true,true,true,true,false,false,true\n" \
               "#default,_result,,,,,,,,\n" \
               ",result,table,_field,_measurement,_start,_stop,_time,_value,tag\n" \
               ",,0,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:20:00Z,2,test1\n" \
               ",,0,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:21:40Z,2,test1\n" \
               ",,0,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:23:20Z,2,test1\n" \
               ",,0,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:25:00Z,2,test1\n" \
               ",,0,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:26:40Z,2,test1\n" \
               ",,0,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:28:20Z,2,test1\n" \
               ",,0,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:30:00Z,2,test1\n" \
               ",,1,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:20:00Z,2,test2\n" \
               ",,1,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:21:40Z,2,test2\n" \
               ",,1,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:23:20Z,2,test2\n" \
               ",,1,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:25:00Z,2,test2\n" \
               ",,1,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:26:40Z,2,test2\n" \
               ",,1,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:28:20Z,2,test2\n" \
               ",,1,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:30:00Z,2,test2\n"

        tables = self._parse_to_tables(data=data)
        with open('tests/query_output.json', 'r') as file:
            query_output = file.read()
            import json
            self.assertEqual(query_output, json.dumps(tables, cls=FluxStructureEncoder, indent=2))

    def test_pandas_lot_of_columns(self):
        data_types = ""
        groups = ""
        defaults = ""
        columns = ""
        values = ""
        for i in range(0, 200):
            data_types += f",long"
            groups += f",false"
            defaults += f","
            columns += f",column_{i}"
            values += f",{i}"

        data = f"#datatype,string,long,string,string,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string{data_types}\n" \
               f"#group,false,false,true,true,true,true,false,false,true{groups}\n" \
               f"#default,_result,,,,,,,,{defaults}\n" \
               f",result,table,_field,_measurement,_start,_stop,_time,_value,tag{columns}\n" \
               f",,0,value,python_client_test,2010-02-27T04:48:32.752600083Z,2020-02-27T16:48:32.752600083Z,2020-02-27T16:20:00Z,2,test1{values}\n" \

        parser = self._parse(data=data, serialization_mode=FluxSerializationMode.dataFrame,
                             response_metadata_mode=FluxResponseMetadataMode.full)
        _dataFrames = list(parser.generator())
        self.assertEqual(1, _dataFrames.__len__())

    def test_pandas_column_datatype(self):
        data = "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,string,string,string,string,long,unsignedLong,string,boolean,double\n" \
               "#group,false,false,true,true,true,true,true,true,false,false,false,false,false\n" \
               "#default,_result,,,,,,,,,,,,\n" \
               ",result,table,_start,_stop,_field,_measurement,host,region,value1,value2,value3,value4,value5\n" \
               ",,0,1977-09-21T00:12:43.145224192Z,2018-07-16T11:21:02.547596934Z,free,mem,A,west,121,11,test,true,6.56\n"
        parser = self._parse(data=data, serialization_mode=FluxSerializationMode.dataFrame,
                             response_metadata_mode=FluxResponseMetadataMode.full)
        df = list(parser.generator())[0]
        self.assertEqual(13, df.dtypes.__len__())
        self.assertEqual('object', df.dtypes['result'].name)
        self.assertEqual('int64', df.dtypes['table'].name)
        self.assertIn('datetime64[ns,', df.dtypes['_start'].name)
        self.assertIn('datetime64[ns,', df.dtypes['_stop'].name)
        self.assertEqual('object', df.dtypes['_field'].name)
        self.assertEqual('object', df.dtypes['_measurement'].name)
        self.assertEqual('object', df.dtypes['host'].name)
        self.assertEqual('object', df.dtypes['region'].name)
        self.assertEqual('int64', df.dtypes['value1'].name)
        self.assertEqual('int64', df.dtypes['value2'].name)
        self.assertEqual('object', df.dtypes['value3'].name)
        self.assertEqual('bool', df.dtypes['value4'].name)
        self.assertEqual('float64', df.dtypes['value5'].name)

    def test_parse_without_datatype(self):
        data = ",result,table,_start,_stop,_field,_measurement,host,region,_value2,value1,value_str\n" \
               ",,0,1677-09-21T00:12:43.145224192Z,2018-07-16T11:21:02.547596934Z,free,mem,A,west,121,11,test\n" \
               ",,1,1677-09-21T00:12:43.145224192Z,2018-07-16T11:21:02.547596934Z,free,mem,A,west,121,11,test\n"

        tables = self._parse_to_tables(data=data, response_metadata_mode=FluxResponseMetadataMode.only_names)
        self.assertEqual(2, tables.__len__())
        self.assertEqual(11, tables[0].columns.__len__())
        self.assertEqual(1, tables[0].records.__len__())
        self.assertEqual(11, tables[0].records[0].values.__len__())
        self.assertEqual('0', tables[0].records[0]['table'])
        self.assertEqual('11', tables[0].records[0]['value1'])
        self.assertEqual('west', tables[0].records[0]['region'])

    @staticmethod
    def _parse_to_tables(data: str, serialization_mode=FluxSerializationMode.tables,
                         response_metadata_mode=FluxResponseMetadataMode.full):
        _parser = FluxCsvParserTest._parse(data, serialization_mode, response_metadata_mode=response_metadata_mode)
        list(_parser.generator())
        tables = _parser.tables
        return tables

    @staticmethod
    def _parse(data, serialization_mode, response_metadata_mode):
        fp = BytesIO(str.encode(data))
        return FluxCsvParser(response=HTTPResponse(fp, preload_content=False),
                             serialization_mode=serialization_mode, response_metadata_mode=response_metadata_mode)
