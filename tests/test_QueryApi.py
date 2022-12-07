import datetime
import json
import unittest

from datetime import timezone
from httpretty import httpretty

from influxdb_client import QueryApi, DurationLiteral, Duration, CallExpression, UnaryExpression, \
    Identifier, InfluxDBClient
from influxdb_client.client.query_api import QueryOptions
from influxdb_client.client.util.date_utils import get_date_helper
from tests.base_test import BaseTest


class SimpleQueryTest(BaseTest):

    def setUp(self) -> None:
        super(SimpleQueryTest, self).setUp()

        httpretty.enable()
        httpretty.reset()

    def tearDown(self) -> None:
        self.client.close()
        httpretty.disable()

    def test_query_raw(self):
        client = self.client

        query_client = client.query_api()

        bucket = "my-bucket"
        result = query_client.query_raw(
            'from(bucket:"' + bucket + '") |> range(start: 1970-01-01T00:00:00.000000001Z) |> last()')

        for x in result:
            print(x, end='')

    def test_query_flux_table(self):
        client = self.client
        bucket = "my-bucket"

        query_client = client.query_api()
        tables = query_client.query(
            'from(bucket:"' + bucket + '") |> range(start: 1970-01-01T00:00:00.000000001Z) |> last()')

        val_count = 0
        for table in tables:
            for row in table:
                for _ in row.values:
                    val_count += 1

        print("Values count: ", val_count)

    def test_query_flux_csv(self):
        client = self.client
        bucket = "my-bucket"
        query_client = client.query_api()
        csv_result = query_client.query_csv(
            'from(bucket:"' + bucket + '") |> range(start: 1970-01-01T00:00:00.000000001Z) |> last()')

        val_count = 0
        for row in csv_result:
            for _ in row:
                val_count += 1

        print("Values count: ", val_count)

    def test_query_ast(self):
        q = '''
        from(bucket:stringParam) 
            |> range(start: startDuration, stop: callParam)
            |> filter(fn: (r) => r["_measurement"] == "my_measurement")
            |> filter(fn: (r) => r["_value"] > intParam)
            |> filter(fn: (r) => r["_value"] > floatParam)
            |> aggregateWindow(every: durationParam, fn: mean, createEmpty: true)
            |> sort(columns: ["_time"], desc: booleanParam) 
        '''

        p = {
            "stringParam": "my-bucket",
            "stopParam": get_date_helper().parse_date("2021-03-20T15:59:10.607352Z"),
            "intParam": 2,
            "durationParam": DurationLiteral("DurationLiteral", [Duration(magnitude=1, unit="d")]),
            "startDuration": UnaryExpression(type="UnaryExpression",
                                             argument=DurationLiteral("DurationLiteral",
                                                                      [Duration(magnitude=30, unit="d")]),
                                             operator="-"),
            "callParam": CallExpression(type="CallExpression", callee=Identifier(type="Identifier", name="now")),
            "timedelta": datetime.timedelta(minutes=10),
            "floatParam": 14.01,
            "booleanParam": True,
        }

        csv_result = self.client.query_api().query_csv(query=q, params=p)

        self.assertIsNotNone(csv_result)

        val_count = 0
        for row in csv_result:
            for _ in row:
                val_count += 1

        print("Values count: ", val_count)

    def test_parameter_ast(self):
        test_data = [["stringParam", "my-bucket", {
            "imports": [],
            "body": [
                {
                    "type": "OptionStatement",
                    "assignment": {
                        "type": "VariableAssignment",
                        "id": {
                            "type": "Identifier",
                            "name": "stringParam"
                        },
                        "init": {
                            "type": "StringLiteral",
                            "value": "my-bucket"
                        }
                    }
                }
            ]
        }], ["datetimeParam", get_date_helper().parse_date("2021-03-20T15:59:10.607352Z"), {
            "body": [
                {
                    "assignment": {
                        "id": {
                            "name": "datetimeParam",
                            "type": "Identifier"
                        },
                        "init": {
                            "type": "DateTimeLiteral",
                            "value": "2021-03-20T15:59:10.607352000Z"
                        },
                        "type": "VariableAssignment"
                    },
                    "type": "OptionStatement"
                }
            ],
            "imports": []
        }], ["datetimeNoTZParam", datetime.datetime(2021, 3, 20, 15, 59, 10, 607352), {
            "body": [
                {
                    "assignment": {
                        "id": {
                            "name": "datetimeNoTZParam",
                            "type": "Identifier"
                        },
                        "init": {
                            "type": "DateTimeLiteral",
                            "value": "2021-03-20T15:59:10.607352000Z"
                        },
                        "type": "VariableAssignment"
                    },
                    "type": "OptionStatement"
                }
            ],
            "imports": []
        }], ["timeDeltaParam", datetime.timedelta(hours=1), {
            "body": [
                {
                    "assignment": {
                        "id": {
                            "name": "timeDeltaParam",
                            "type": "Identifier"
                        },
                        "init": {
                            "type": "DurationLiteral",
                            "values": [
                                {
                                    "magnitude": 3600000000,
                                    "unit": "us"
                                }
                            ]
                        },
                        "type": "VariableAssignment"
                    },
                    "type": "OptionStatement"
                }
            ],
            "imports": []
        }], ["timeDeltaNegativeParam", datetime.timedelta(minutes=-5), {
            "body": [
                {
                    "assignment": {
                        "id": {
                            "name": "timeDeltaNegativeParam",
                            "type": "Identifier"
                        },
                        "init": {
                            "argument": {
                                "type": "DurationLiteral",
                                "values": [
                                    {
                                        "magnitude": 300000000,
                                        "unit": "us"
                                    }
                                ]
                            },
                            "operator": "-",
                            "type": "UnaryExpression"
                        },
                        "type": "VariableAssignment"
                    },
                    "type": "OptionStatement"
                }
            ],
            "imports": []
        }], ["booleanParam", True, {
            "body": [
                {
                    "assignment": {
                        "id": {
                            "name": "booleanParam",
                            "type": "Identifier"
                        },
                        "init": {
                            "type": "BooleanLiteral",
                            "value": True
                        },
                        "type": "VariableAssignment"
                    },
                    "type": "OptionStatement"
                }
            ],
            "imports": []
        }], ["intParam", int(10), {
            "body": [
                {
                    "assignment": {
                        "id": {
                            "name": "intParam",
                            "type": "Identifier"
                        },
                        "init": {
                            "type": "IntegerLiteral",
                            "value": "10"
                        },
                        "type": "VariableAssignment"
                    },
                    "type": "OptionStatement"
                }
            ],
            "imports": []
        }], ["floatParam", 10.333,
             {
                 "body": [
                     {
                         "assignment": {
                             "id": {
                                 "name": "floatParam",
                                 "type": "Identifier"
                             },
                             "init": {
                                 "type": "FloatLiteral",
                                 "value": 10.333
                             },
                             "type": "VariableAssignment"
                         },
                         "type": "OptionStatement"
                     }
                 ],
                 "imports": []
             }], ["arrayParam", ["bar1", "bar2", "bar3"],
                  {
                      "body": [
                          {
                              "assignment": {
                                  "id": {
                                      "name": "arrayParam",
                                      "type": "Identifier"
                                  },
                                  "init": {
                                      "elements": [
                                          {
                                              "type": "StringLiteral",
                                              "value": "bar1"
                                          },
                                          {
                                              "type": "StringLiteral",
                                              "value": "bar2"
                                          },
                                          {
                                              "type": "StringLiteral",
                                              "value": "bar3"
                                          }
                                      ],
                                      "type": "ArrayExpression"
                                  },
                                  "type": "VariableAssignment"
                              },
                              "type": "OptionStatement"
                          }
                      ],
                      "imports": []
                  }]]

        for data in test_data:
            param = {data[0]: data[1]}
            print("testing: ", param)
            ast = QueryApi._build_flux_ast(param)
            got_sanitized = self.client.api_client.sanitize_for_serialization(ast)
            self.assertEqual(json.dumps(got_sanitized, sort_keys=True, indent=2),
                             json.dumps(data[2], sort_keys=True, indent=2))

    def test_query_profiler_enabled(self):
        q = '''
        from(bucket:stringParam) 
            |> range(start: 0, stop: callParam) |> last()
        '''
        p = {
            "stringParam": "my-bucket",
            "stopParam": get_date_helper().parse_date("2021-03-20T15:59:10.607352Z"),
            "durationParam": DurationLiteral("DurationLiteral", [Duration(magnitude=1, unit="d")]),
            "callParam": CallExpression(type="CallExpression", callee=Identifier(type="Identifier", name="now")),
        }
        query_api = self.client.query_api(query_options=QueryOptions(profilers=["query", "operator"]))
        csv_result = query_api.query(query=q, params=p)

        for table in csv_result:
            self.assertFalse(any(filter(lambda column: (column.default_value == "_profiler"), table.columns)))
            for flux_record in table:
                self.assertFalse(flux_record["_measurement"].startswith("profiler/"))

        records = self.client.query_api().query_stream(query=q, params=p)

        for flux_record in records:
            self.assertFalse(flux_record["_measurement"].startswith("profiler/"))

        self.assertIsNotNone(csv_result)

    def test_query_profiler_present(self):

        client = self.client
        q = '''
        import "profiler"

        option profiler.enabledProfilers = ["query", "operator"]

        from(bucket:stringParam) 
            |> range(start: 0, stop: callParam) |> last()
        '''

        p = {
            "stringParam": "my-bucket",
            "stopParam": get_date_helper().parse_date("2021-03-20T15:59:10.607352Z"),
            "durationParam": DurationLiteral("DurationLiteral", [Duration(magnitude=1, unit="d")]),
            "callParam": CallExpression(type="CallExpression", callee=Identifier(type="Identifier", name="now")),
        }
        csv_result = client.query_api(query_options=QueryOptions(profilers=None)).query(query=q, params=p)
        self.assertIsNotNone(csv_result)

        found_profiler_table = False
        found_profiler_records = False

        for table in csv_result:
            if any(filter(lambda column: (column.default_value == "_profiler"), table.columns)):
                found_profiler_table = True
                print(f"Profiler table : {table} ")
            for flux_record in table:
                if flux_record["_measurement"].startswith("profiler/"):
                    found_profiler_records = True
                    print(f"Profiler record: {flux_record}")

        self.assertTrue(found_profiler_table)
        self.assertTrue(found_profiler_records)

        records = client.query_api().query_stream(query=q, params=p)

        found_profiler_records = False
        for flux_record in records:
            if flux_record["_measurement"].startswith("profiler/"):
                found_profiler_records = True
                print(f"Profiler record: {flux_record}")
        self.assertTrue(found_profiler_records)

    def test_profilers_callback(self):

        class ProfilersCallback(object):
            def __init__(self):
                self.records = []

            def __call__(self, flux_record):
                self.records.append(flux_record.values)

            def get_record(self, num, val):
                return (self.records[num])[val]

        callback = ProfilersCallback()

        query_api = self.client.query_api(query_options=QueryOptions(profilers=["query", "operator"],
                                                                     profiler_callback=callback))
        query_api.query('from(bucket:"my-bucket") |> range(start: -10m)')

        self.assertEqual("profiler/query", callback.get_record(0, "_measurement"))
        self.assertEqual("profiler/operator", callback.get_record(1, "_measurement"))

    def test_profiler_ast(self):

        expect = {
            "body": [
                {
                    "assignment": {
                        "init": {
                            "elements": [
                                {
                                    "type": "StringLiteral",
                                    "value": "first-profiler"
                                },
                                {
                                    "type": "StringLiteral",
                                    "value": "second-profiler"
                                }
                            ],
                            "type": "ArrayExpression"
                        },
                        "member": {
                            "object": {
                                "name": "profiler",
                                "type": "Identifier"
                            },
                            "property": {
                                "name": "enabledProfilers",
                                "type": "Identifier"
                            },
                            "type": "MemberExpression"
                        },
                        "type": "MemberAssignment"
                    },
                    "type": "OptionStatement"
                }
            ],
            "imports": [
                {
                    "path": {
                        "type": "StringLiteral",
                        "value": "profiler"
                    },
                    "type": "ImportDeclaration"
                }
            ]
        }

        ast = QueryApi._build_flux_ast(params=None, profilers=["first-profiler", "second-profiler"])
        got_sanitized = self.client.api_client.sanitize_for_serialization(ast)
        print(json.dumps(got_sanitized, sort_keys=True, indent=2))

        self.assertEqual(json.dumps(got_sanitized, sort_keys=True, indent=2),
                         json.dumps(expect, sort_keys=True, indent=2))

    def test_profiler_mock(self):

        query_response = """#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,double,double,double
#group,false,false,true,true,false,true,true,false,false,false
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_measurement,host,available,free,used
,,0,2021-05-24T08:40:44.7850004Z,2021-05-24T08:45:44.7850004Z,2021-05-24T08:41:00Z,mem,kozel.local,5832097792,317063168,11347771392
,,0,2021-05-24T08:40:44.7850004Z,2021-05-24T08:45:44.7850004Z,2021-05-24T08:42:00Z,mem,kozel.local,5713765717.333333,118702080,11466103466.666666
,,0,2021-05-24T08:40:44.7850004Z,2021-05-24T08:45:44.7850004Z,2021-05-24T08:43:00Z,mem,kozel.local,5776302080,135763968,11403567104
,,0,2021-05-24T08:40:44.7850004Z,2021-05-24T08:45:44.7850004Z,2021-05-24T08:44:00Z,mem,kozel.local,5758485162.666667,85798229.33333333,11421384021.333334
,,0,2021-05-24T08:40:44.7850004Z,2021-05-24T08:45:44.7850004Z,2021-05-24T08:45:00Z,mem,kozel.local,5788656981.333333,119243434.66666667,11391212202.666666
,,0,2021-05-24T08:40:44.7850004Z,2021-05-24T08:45:44.7850004Z,2021-05-24T08:45:44.7850004Z,mem,kozel.local,5727718400,35330048,11452150784

#datatype,string,long,string,long,long,long,long,long,long,long,long,long,string,string,long,long
#group,false,false,true,false,false,false,false,false,false,false,false,false,false,false,false,false
#default,_profiler,,,,,,,,,,,,,,,
,result,table,_measurement,TotalDuration,CompileDuration,QueueDuration,PlanDuration,RequeueDuration,ExecuteDuration,Concurrency,MaxAllocated,TotalAllocated,RuntimeErrors,flux/query-plan,influxdb/scanned-bytes,influxdb/scanned-values
,,0,profiler/query,8924700,350900,33800,0,0,8486500,0,2072,0,,"digraph {
  ReadWindowAggregateByTime11
  // every = 1m, aggregates = [mean], createEmpty = true, timeColumn = ""_stop""
  pivot8
  generated_yield

  ReadWindowAggregateByTime11 -> pivot8
  pivot8 -> generated_yield
}

",0,0

#datatype,string,long,string,string,string,long,long,long,long,double
#group,false,false,true,false,false,false,false,false,false,false
#default,_profiler,,,,,,,,,
,result,table,_measurement,Type,Label,Count,MinDuration,MaxDuration,DurationSum,MeanDuration
,,1,profiler/operator,*universe.pivotTransformation,pivot8,3,32600,126200,193400,64466.666666666664
,,1,profiler/operator,*influxdb.readWindowAggregateSource,ReadWindowAggregateByTime11,1,940500,940500,940500,940500
"""

        query = """
        from(bucket: "my-bucket") 
          |> range(start: -5m, stop: now()) 
          |> filter(fn: (r) => r._measurement == "mem") 
          |> filter(fn: (r) => r._field == "available" or r._field == "free" or r._field == "used")
          |> aggregateWindow(every: 1m, fn: mean)
          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        """

        httpretty.register_uri(httpretty.POST, uri="http://localhost/api/v2/query", status=200, body=query_response)
        self.client = InfluxDBClient("http://localhost", "my-token", org="my-org", enable_gzip=False)
        query_api = self.client.query_api(query_options=QueryOptions(profilers=["query", "operator"]))
        tables = query_api.query(query=query)
        self.assertEqual(len(tables), 1)
        self.assertEqual(len(tables[0].columns), 10)
        self.assertEqual(len(tables[0].records), 6)

        self.assertEqual(tables[0].records[5].values,
                          {'result': '_result', 'table': 0,
                           '_start': datetime.datetime(2021, 5, 24, 8, 40, 44, 785000, tzinfo=timezone.utc),
                           '_stop': datetime.datetime(2021, 5, 24, 8, 45, 44, 785000, tzinfo=timezone.utc),
                           '_time': datetime.datetime(2021, 5, 24, 8, 45, 44, 785000, tzinfo=timezone.utc),
                           '_measurement': 'mem',
                           'host': 'kozel.local',
                           'available': 5727718400, 'free': 35330048,
                           'used': 11452150784})

    def test_time_to_ast(self):
        from influxdb_client.extras import pd
        import dateutil.parser

        literals = [
            (pd.Timestamp('1996-02-25T21:20:00.001001231Z'), '1996-02-25T21:20:00.001001231Z'),
            (dateutil.parser.parse('1996-02-25T21:20:00.001001231Z'), '1996-02-25T21:20:00.001001000Z'),
            (dateutil.parser.parse('1996-02-25'), '1996-02-25T00:00:00.000000000Z'),
            (datetime.datetime(2021, 5, 24, 8, 40, 44, 785000, tzinfo=timezone.utc), '2021-05-24T08:40:44.785000000Z'),
        ]

        for literal in literals:
            ast = QueryApi._build_flux_ast({'date': literal[0]})
            self.assertEqual('DateTimeLiteral', ast.body[0].assignment.init.type)
            self.assertEqual(literal[1], ast.body[0].assignment.init.value)

    def test_csv_empty_lines(self):
        query_response = '#datatype,string,long,dateTime:RFC3339,double,string\n' \
                         '#group,false,false,false,false,true\n' \
                         '#default,_result,,,,\n' \
                         ',result,table,_time,_value,_field\n' \
                         ',,0,2022-11-24T10:00:10Z,0.1,_1_current_(mA)\n' \
                         ',,1,2022-11-24T10:00:10Z,4,_1_current_limit_(mA)\n' \
                         ',,2,2022-11-24T10:00:10Z,1,_1_voltage_(V)\n' \
                         ',,3,2022-11-24T10:00:10Z,1,_1_voltage_limit_(V)\n' \
                         ',,4,2022-11-24T10:00:10Z,0,_2_current_(mA)\n' \
                         ',,5,2022-11-24T10:00:10Z,0,_2_current_limit_(mA)\n' \
                         ',,6,2022-11-24T10:00:10Z,0,_2_voltage_(V)\n' \
                         ',,7,2022-11-24T10:00:10Z,0,_2_voltage_limit_(V)\n' \
                         '\n' \
                         '\n' \
                         '#datatype,string,long,dateTime:RFC3339,string,string\n' \
                         '#group,false,false,false,false,true\n' \
                         '#default,_result,,,,\n' \
                         ',result,table,_time,_value,_field\n' \
                         ',,8,2022-11-24T10:00:10Z,K,type\n' \
                         ',,9,2022-11-24T10:00:10Z,,type2\n' \
                         '\n'

        httpretty.register_uri(httpretty.POST, uri="http://localhost/api/v2/query", status=200, body=query_response)

        self.client = InfluxDBClient("http://localhost", "my-token", org="my-org", enable_gzip=False)

        csv_lines = list(self.client.query_api().query_csv('from(bucket: "my-bucket")', "my-org"))
        self.assertEqual(18, len(csv_lines))
        for csv_line in csv_lines:
            self.assertEqual(6, len(csv_line))

        # to_values
        csv_lines = self.client.query_api().query_csv('from(bucket: "my-bucket")', "my-org").to_values()
        self.assertEqual(18, len(csv_lines))

if __name__ == '__main__':
    unittest.main()
