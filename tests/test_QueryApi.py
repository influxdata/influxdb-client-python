import datetime
import json
import unittest

from influxdb_client import QueryApi, DurationLiteral, Duration, CallExpression, Expression, UnaryExpression, Identifier
from influxdb_client.client.util.date_utils import get_date_helper
from tests.base_test import BaseTest


class SimpleQueryTest(BaseTest):

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
                for cell in row.values:
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
            for cell in row:
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
            for cell in row:
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
                            "value": "2021-03-20T15:59:10.607352Z"
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
                            "value": "2021-03-20T15:59:10.607352Z"
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
             }]]

        for data in test_data:
            param = {data[0]: data[1]}
            print("testing: ", param)
            ast = QueryApi._build_flux_ast(param)
            got_sanitized = self.client.api_client.sanitize_for_serialization(ast)
            self.assertEqual(json.dumps(got_sanitized, sort_keys=True, indent=2),
                             json.dumps(data[2], sort_keys=True, indent=2))


if __name__ == '__main__':
    unittest.main()
