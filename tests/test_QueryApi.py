import unittest

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


if __name__ == '__main__':
    unittest.main()
