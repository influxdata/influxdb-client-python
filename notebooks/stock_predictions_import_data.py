"""
Import VIX - CBOE Volatility Index - from "vix-daily.csv" file into InfluxDB 2.0

https://datahub.io/core/finance-vix#data
"""
from collections import OrderedDict
from csv import DictReader

import ciso8601
import requests
import rx
from pytz import UTC
from rx import operators as ops

from influxdb_client import InfluxDBClient, WriteOptions
from influxdb_client.client.write.point import EPOCH

_progress = 0


def parse_row(row: OrderedDict):
    """Parse row of CSV file into LineProtocol with structure:

    CSV format:
        date,symbol,open,close,low,high,volume
        2016-01-05,WLTW,123.43,125.839996,122.309998,126.25,2163600.0
        2016-01-06,WLTW,125.239998,119.980003,119.940002,125.540001,2386400.0
        2016-01-07,WLTW,116.379997,114.949997,114.93,119.739998,2489500.0
        2016-01-08,WLTW,115.480003,116.620003,113.5,117.440002,2006300.0
        2016-01-11,WLTW,117.010002,114.970001,114.089996,117.330002,1408600.0
        2016-01-12,WLTW,115.510002,115.550003,114.5,116.059998,1098000.0
        2016-01-13,WLTW,116.459999,112.849998,112.589996,117.07,949600.0
        ...

    :param row: the row of CSV file
    :return: Parsed csv row to LineProtocol
    """
    global _progress
    _progress += 1

    if _progress % 10000 == 0:
        print(_progress)

    time = (UTC.localize(ciso8601.parse_datetime(row["date"])) - EPOCH).total_seconds() * 1e9

    return f'financial-analysis,symbol={row["symbol"]} ' \
           f'close={row["close"]},high={row["high"]},low={row["low"]},open={row["open"]} ' \
           f'{int(time)}'


def main():
    parse_row.progress = 0

    url = "https://github.com/influxdata/influxdb-client-python/wiki/data/stock-prices-example.csv"
    response = requests.get(url, stream=True)
    data = rx \
        .from_iterable(DictReader(response.iter_lines(decode_unicode=True))) \
        .pipe(ops.map(lambda row: parse_row(row)))

    client = InfluxDBClient(url="http://localhost:9999", token="my-token", org="my-org", debug=False)
    write_api = client.write_api(write_options=WriteOptions(batch_size=50_000, flush_interval=10_000))

    write_api.write(bucket="my-bucket", record=data)
    write_api.__del__()

    query = '''
    from(bucket:"my-bucket")
            |> range(start: 0, stop: now())
            |> filter(fn: (r) => r._measurement == "financial-analysis")
            |> filter(fn: (r) => r.symbol == "AAPL")
            |> filter(fn: (r) => r._field == "close")
            |> drop(columns: ["_start", "_stop", "table", "_field","_measurement"])
    '''

    result = client.query_api().query_data_frame(query=query)
    print(result.head(100))

    """
    Close client
    """
    client.__del__()
    # %%


if __name__ == '__main__':
    main()
