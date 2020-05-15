"""
How to ingest DataFrame with default tags.
"""

import pandas as pd

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS, PointSettings

"""
Load DataFrame form CSV File
"""
df = pd.read_csv("vix-daily.csv")
print(df.head())

client = InfluxDBClient(url="http://localhost:9999", token="my-token", org="my-org")

"""
Ingest DataFrame with default tags
"""
point_settings = PointSettings(**{"type": "vix-daily"})
point_settings.add_default_tag("example-name", "ingest-data-frame")

write_api = client.write_api(write_options=SYNCHRONOUS, point_settings=point_settings)
write_api.write(bucket="my-bucket", record=df, data_frame_measurement_name="financial-analysis-df")

"""
Querying ingested data
"""
query = 'from(bucket:"my-bucket")' \
        ' |> range(start: 0, stop: now())' \
        ' |> filter(fn: (r) => r._measurement == "financial-analysis-df")' \
        ' |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")' \
        ' |> limit(n:10, offset: 0)'
result = client.query_api().query(query=query)

"""
Processing results
"""
print()
print("=== results ===")
print()
for table in result:
    for record in table.records:
        print('{4}: Open {0}, Close {1}, High {2}, Low {3}'.format(record["VIX Open"], record["VIX Close"],
                                                                   record["VIX High"], record["VIX Low"],
                                                                   record["type"]))

"""
Close client
"""
client.__del__()
