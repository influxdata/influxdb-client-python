"""
How to ingest large DataFrame by splitting into chunks.
"""
import logging
import random
from datetime import datetime

from influxdb_client import InfluxDBClient
from influxdb_client.extras import pd, np

"""
Enable logging for DataFrame serializer
"""
loggerSerializer = logging.getLogger('influxdb_client.client.write.dataframe_serializer')
loggerSerializer.setLevel(level=logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s | %(message)s'))
loggerSerializer.addHandler(handler)

"""
Configuration
"""
url = 'http://localhost:8086'
token = 'my-token'
org = 'my-org'
bucket = 'my-bucket'

"""
Generate Dataframe
"""
print()
print("=== Generating DataFrame ===")
print()
dataframe_rows_count = 150_000

col_data = {
    'time': np.arange(0, dataframe_rows_count, 1, dtype=int),
    'tag': np.random.choice(['tag_a', 'tag_b', 'test_c'], size=(dataframe_rows_count,)),
}
for n in range(2, 2999):
    col_data[f'col{n}'] = random.randint(1, 10)

data_frame = pd.DataFrame(data=col_data).set_index('time')
print(data_frame)

"""
Ingest DataFrame
"""
print()
print("=== Ingesting DataFrame via batching API ===")
print()
startTime = datetime.now()

with InfluxDBClient(url=url, token=token, org=org) as client:

    """
    Use batching API
    """
    with client.write_api() as write_api:
        write_api.write(bucket=bucket, record=data_frame,
                        data_frame_tag_columns=['tag'],
                        data_frame_measurement_name="measurement_name")
        print()
        print("Wait to finishing ingesting DataFrame...")
        print()

print()
print(f'Import finished in: {datetime.now() - startTime}')
print()
