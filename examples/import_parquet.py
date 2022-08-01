import pyarrow.parquet as pq

from influxdb_client import InfluxDBClient, WriteOptions

with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org", timeout=0, debug=False) as client:
    """
    You can download NYC TLC Trip Record Data parquet file from https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
    """
    table = pq.read_table('fhvhv_tripdata_2022-01.parquet')
    with client.write_api(write_options=WriteOptions(batch_size=50_000)) as write_api:

        dataframe = table.to_pandas()
        """
        Keep only interesting columns
        """
        keep_df = dataframe[
            ['dispatching_base_num', "PULocationID", "DOLocationID", "pickup_datetime", "dropoff_datetime", "shared_request_flag"]]
        print(keep_df.tail().to_string())

        write_api.write(bucket="my-bucket", record=keep_df, data_frame_measurement_name="taxi-trip-data",
                        data_frame_tag_columns=['dispatching_base_num', "shared_request_flag"],
                        data_frame_timestamp_column="pickup_datetime")

    """
    Querying 10 pickups from dispatching 'B03404'
    """
    query = '''
            from(bucket:"my-bucket")
            |> range(start: 2022-01-01T00:00:00Z, stop: now()) 
            |> filter(fn: (r) => r._measurement == "taxi-trip-data")
            |> filter(fn: (r) => r.dispatching_base_num == "B03404")
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> rename(columns: {_time: "pickup_datetime"})
            |> drop(columns: ["_start", "_stop"])
            |> limit(n:10, offset: 0)
            '''

    result = client.query_api().query(query=query)

    """
    Processing results
    """
    print()
    print("=== Querying 10 pickups from dispatching 'B03404' ===")
    print()
    for table in result:
        for record in table.records:
            print(
                f'Dispatching: {record["dispatching_base_num"]} pickup: {record["pickup_datetime"]} dropoff: {record["dropoff_datetime"]}')
