from influxdb_client import InfluxDBClient, TaskCreateRequest

url = "http://localhost:8086"
org = "my-org"
bucket = "my-bucket"
token = "my-token"

with InfluxDBClient(url=url, token=token, org=org, debug=True) as client:
    tasks_api = client.tasks_api()

    flux = \
        '''
  option task = {{
    name: "{task_name}",
    every: 1d
  }}
  
  from(bucket: "{from_bucket}") 
    |> range(start: -task.every) 
    |> filter(fn: (r) => (r._measurement == "m")) 
    |> aggregateWindow(every: 1h, fn: mean) 
    |> to(bucket: "{to_bucket}", org: "{org}")
'''.format(task_name="my-task", from_bucket=bucket, to_bucket="to-my-bucket", org=org)

    task_request = TaskCreateRequest(flux=flux, org=org, description="Task Description", status="active")
    task = tasks_api.create_task(task_create_request=task_request)
    print(task)

    tasks = tasks_api.find_tasks_iter()

    # print all tasks id
    for task in tasks:
        print(task.id)
