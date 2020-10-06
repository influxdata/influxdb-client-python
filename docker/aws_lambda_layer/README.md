## AWS Lambda Layer Docker image
Docker image which allows user to create a zip file with all Python dependencies for a custom AWS Lambda function (by default with influxdb-client). This should be within the limit of 10MB per single browser upload (~3,5MB with just the influxdb-client-python library). If the zip archive is uploaded to a custom Lambda Layer then user is able to keep using the Console IDE for editing main Lambda function.
### Build image:
`docker build -t lambdalayer:latest .`
### Create container:
`docker create --name lambdalayer lambdalayer:latest`
### Copy zip from container:
`docker cp lambdalayer:/install/python.zip .`
### Upload zip to AWS Lambda
Use AWS CLI or AWS Console to create and upload archive to a custom Lambda Layer. Then import those dependencies in lambda function as usual.
```
...
from influxdb_client import InfluxDBClient, Point
...
```
### Reference:
https://docs.aws.amazon.com/lambda/latest/dg/python-package.html#python-package-dependencies
https://docs.aws.amazon.com/lambda/latest/dg/configuration-layers.html#configuration-layers-using