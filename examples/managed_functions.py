"""
How to use Cloud Managed Functions
"""
import datetime

from influxdb_client import InfluxDBClient, FunctionCreateRequest, FunctionLanguage, FunctionInvocationParams
from influxdb_client.service import FunctionsService

"""
Define credentials
"""
influx_cloud_url = 'https://us-west-2-1.aws.cloud2.influxdata.com'
influx_cloud_token = '...'
bucket_name = '...'
org_name = '...'

with InfluxDBClient(url=influx_cloud_url, token=influx_cloud_token, org=org_name, debug=True, timeout=20_000) as client:
    uniqueId = str(datetime.datetime.now())
    """
    Find Organization ID by Organization API.
    """
    org = client.organizations_api().find_organizations(org=org_name)[0]

    functions_service = FunctionsService(api_client=client.api_client)

    """
    Create Managed function
    """
    print(f"------- Create -------\n")
    create_request = FunctionCreateRequest(name=f"my_func_{uniqueId}",
                                           description="my first try",
                                           language=FunctionLanguage.FLUX,
                                           org_id=org.id,
                                           script=f"from(bucket: \"{bucket_name}\") |> range(start: -7d) |> limit(n:2)")

    created_function = functions_service.post_functions(function_create_request=create_request)
    print(created_function)

    """
    Invoke Function
    """
    print(f"\n------- Invoke Function: -------\n")
    response = functions_service.post_functions_id_invoke(function_id=created_function.id,
                                                          function_invocation_params=FunctionInvocationParams(
                                                              params={"bucket_name": bucket_name}))

    """
    List all Functions
    """
    print(f"\n------- Functions: -------\n")
    functions = functions_service.get_functions(org=org).functions
    print("\n".join([f" ---\n ID: {it.id}\n Name: {it.name}\n Description: {it.description}" for it in functions]))
    print("---")

    """
    Delete previously created Function
    """
    print(f"------- Delete -------\n")
    functions_service.delete_functions_id(created_function.id)
    print(f" successfully deleted function: {created_function.name}")
