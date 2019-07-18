from __future__ import absolute_import

import influxdb2
from influxdb2.client.bucket_client import BucketsClient
from influxdb2.client.query_client import QueryClient
from influxdb2.client.write_api import WriteApiClient


class InfluxDBClient(object):

    def __init__(self,
                 url,
                 token,
                 auth_scheme='token',
                 username=None,
                 password=None,
                 debug=None,
                 timeout=10000, org=None) -> None:
        self.url = url
        self.auth_scheme = auth_scheme
        self.token = token
        self.username = username
        self.password = password
        self.timeout = timeout
        self.org = org

        conf = influxdb2.configuration.Configuration()
        conf.host = self.url
        conf.debug = debug

        auth_token = self.token
        auth_header_name = "Authorization"
        auth_header_value = "Token " + auth_token

        self.api_client = influxdb2.ApiClient(configuration=conf, header_name=auth_header_name,
                                              header_value=auth_header_value)

    def find_my_org(self):
        org_api = influxdb2.api.organizations_api.OrganizationsApi(self.api_client)
        orgs = org_api.get_orgs()
        for org in orgs.orgs:
            if org.name == self.org:
                return org

        return None

    def write_client(self):
        service = influxdb2.api.write_api.WriteApi(self.api_client)
        return WriteApiClient(service=service)
        # return

    def query_client(self):
        return QueryClient(self)

    def __del__(self):
        self.api_client.__del__()

    def buckets_client(self):
        return BucketsClient(self)
