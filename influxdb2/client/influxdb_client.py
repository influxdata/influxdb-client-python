from __future__ import absolute_import

import influxdb2
from influxdb2.client.authorizations_api import AuthorizationsApi
from influxdb2.client.bucket_api import BucketsApi
from influxdb2.client.organizations_api import OrganizationsApi
from influxdb2.client.query_api import QueryApi
from influxdb2.client.users_api import UsersApi
from influxdb2.client.write_api import WriteApiClient, WriteOptions


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

    def write_api(self, write_options=WriteOptions()):
        service = influxdb2.service.write_service.WriteService(self.api_client)
        return WriteApiClient(service=service, write_options=write_options)

    def query_api(self):
        return QueryApi(self)

    def __del__(self):
        self.api_client.__del__()

    def buckets_api(self) -> BucketsApi:
        return BucketsApi(self)

    def authorizations_api(self) -> AuthorizationsApi:
        return AuthorizationsApi(self)

    def users_api(self) -> UsersApi:
        return UsersApi(self)

    def organizations_api(self) -> OrganizationsApi:
        return OrganizationsApi(self)
