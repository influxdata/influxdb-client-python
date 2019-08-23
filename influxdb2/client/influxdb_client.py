from __future__ import absolute_import

import influxdb2
from influxdb2 import Configuration
from influxdb2.client.authorizations_api import AuthorizationsApi
from influxdb2.client.bucket_api import BucketsApi
from influxdb2.client.organizations_api import OrganizationsApi
from influxdb2.client.query_api import QueryApi
from influxdb2.client.tasks_api import TasksApi
from influxdb2.client.users_api import UsersApi
from influxdb2.client.write_api import WriteApi, WriteOptions


class InfluxDBClient(object):

    def __init__(self,
                 url,
                 token,
                 auth_scheme='token',
                 username=None,
                 password=None,
                 debug=None,
                 timeout=10000,
                 enable_gzip=False,
                 org=None) -> None:
        """

        :param enable_gzip: Enable Gzip compress for http requests. Currently only the "Write" and "Query" endpoints
                            supports the Gzip compression.
        """
        self.url = url
        self.auth_scheme = auth_scheme
        self.token = token
        self.username = username
        self.password = password
        self.timeout = timeout
        self.org = org

        conf = _Configuration()
        conf.host = self.url
        conf.enable_gzip = enable_gzip
        conf.debug = debug

        auth_token = self.token
        auth_header_name = "Authorization"
        auth_header_value = "Token " + auth_token

        self.api_client = influxdb2.ApiClient(configuration=conf, header_name=auth_header_name,
                                              header_value=auth_header_value)

    def write_api(self, write_options=WriteOptions()):
        service = influxdb2.service.write_service.WriteService(self.api_client)
        return WriteApi(service=service, write_options=write_options)

    def query_api(self):
        return QueryApi(self)

    def close(self):
        self.__del__()

    def __del__(self):
        if self.api_client:
            self.api_client.__del__()
            self.api_client = None

    def buckets_api(self) -> BucketsApi:
        return BucketsApi(self)

    def authorizations_api(self) -> AuthorizationsApi:
        return AuthorizationsApi(self)

    def users_api(self) -> UsersApi:
        return UsersApi(self)

    def organizations_api(self) -> OrganizationsApi:
        return OrganizationsApi(self)

    def tasks_api(self) -> TasksApi:
        return TasksApi(self)


class _Configuration(Configuration):

    def __init__(self):
        Configuration.__init__(self)
        self.enable_gzip = False

    def update_request_header_params(self, path: str, params: dict):
        super().update_request_header_params(path, params)
        if self.enable_gzip:
            # GZIP Request
            if path == '/write':
                params["Content-Encoding"] = "gzip"
                params["Accept-Encoding"] = "identity"
                pass
            # GZIP Response
            if path == '/query':
                # params["Content-Encoding"] = "gzip"
                params["Accept-Encoding"] = "gzip"
                pass
            pass
        pass

    def update_request_body(self, path: str, body):
        _body = super().update_request_body(path, body)
        if self.enable_gzip:
            # GZIP Request
            if path == '/write':
                import gzip
                return gzip.compress(bytes(_body, "utf-8"))
        return _body

