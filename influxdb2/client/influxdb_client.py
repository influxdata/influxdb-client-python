from __future__ import absolute_import

from influxdb2 import Configuration, ApiClient, HealthCheck, HealthService, Ready, ReadyService
from influxdb2.client.authorizations_api import AuthorizationsApi
from influxdb2.client.bucket_api import BucketsApi
from influxdb2.client.labels_api import LabelsApi
from influxdb2.client.organizations_api import OrganizationsApi
from influxdb2.client.query_api import QueryApi
from influxdb2.client.tasks_api import TasksApi
from influxdb2.client.users_api import UsersApi
from influxdb2.client.write_api import WriteApi, WriteOptions


class InfluxDBClient(object):

    def __init__(self, url, token, debug=None, timeout=10000, enable_gzip=False, org: str = None) -> None:
        """
        Creates a new client instance
        :param url: InfluxDB server API url (ex. http://localhost:9999/api/v2)
        :param token: auth token
        :param debug: enable verbose logging of http requests
        :param timeout: default http client timeout
        :param enable_gzip: Enable Gzip compress for http requests. Currently only the "Write" and "Query" endpoints
                            supports the Gzip compression.
        :param org: organization name (used as a default in query and write API)

        """
        self.url = url
        self.token = token
        self.timeout = timeout
        self.org = org

        conf = _Configuration()
        conf.host = self.url
        conf.enable_gzip = enable_gzip
        conf.debug = debug

        auth_token = self.token
        auth_header_name = "Authorization"
        auth_header_value = "Token " + auth_token

        self.api_client = ApiClient(configuration=conf, header_name=auth_header_name,
                                    header_value=auth_header_value)

    def write_api(self, write_options=WriteOptions()) -> WriteApi:
        """
        Creates a Write API instance
        :param write_options: write api configuration
        :return: write api instance
        """
        return WriteApi(influxdb_client=self, write_options=write_options)

    def query_api(self) -> QueryApi:
        """
        Creates a Query API instance
        :return: Query api instance
        """
        return QueryApi(self)

    def close(self):
        """
        Shutdowns the client
        """
        self.__del__()

    def __del__(self):
        if self.api_client:
            self.api_client.__del__()
            self.api_client = None

    def buckets_api(self) -> BucketsApi:
        """
        Creates the Bucket API instance
        :return: buckets api
        """
        return BucketsApi(self)

    def authorizations_api(self) -> AuthorizationsApi:
        """
        Creates the Authorizations API instance
        :return: authorizations api
        """
        return AuthorizationsApi(self)

    def users_api(self) -> UsersApi:
        """
        Creates the Users api
        :return: users api
        """
        return UsersApi(self)

    def organizations_api(self) -> OrganizationsApi:
        """
        Creates the Organizations api
        :return: organizations api
        """
        return OrganizationsApi(self)

    def tasks_api(self) -> TasksApi:
        """
        Creates the Tasks api
        :return: tasks api
        """
        return TasksApi(self)

    def labels_api(self) -> LabelsApi:
        """
        Creates the Labels api
        :return: labels api
        """
        return LabelsApi(self)

    def health(self) -> HealthCheck:
        """
        Get the health of an instance
        :return:
        """
        health_service = HealthService(self.api_client)
        return health_service.get_health()

    def ready(self) -> Ready:
        """
        Gets The readiness of the InfluxDB 2.0.
        :return:
        """
        ready_service = ReadyService(api_client=self)
        return ready_service.get_ready()



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
                if isinstance(_body, bytes):
                    return gzip.compress(data=_body)
                else:
                    return gzip.compress(bytes(_body, "utf-8"))

        return _body
