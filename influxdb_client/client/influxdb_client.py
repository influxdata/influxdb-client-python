from __future__ import absolute_import

from influxdb_client import Configuration, ApiClient, HealthCheck, HealthService, Ready, ReadyService
from influxdb_client.client.authorizations_api import AuthorizationsApi
from influxdb_client.client.bucket_api import BucketsApi
from influxdb_client.client.labels_api import LabelsApi
from influxdb_client.client.organizations_api import OrganizationsApi
from influxdb_client.client.query_api import QueryApi
from influxdb_client.client.tasks_api import TasksApi
from influxdb_client.client.users_api import UsersApi
from influxdb_client.client.write_api import WriteApi, WriteOptions


class InfluxDBClient(object):

    def __init__(self, url, token, debug=None, timeout=10000, enable_gzip=False, org: str = None) -> None:
        """
        :class:`influxdb_client.InfluxDBClient` is client for HTTP API defined
        in https://github.com/influxdata/influxdb/blob/master/http/swagger.yml.

        :param url: InfluxDB server API url (ex. http://localhost:9999).
        :param token: auth token
        :param debug: enable verbose logging of http requests
        :param timeout: default http client timeout
        :param enable_gzip: Enable Gzip compression for http requests. Currently only the "Write" and "Query" endpoints
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
        Creates the Bucket API instance.

        :return: buckets api
        """
        return BucketsApi(self)

    def authorizations_api(self) -> AuthorizationsApi:
        """
        Creates the Authorizations API instance.

        :return: authorizations api
        """
        return AuthorizationsApi(self)

    def users_api(self) -> UsersApi:
        """
        Creates the Users API instance.

        :return: users api
        """
        return UsersApi(self)

    def organizations_api(self) -> OrganizationsApi:
        """
        Creates the Organizations API instance.

        :return: organizations api
        """
        return OrganizationsApi(self)

    def tasks_api(self) -> TasksApi:
        """
        Creates the Tasks API instance.

        :return: tasks api
        """
        return TasksApi(self)

    def labels_api(self) -> LabelsApi:
        """
        Creates the Labels API instance.

        :return: labels api
        """
        return LabelsApi(self)

    def health(self) -> HealthCheck:
        """
        Get the health of an instance.

        :return: HealthCheck
        """
        health_service = HealthService(self.api_client)

        try:
            health = health_service.get_health()
            return health
        except Exception as e:
            print(e)
            return HealthCheck(name="influxdb", message=str(e), status="fail")

    def ready(self) -> Ready:
        """
        Gets The readiness of the InfluxDB 2.0.

        :return: Ready
        """
        ready_service = ReadyService(self.api_client)
        return ready_service.get_ready()


class _Configuration(Configuration):
    def __init__(self):
        Configuration.__init__(self)
        self.enable_gzip = False

    def update_request_header_params(self, path: str, params: dict):
        super().update_request_header_params(path, params)
        if self.enable_gzip:
            # GZIP Request
            if path == '/api/v2/write':
                params["Content-Encoding"] = "gzip"
                params["Accept-Encoding"] = "identity"
                pass
            # GZIP Response
            if path == '/api/v2/query':
                # params["Content-Encoding"] = "gzip"
                params["Accept-Encoding"] = "gzip"
                pass
            pass
        pass

    def update_request_body(self, path: str, body):
        _body = super().update_request_body(path, body)
        if self.enable_gzip:
            # GZIP Request
            if path == '/api/v2/write':
                import gzip
                if isinstance(_body, bytes):
                    return gzip.compress(data=_body)
                else:
                    return gzip.compress(bytes(_body, "utf-8"))

        return _body
