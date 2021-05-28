"""InfluxDBClient is client for API defined in https://github.com/influxdata/influxdb/blob/master/http/swagger.yml."""

from __future__ import absolute_import

import configparser
import os
import base64

from influxdb_client import Configuration, ApiClient, HealthCheck, HealthService, Ready, ReadyService
from influxdb_client.client.authorizations_api import AuthorizationsApi
from influxdb_client.client.bucket_api import BucketsApi
from influxdb_client.client.delete_api import DeleteApi
from influxdb_client.client.labels_api import LabelsApi
from influxdb_client.client.organizations_api import OrganizationsApi
from influxdb_client.client.query_api import QueryApi, QueryOptions
from influxdb_client.client.tasks_api import TasksApi
from influxdb_client.client.users_api import UsersApi
from influxdb_client.client.write_api import WriteApi, WriteOptions, PointSettings


class InfluxDBClient(object):
    """InfluxDBClient is client for InfluxDB v2."""

    def __init__(self, url, token, debug=None, timeout=10_000, enable_gzip=False, org: str = None,
                 default_tags: dict = None, **kwargs) -> None:
        """
        Initialize defaults.

        :param url: InfluxDB server API url (ex. http://localhost:8086).
        :param token: auth token
        :param debug: enable verbose logging of http requests
        :param timeout: HTTP client timeout setting for a request specified in milliseconds.
                        If one number provided, it will be total request timeout.
                        It can also be a pair (tuple) of (connection, read) timeouts.
        :param enable_gzip: Enable Gzip compression for http requests. Currently only the "Write" and "Query" endpoints
                            supports the Gzip compression.
        :param org: organization name (used as a default in query and write API)
        :key bool verify_ssl: Set this to false to skip verifying SSL certificate when calling API from https server.
        :key str ssl_ca_cert: Set this to customize the certificate file to verify the peer.
        :key str proxy: Set this to configure the http proxy to be used (ex. http://localhost:3128)
        :key int connection_pool_maxsize: Number of connections to save that can be reused by urllib3.
                                          Defaults to "multiprocessing.cpu_count() * 5".
        :key urllib3.util.retry.Retry retries: Set the default retry strategy that is used for all HTTP requests
                                               except batching writes. As a default there is no one retry strategy.
        :key bool auth_basic: Set this to true to enable basic authentication when talking to a InfluxDB 1.8.x that
                              does not use auth-enabled but is protected by a reverse proxy with basic authentication.
                              (defaults to false, don't set to true when talking to InfluxDB 2)
        :key list[str] profilers: list of enabled Flux profilers
        """
        self.url = url
        self.token = token
        self.org = org

        self.default_tags = default_tags

        conf = _Configuration()
        if self.url.endswith("/"):
            conf.host = self.url[:-1]
        else:
            conf.host = self.url
        conf.enable_gzip = enable_gzip
        conf.debug = debug
        conf.verify_ssl = kwargs.get('verify_ssl', True)
        conf.ssl_ca_cert = kwargs.get('ssl_ca_cert', None)
        conf.proxy = kwargs.get('proxy', None)
        conf.connection_pool_maxsize = kwargs.get('connection_pool_maxsize', conf.connection_pool_maxsize)
        conf.timeout = timeout

        auth_token = self.token
        auth_header_name = "Authorization"
        auth_header_value = "Token " + auth_token

        auth_basic = kwargs.get('auth_basic', False)
        if auth_basic:
            auth_header_value = "Basic " + base64.b64encode(token.encode()).decode()

        retries = kwargs.get('retries', False)

        self.profilers = kwargs.get('profilers', None)

        self.api_client = ApiClient(configuration=conf, header_name=auth_header_name,
                                    header_value=auth_header_value, retries=retries)

    def __enter__(self):
        """
        Enter the runtime context related to this object.

        It will bind this method’s return value to the target(s)
        specified in the `as` clause of the statement.

        return: self instance
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the runtime context related to this object and close the client."""
        self.close()

    @classmethod
    def from_config_file(cls, config_file: str = "config.ini", debug=None, enable_gzip=False):
        """
        Configure client via configuration file. The configuration has to be under 'influx' section.

        The supported formats:
            - https://docs.python.org/3/library/configparser.html
            - https://toml.io/en/

        Configuration options:
            - url
            - org
            - token
            - timeout,
            - verify_ssl
            - ssl_ca_cert
            - connection_pool_maxsize
            - auth_basic
            - profilers


        config.ini example::

            [influx2]
            url=http://localhost:8086
            org=my-org
            token=my-token
            timeout=6000
            connection_pool_maxsize=25
            auth_basic=false
            profilers=query,operator

            [tags]
            id = 132-987-655
            customer = California Miner
            data_center = ${env.data_center}

        config.toml example::

            [influx2]
                url = "http://localhost:8086"
                token = "my-token"
                org = "my-org"
                timeout = 6000
                connection_pool_maxsize = 25
                auth_basic = false
                profilers="query, operator"

            [tags]
                id = "132-987-655"
                customer = "California Miner"
                data_center = "${env.data_center}"

        """
        config = configparser.ConfigParser()
        config.read(config_file)

        def config_value(key: str):
            return config['influx2'][key].strip('"')

        url = config_value('url')
        token = config_value('token')

        timeout = None
        if config.has_option('influx2', 'timeout'):
            timeout = config_value('timeout')

        org = None
        if config.has_option('influx2', 'org'):
            org = config_value('org')

        verify_ssl = True
        if config.has_option('influx2', 'verify_ssl'):
            verify_ssl = config_value('verify_ssl')

        ssl_ca_cert = None
        if config.has_option('influx2', 'ssl_ca_cert'):
            ssl_ca_cert = config_value('ssl_ca_cert')

        connection_pool_maxsize = None
        if config.has_option('influx2', 'connection_pool_maxsize'):
            connection_pool_maxsize = config_value('connection_pool_maxsize')

        auth_basic = False
        if config.has_option('influx2', 'auth_basic'):
            auth_basic = config_value('auth_basic')

        default_tags = None
        if config.has_section('tags'):
            tags = {k: v.strip('"') for k, v in config.items('tags')}
            default_tags = dict(tags)

        profilers = None
        if config.has_option('influx2', 'profilers'):
            profilers = [x.strip() for x in config_value('profilers').split(',')]

        return cls(url, token, debug=debug, timeout=_to_int(timeout), org=org, default_tags=default_tags,
                   enable_gzip=enable_gzip, verify_ssl=_to_bool(verify_ssl), ssl_ca_cert=ssl_ca_cert,
                   connection_pool_maxsize=_to_int(connection_pool_maxsize), auth_basic=_to_bool(auth_basic),
                   profilers=profilers)

    @classmethod
    def from_env_properties(cls, debug=None, enable_gzip=False):
        """
        Configure client via environment properties.

        Supported environment properties:
            - INFLUXDB_V2_URL
            - INFLUXDB_V2_ORG
            - INFLUXDB_V2_TOKEN
            - INFLUXDB_V2_TIMEOUT
            - INFLUXDB_V2_VERIFY_SSL
            - INFLUXDB_V2_SSL_CA_CERT
            - INFLUXDB_V2_CONNECTION_POOL_MAXSIZE
            - INFLUXDB_V2_AUTH_BASIC
        """
        url = os.getenv('INFLUXDB_V2_URL', "http://localhost:8086")
        token = os.getenv('INFLUXDB_V2_TOKEN', "my-token")
        timeout = os.getenv('INFLUXDB_V2_TIMEOUT', "10000")
        org = os.getenv('INFLUXDB_V2_ORG', "my-org")
        verify_ssl = os.getenv('INFLUXDB_V2_VERIFY_SSL', "True")
        ssl_ca_cert = os.getenv('INFLUXDB_V2_SSL_CA_CERT', None)
        connection_pool_maxsize = os.getenv('INFLUXDB_V2_CONNECTION_POOL_MAXSIZE', None)
        auth_basic = os.getenv('INFLUXDB_V2_AUTH_BASIC', "False")

        prof = os.getenv("INFLUXDB_V2_PROFILERS", None)
        profilers = None
        if prof is not None:
            profilers = [x.strip() for x in prof.split(',')]

        default_tags = dict()

        for key, value in os.environ.items():
            if key.startswith("INFLUXDB_V2_TAG_"):
                default_tags[key[16:].lower()] = value

        return cls(url, token, debug=debug, timeout=_to_int(timeout), org=org, default_tags=default_tags,
                   enable_gzip=enable_gzip, verify_ssl=_to_bool(verify_ssl), ssl_ca_cert=ssl_ca_cert,
                   connection_pool_maxsize=_to_int(connection_pool_maxsize), auth_basic=_to_bool(auth_basic),
                   profilers=profilers)

    def write_api(self, write_options=WriteOptions(), point_settings=PointSettings()) -> WriteApi:
        """
        Create a Write API instance.

        :param point_settings:
        :param write_options: write api configuration
        :return: write api instance
        """
        return WriteApi(influxdb_client=self, write_options=write_options, point_settings=point_settings)

    def query_api(self, query_options: QueryOptions = QueryOptions()) -> QueryApi:
        """
        Create a Query API instance.

        :param query_options: optional query api configuration
        :return: Query api instance
        """
        return QueryApi(self, query_options)

    def close(self):
        """Shutdown the client."""
        self.__del__()

    def __del__(self):
        """Shutdown the client."""
        if self.api_client:
            self.api_client.__del__()
            self.api_client = None

    def buckets_api(self) -> BucketsApi:
        """
        Create the Bucket API instance.

        :return: buckets api
        """
        return BucketsApi(self)

    def authorizations_api(self) -> AuthorizationsApi:
        """
        Create the Authorizations API instance.

        :return: authorizations api
        """
        return AuthorizationsApi(self)

    def users_api(self) -> UsersApi:
        """
        Create the Users API instance.

        :return: users api
        """
        return UsersApi(self)

    def organizations_api(self) -> OrganizationsApi:
        """
        Create the Organizations API instance.

        :return: organizations api
        """
        return OrganizationsApi(self)

    def tasks_api(self) -> TasksApi:
        """
        Create the Tasks API instance.

        :return: tasks api
        """
        return TasksApi(self)

    def labels_api(self) -> LabelsApi:
        """
        Create the Labels API instance.

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
            return HealthCheck(name="influxdb", message=str(e), status="fail")

    def ready(self) -> Ready:
        """
        Get The readiness of the InfluxDB 2.0.

        :return: Ready
        """
        ready_service = ReadyService(self.api_client)
        return ready_service.get_ready()

    def delete_api(self) -> DeleteApi:
        """
        Get the delete metrics API instance.

        :return: delete api
        """
        return DeleteApi(self)


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


def _to_bool(bool_value):
    return str(bool_value).lower() in ("yes", "true")


def _to_int(int_value):
    return int(int_value) if int_value is not None else None
