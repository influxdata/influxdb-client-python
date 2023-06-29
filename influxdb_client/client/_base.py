"""Commons function for Sync and Async client."""
from __future__ import absolute_import

import base64
import configparser
import logging
import os
from datetime import datetime, timedelta
from typing import List, Generator, Any, Union, Iterable, AsyncGenerator

from urllib3 import HTTPResponse

from influxdb_client import Configuration, Dialect, Query, OptionStatement, VariableAssignment, Identifier, \
    Expression, BooleanLiteral, IntegerLiteral, FloatLiteral, DateTimeLiteral, UnaryExpression, DurationLiteral, \
    Duration, StringLiteral, ArrayExpression, ImportDeclaration, MemberExpression, MemberAssignment, File, \
    WriteService, QueryService, DeleteService, DeletePredicateRequest
from influxdb_client.client.flux_csv_parser import FluxResponseMetadataMode, FluxCsvParser, FluxSerializationMode
from influxdb_client.client.flux_table import FluxRecord, TableList, CSVIterator
from influxdb_client.client.util.date_utils import get_date_helper
from influxdb_client.client.util.helpers import get_org_query_param
from influxdb_client.client.warnings import MissingPivotFunction
from influxdb_client.client.write.dataframe_serializer import DataframeSerializer
from influxdb_client.rest import _UTF_8_encoding

try:
    import dataclasses

    _HAS_DATACLASS = True
except ModuleNotFoundError:
    _HAS_DATACLASS = False

LOGGERS_NAMES = [
    'influxdb_client.client.influxdb_client',
    'influxdb_client.client.influxdb_client_async',
    'influxdb_client.client.write_api',
    'influxdb_client.client.write_api_async',
    'influxdb_client.client.write.retry',
    'influxdb_client.client.write.dataframe_serializer',
    'influxdb_client.client.util.multiprocessing_helper',
    'influxdb_client.client.http',
    'influxdb_client.client.exceptions',
]


# noinspection PyMethodMayBeStatic
class _BaseClient(object):
    def __init__(self, url, token, debug=None, timeout=10_000, enable_gzip=False, org: str = None,
                 default_tags: dict = None, http_client_logger: str = None, **kwargs) -> None:
        self.url = url
        self.token = token
        self.org = org

        self.default_tags = default_tags

        self.conf = _Configuration()
        if self.url.endswith("/"):
            self.conf.host = self.url[:-1]
        else:
            self.conf.host = self.url
        self.conf.enable_gzip = enable_gzip
        self.conf.verify_ssl = kwargs.get('verify_ssl', True)
        self.conf.ssl_ca_cert = kwargs.get('ssl_ca_cert', None)
        self.conf.cert_file = kwargs.get('cert_file', None)
        self.conf.cert_key_file = kwargs.get('cert_key_file', None)
        self.conf.cert_key_password = kwargs.get('cert_key_password', None)
        self.conf.ssl_context = kwargs.get('ssl_context', None)
        self.conf.proxy = kwargs.get('proxy', None)
        self.conf.proxy_headers = kwargs.get('proxy_headers', None)
        self.conf.connection_pool_maxsize = kwargs.get('connection_pool_maxsize', self.conf.connection_pool_maxsize)
        self.conf.timeout = timeout
        # logging
        self.conf.loggers["http_client_logger"] = logging.getLogger(http_client_logger)
        for client_logger in LOGGERS_NAMES:
            self.conf.loggers[client_logger] = logging.getLogger(client_logger)
        self.conf.debug = debug

        self.conf.username = kwargs.get('username', None)
        self.conf.password = kwargs.get('password', None)
        # defaults
        self.auth_header_name = None
        self.auth_header_value = None
        # by token
        if self.token:
            self.auth_header_name = "Authorization"
            self.auth_header_value = "Token " + self.token
        # by HTTP basic
        auth_basic = kwargs.get('auth_basic', False)
        if auth_basic:
            self.auth_header_name = "Authorization"
            self.auth_header_value = "Basic " + base64.b64encode(token.encode()).decode()
        # by username, password
        if self.conf.username and self.conf.password:
            self.auth_header_name = None
            self.auth_header_value = None

        self.retries = kwargs.get('retries', False)

        self.profilers = kwargs.get('profilers', None)
        pass

    @classmethod
    def _from_config_file(cls, config_file: str = "config.ini", debug=None, enable_gzip=False, **kwargs):
        config = configparser.ConfigParser()
        config_name = kwargs.get('config_name', 'influx2')
        is_json = False
        try:
            config.read(config_file)
        except configparser.ParsingError:
            with open(config_file) as json_file:
                import json
                config = json.load(json_file)
                is_json = True

        def _config_value(key: str):
            value = str(config[key]) if is_json else config[config_name][key]
            return value.strip('"')

        def _has_option(key: str):
            return key in config if is_json else config.has_option(config_name, key)

        def _has_section(key: str):
            return key in config if is_json else config.has_section(key)

        url = _config_value('url')
        token = _config_value('token')

        timeout = None
        if _has_option('timeout'):
            timeout = _config_value('timeout')

        org = None
        if _has_option('org'):
            org = _config_value('org')

        verify_ssl = True
        if _has_option('verify_ssl'):
            verify_ssl = _config_value('verify_ssl')

        ssl_ca_cert = None
        if _has_option('ssl_ca_cert'):
            ssl_ca_cert = _config_value('ssl_ca_cert')

        cert_file = None
        if _has_option('cert_file'):
            cert_file = _config_value('cert_file')

        cert_key_file = None
        if _has_option('cert_key_file'):
            cert_key_file = _config_value('cert_key_file')

        cert_key_password = None
        if _has_option('cert_key_password'):
            cert_key_password = _config_value('cert_key_password')

        connection_pool_maxsize = None
        if _has_option('connection_pool_maxsize'):
            connection_pool_maxsize = _config_value('connection_pool_maxsize')

        auth_basic = False
        if _has_option('auth_basic'):
            auth_basic = _config_value('auth_basic')

        default_tags = None
        if _has_section('tags'):
            if is_json:
                default_tags = config['tags']
            else:
                tags = {k: v.strip('"') for k, v in config.items('tags')}
                default_tags = dict(tags)

        profilers = None
        if _has_option('profilers'):
            profilers = [x.strip() for x in _config_value('profilers').split(',')]

        proxy = None
        if _has_option('proxy'):
            proxy = _config_value('proxy')

        return cls(url, token, debug=debug, timeout=_to_int(timeout), org=org, default_tags=default_tags,
                   enable_gzip=enable_gzip, verify_ssl=_to_bool(verify_ssl), ssl_ca_cert=ssl_ca_cert,
                   cert_file=cert_file, cert_key_file=cert_key_file, cert_key_password=cert_key_password,
                   connection_pool_maxsize=_to_int(connection_pool_maxsize), auth_basic=_to_bool(auth_basic),
                   profilers=profilers, proxy=proxy, **kwargs)

    @classmethod
    def _from_env_properties(cls, debug=None, enable_gzip=False, **kwargs):
        url = os.getenv('INFLUXDB_V2_URL', "http://localhost:8086")
        token = os.getenv('INFLUXDB_V2_TOKEN', "my-token")
        timeout = os.getenv('INFLUXDB_V2_TIMEOUT', "10000")
        org = os.getenv('INFLUXDB_V2_ORG', "my-org")
        verify_ssl = os.getenv('INFLUXDB_V2_VERIFY_SSL', "True")
        ssl_ca_cert = os.getenv('INFLUXDB_V2_SSL_CA_CERT', None)
        cert_file = os.getenv('INFLUXDB_V2_CERT_FILE', None)
        cert_key_file = os.getenv('INFLUXDB_V2_CERT_KEY_FILE', None)
        cert_key_password = os.getenv('INFLUXDB_V2_CERT_KEY_PASSWORD', None)
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
                   cert_file=cert_file, cert_key_file=cert_key_file, cert_key_password=cert_key_password,
                   connection_pool_maxsize=_to_int(connection_pool_maxsize), auth_basic=_to_bool(auth_basic),
                   profilers=profilers, **kwargs)


# noinspection PyMethodMayBeStatic
class _BaseQueryApi(object):
    default_dialect = Dialect(header=True, delimiter=",", comment_prefix="#",
                              annotations=["datatype", "group", "default"], date_time_format="RFC3339")

    def __init__(self, influxdb_client, query_options=None):
        from influxdb_client.client.query_api import QueryOptions
        self._query_options = QueryOptions() if query_options is None else query_options
        self._influxdb_client = influxdb_client
        self._query_api = QueryService(influxdb_client.api_client)

    """Base implementation for Queryable API."""

    def _to_tables(self, response, query_options=None, response_metadata_mode:
                   FluxResponseMetadataMode = FluxResponseMetadataMode.full) -> TableList:
        """
        Parse HTTP response to TableList.

        :param response: HTTP response from an HTTP client. Expected type: `urllib3.response.HTTPResponse`.
        """
        _parser = self._to_tables_parser(response, query_options, response_metadata_mode)
        list(_parser.generator())
        return _parser.table_list()

    async def _to_tables_async(self, response, query_options=None, response_metadata_mode:
                               FluxResponseMetadataMode = FluxResponseMetadataMode.full) -> TableList:
        """
        Parse HTTP response to TableList.

        :param response: HTTP response from an HTTP client. Expected type: `aiohttp.client_reqrep.ClientResponse`.
        """
        async with self._to_tables_parser(response, query_options, response_metadata_mode) as parser:
            async for _ in parser.generator_async():
                pass
            return parser.table_list()

    def _to_csv(self, response: HTTPResponse) -> CSVIterator:
        """Parse HTTP response to CSV."""
        return CSVIterator(response)

    def _to_flux_record_stream(self, response, query_options=None,
                               response_metadata_mode: FluxResponseMetadataMode = FluxResponseMetadataMode.full) -> \
            Generator[FluxRecord, Any, None]:
        """
        Parse HTTP response to FluxRecord stream.

        :param response: HTTP response from an HTTP client. Expected type: `urllib3.response.HTTPResponse`.
        """
        _parser = self._to_flux_record_stream_parser(query_options, response, response_metadata_mode)
        return _parser.generator()

    async def _to_flux_record_stream_async(self, response, query_options=None, response_metadata_mode:
                                           FluxResponseMetadataMode = FluxResponseMetadataMode.full) -> \
            AsyncGenerator['FluxRecord', None]:
        """
        Parse HTTP response to FluxRecord stream.

        :param response: HTTP response from an HTTP client. Expected type: `aiohttp.client_reqrep.ClientResponse`.
        """
        _parser = self._to_flux_record_stream_parser(query_options, response, response_metadata_mode)
        return (await _parser.__aenter__()).generator_async()

    def _to_data_frame_stream(self, data_frame_index, response, query_options=None,
                              response_metadata_mode: FluxResponseMetadataMode = FluxResponseMetadataMode.full):
        """
        Parse HTTP response to DataFrame stream.

        :param response: HTTP response from an HTTP client. Expected type: `urllib3.response.HTTPResponse`.
        """
        _parser = self._to_data_frame_stream_parser(data_frame_index, query_options, response, response_metadata_mode)
        return _parser.generator()

    async def _to_data_frame_stream_async(self, data_frame_index, response, query_options=None, response_metadata_mode:
                                          FluxResponseMetadataMode = FluxResponseMetadataMode.full):
        """
        Parse HTTP response to DataFrame stream.

        :param response: HTTP response from an HTTP client. Expected type: `aiohttp.client_reqrep.ClientResponse`.
        """
        _parser = self._to_data_frame_stream_parser(data_frame_index, query_options, response, response_metadata_mode)
        return (await _parser.__aenter__()).generator_async()

    def _to_tables_parser(self, response, query_options, response_metadata_mode):
        return FluxCsvParser(response=response, serialization_mode=FluxSerializationMode.tables,
                             query_options=query_options, response_metadata_mode=response_metadata_mode)

    def _to_flux_record_stream_parser(self, query_options, response, response_metadata_mode):
        return FluxCsvParser(response=response, serialization_mode=FluxSerializationMode.stream,
                             query_options=query_options, response_metadata_mode=response_metadata_mode)

    def _to_data_frame_stream_parser(self, data_frame_index, query_options, response, response_metadata_mode):
        return FluxCsvParser(response=response, serialization_mode=FluxSerializationMode.dataFrame,
                             data_frame_index=data_frame_index, query_options=query_options,
                             response_metadata_mode=response_metadata_mode)

    def _to_data_frames(self, _generator):
        """Parse stream of DataFrames into expected type."""
        from ..extras import pd
        if isinstance(_generator, list):
            _dataFrames = _generator
        else:
            _dataFrames = list(_generator)

        if len(_dataFrames) == 0:
            return pd.DataFrame(columns=[], index=None)
        elif len(_dataFrames) == 1:
            return _dataFrames[0]
        else:
            return _dataFrames

    def _org_param(self, org):
        return get_org_query_param(org=org, client=self._influxdb_client)

    def _get_query_options(self):
        if self._query_options and self._query_options.profilers:
            return self._query_options
        elif self._influxdb_client.profilers:
            from influxdb_client.client.query_api import QueryOptions
            return QueryOptions(profilers=self._influxdb_client.profilers)

    def _create_query(self, query, dialect=default_dialect, params: dict = None, **kwargs):
        query_options = self._get_query_options()
        profilers = query_options.profilers if query_options is not None else None
        q = Query(query=query, dialect=dialect, extern=_BaseQueryApi._build_flux_ast(params, profilers))

        if profilers:
            print("\n===============")
            print("Profiler: query")
            print("===============")
            print(query)

        if kwargs.get('dataframe_query', False):
            MissingPivotFunction.print_warning(query)

        return q

    @staticmethod
    def _params_to_extern_ast(params: dict) -> List['OptionStatement']:

        statements = []
        for key, value in params.items():
            expression = _BaseQueryApi._parm_to_extern_ast(value)
            if expression is None:
                continue

            statements.append(OptionStatement("OptionStatement",
                                              VariableAssignment("VariableAssignment", Identifier("Identifier", key),
                                                                 expression)))
        return statements

    @staticmethod
    def _parm_to_extern_ast(value) -> Union[Expression, None]:
        if value is None:
            return None
        if isinstance(value, bool):
            return BooleanLiteral("BooleanLiteral", value)
        elif isinstance(value, int):
            return IntegerLiteral("IntegerLiteral", str(value))
        elif isinstance(value, float):
            return FloatLiteral("FloatLiteral", value)
        elif isinstance(value, datetime):
            value = get_date_helper().to_utc(value)
            nanoseconds = getattr(value, 'nanosecond', 0)
            fraction = f'{(value.microsecond * 1000 + nanoseconds):09d}'
            return DateTimeLiteral("DateTimeLiteral", value.strftime('%Y-%m-%dT%H:%M:%S.') + fraction + 'Z')
        elif isinstance(value, timedelta):
            _micro_delta = int(value / timedelta(microseconds=1))
            if _micro_delta < 0:
                return UnaryExpression("UnaryExpression", argument=DurationLiteral("DurationLiteral", [
                    Duration(magnitude=-_micro_delta, unit="us")]), operator="-")
            else:
                return DurationLiteral("DurationLiteral", [Duration(magnitude=_micro_delta, unit="us")])
        elif isinstance(value, str):
            return StringLiteral("StringLiteral", str(value))
        elif isinstance(value, Iterable):
            return ArrayExpression("ArrayExpression",
                                   elements=list(map(lambda it: _BaseQueryApi._parm_to_extern_ast(it), value)))
        else:
            return value

    @staticmethod
    def _build_flux_ast(params: dict = None, profilers: List[str] = None):

        imports = []
        body = []

        if profilers is not None and len(profilers) > 0:
            imports.append(ImportDeclaration(
                "ImportDeclaration",
                path=StringLiteral("StringLiteral", "profiler")))

            elements = []
            for profiler in profilers:
                elements.append(StringLiteral("StringLiteral", value=profiler))

            member = MemberExpression(
                "MemberExpression",
                object=Identifier("Identifier", "profiler"),
                _property=Identifier("Identifier", "enabledProfilers"))

            prof = OptionStatement(
                "OptionStatement",
                assignment=MemberAssignment(
                    "MemberAssignment",
                    member=member,
                    init=ArrayExpression(
                        "ArrayExpression",
                        elements=elements)))

            body.append(prof)

        if params is not None:
            body.extend(_BaseQueryApi._params_to_extern_ast(params))

        return File(package=None, name=None, type=None, imports=imports, body=body)


class _BaseWriteApi(object):
    def __init__(self, influxdb_client, point_settings=None):
        self._influxdb_client = influxdb_client
        self._point_settings = point_settings
        self._write_service = WriteService(influxdb_client.api_client)
        if influxdb_client.default_tags:
            for key, value in influxdb_client.default_tags.items():
                self._point_settings.add_default_tag(key, value)

    def _append_default_tag(self, key, val, record):
        from influxdb_client import Point
        if isinstance(record, bytes) or isinstance(record, str):
            pass
        elif isinstance(record, Point):
            record.tag(key, val)
        elif isinstance(record, dict):
            record.setdefault("tags", {})
            record.get("tags")[key] = val
        elif isinstance(record, Iterable):
            for item in record:
                self._append_default_tag(key, val, item)

    def _append_default_tags(self, record):
        if self._point_settings.defaultTags and record is not None:
            for key, val in self._point_settings.defaultTags.items():
                self._append_default_tag(key, val, record)

    def _serialize(self, record, write_precision, payload, **kwargs):
        from influxdb_client import Point
        if isinstance(record, bytes):
            payload[write_precision].append(record)

        elif isinstance(record, str):
            self._serialize(record.encode(_UTF_8_encoding), write_precision, payload, **kwargs)

        elif isinstance(record, Point):
            precision_from_point = kwargs.get('precision_from_point', True)
            precision = record.write_precision if precision_from_point else write_precision
            self._serialize(record.to_line_protocol(precision=precision), precision, payload, **kwargs)

        elif isinstance(record, dict):
            self._serialize(Point.from_dict(record, write_precision=write_precision, **kwargs),
                            write_precision, payload, **kwargs)
        elif 'DataFrame' in type(record).__name__:
            serializer = DataframeSerializer(record, self._point_settings, write_precision, **kwargs)
            self._serialize(serializer.serialize(), write_precision, payload, **kwargs)
        elif hasattr(record, "_asdict"):
            # noinspection PyProtectedMember
            self._serialize(record._asdict(), write_precision, payload, **kwargs)
        elif _HAS_DATACLASS and dataclasses.is_dataclass(record):
            self._serialize(dataclasses.asdict(record), write_precision, payload, **kwargs)
        elif isinstance(record, Iterable):
            for item in record:
                self._serialize(item, write_precision, payload, **kwargs)


# noinspection PyMethodMayBeStatic
class _BaseDeleteApi(object):
    def __init__(self, influxdb_client):
        self._influxdb_client = influxdb_client
        self._service = DeleteService(influxdb_client.api_client)

    def _prepare_predicate_request(self, start, stop, predicate):
        date_helper = get_date_helper()
        if isinstance(start, datetime):
            start = date_helper.to_utc(start)
        if isinstance(stop, datetime):
            stop = date_helper.to_utc(stop)
        predicate_request = DeletePredicateRequest(start=start, stop=stop, predicate=predicate)
        return predicate_request


class _Configuration(Configuration):
    def __init__(self):
        Configuration.__init__(self)
        self.enable_gzip = False
        self.username = None
        self.password = None

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
                    return gzip.compress(bytes(_body, _UTF_8_encoding))

        return _body


def _to_bool(bool_value):
    return str(bool_value).lower() in ("yes", "true")


def _to_int(int_value):
    return int(int_value) if int_value is not None else None
