"""Commons function for Sync and Async client."""
from __future__ import absolute_import

import base64
import codecs
import csv
from datetime import datetime, timedelta
from typing import Iterator, List, Generator, Any, Union, Iterable, AsyncGenerator

from urllib3 import HTTPResponse

from influxdb_client import Configuration, Dialect, Query, OptionStatement, VariableAssignment, Identifier, \
    Expression, BooleanLiteral, IntegerLiteral, FloatLiteral, DateTimeLiteral, UnaryExpression, DurationLiteral, \
    Duration, StringLiteral, ArrayExpression, ImportDeclaration, MemberExpression, MemberAssignment, File, \
    WriteService, QueryService
from influxdb_client.client.flux_csv_parser import FluxResponseMetadataMode, FluxCsvParser, FluxSerializationMode, \
    _CSV_ENCODING
from influxdb_client.client.flux_table import FluxTable, FluxRecord
from influxdb_client.client.util.date_utils import get_date_helper
from influxdb_client.client.util.helpers import get_org_query_param
from influxdb_client.client.write.dataframe_serializer import DataframeSerializer

try:
    import dataclasses

    _HAS_DATACLASS = True
except ModuleNotFoundError:
    _HAS_DATACLASS = False


# noinspection PyMethodMayBeStatic
class _BaseClient(object):
    def __init__(self, url, token, debug=None, timeout=10_000, enable_gzip=False, org: str = None,
                 default_tags: dict = None, **kwargs) -> None:
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
        self.conf.debug = debug
        self.conf.verify_ssl = kwargs.get('verify_ssl', True)
        self.conf.ssl_ca_cert = kwargs.get('ssl_ca_cert', None)
        self.conf.proxy = kwargs.get('proxy', None)
        self.conf.proxy_headers = kwargs.get('proxy_headers', None)
        self.conf.connection_pool_maxsize = kwargs.get('connection_pool_maxsize', self.conf.connection_pool_maxsize)
        self.conf.timeout = timeout

        auth_token = self.token
        self.auth_header_name = "Authorization"
        self.auth_header_value = "Token " + auth_token

        auth_basic = kwargs.get('auth_basic', False)
        if auth_basic:
            self.auth_header_value = "Basic " + base64.b64encode(token.encode()).decode()

        self.retries = kwargs.get('retries', False)

        self.profilers = kwargs.get('profilers', None)
        pass

    def _version(self, response) -> str:
        if response is not None and len(response) >= 3:
            if 'X-Influxdb-Version' in response[2]:
                return response[2]['X-Influxdb-Version']

        return "unknown"


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
                   FluxResponseMetadataMode = FluxResponseMetadataMode.full) -> List[FluxTable]:
        """
        Parse HTTP response to FluxTables.

        :param response: HTTP response from an HTTP client. Expected type: `urllib3.response.HTTPResponse`.
        """
        _parser = self._to_tables_parser(response, query_options, response_metadata_mode)
        list(_parser.generator())
        return _parser.table_list()

    async def _to_tables_async(self, response, query_options=None, response_metadata_mode:
                               FluxResponseMetadataMode = FluxResponseMetadataMode.full) -> List[FluxTable]:
        """
        Parse HTTP response to FluxTables.

        :param response: HTTP response from an HTTP client. Expected type: `aiohttp.client_reqrep.ClientResponse`.
        """
        async with self._to_tables_parser(response, query_options, response_metadata_mode) as parser:
            async for _ in parser.generator_async():
                pass
            return parser.table_list()

    def _to_csv(self, response: HTTPResponse) -> Iterator[List[str]]:
        """Parse HTTP response to CSV."""
        return csv.reader(codecs.iterdecode(response, _CSV_ENCODING))

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

    def _create_query(self, query, dialect=default_dialect, params: dict = None):
        query_options = self._get_query_options()
        profilers = query_options.profilers if query_options is not None else None
        q = Query(query=query, dialect=dialect, extern=_BaseQueryApi._build_flux_ast(params, profilers))

        if profilers:
            print("\n===============")
            print("Profiler: query")
            print("===============")
            print(query)

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
            return DateTimeLiteral("DateTimeLiteral", value.strftime('%Y-%m-%dT%H:%M:%S.%fZ'))
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
            self._serialize(record.encode("utf-8"), write_precision, payload, **kwargs)

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
