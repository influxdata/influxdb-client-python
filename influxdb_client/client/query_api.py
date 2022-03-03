"""
Querying InfluxDB bu FluxLang.

Flux is InfluxData’s functional data scripting language designed for querying, analyzing, and acting on data.
"""

from datetime import datetime, timedelta
from typing import List, Generator, Any, Union, Iterable, Callable

from influxdb_client import Dialect, IntegerLiteral, BooleanLiteral, FloatLiteral, DateTimeLiteral, StringLiteral, \
    VariableAssignment, Identifier, OptionStatement, File, DurationLiteral, Duration, UnaryExpression, Expression, \
    ImportDeclaration, MemberAssignment, MemberExpression, ArrayExpression
from influxdb_client import Query, QueryService
from influxdb_client.client.flux_table import FluxTable, FluxRecord
from influxdb_client.client.queryable_api import QueryableApi
from influxdb_client.client.util.date_utils import get_date_helper
from influxdb_client.client.util.helpers import get_org_query_param


class QueryOptions(object):
    """Query options."""

    def __init__(self, profilers: List[str] = None, profiler_callback: Callable = None) -> None:
        """
        Initialize query options.

        :param profilers: list of enabled flux profilers
        :param profiler_callback: callback function return profilers (FluxRecord)
        """
        self.profilers = profilers
        self.profiler_callback = profiler_callback


class QueryApi(QueryableApi):
    """Implementation for '/api/v2/query' endpoint."""

    default_dialect = Dialect(header=True, delimiter=",", comment_prefix="#",
                              annotations=["datatype", "group", "default"], date_time_format="RFC3339")

    def __init__(self, influxdb_client, query_options=QueryOptions()):
        """
        Initialize query client.

        :param influxdb_client: influxdb client
        """
        self._influxdb_client = influxdb_client
        self._query_options = query_options
        self._query_api = QueryService(influxdb_client.api_client)

    def query_csv(self, query: str, org=None, dialect: Dialect = default_dialect, params: dict = None):
        """
        Execute the Flux query and return results as a CSV iterator. Each iteration returns a row of the CSV file.

        :param query: a Flux query
        :param str, Organization org: specifies the organization for executing the query;
                                      Take the ``ID``, ``Name`` or ``Organization``.
                                      If not specified the default value from ``InfluxDBClient.org`` is used.
        :param dialect: csv dialect format
        :param params: bind parameters
        :return: The returned object is an iterator. Each iteration returns a row of the CSV file
                 (which can span multiple input lines).
        """
        org = self._org_param(org)
        response = self._query_api.post_query(org=org, query=self._create_query(query, dialect, params),
                                              async_req=False, _preload_content=False)

        return self._to_csv(response)

    def query_raw(self, query: str, org=None, dialect=default_dialect, params: dict = None):
        """
        Execute synchronous Flux query and return result as raw unprocessed result as a str.

        :param query: a Flux query
        :param str, Organization org: specifies the organization for executing the query;
                                      Take the ``ID``, ``Name`` or ``Organization``.
                                      If not specified the default value from ``InfluxDBClient.org`` is used.
        :param dialect: csv dialect format
        :param params: bind parameters
        :return: str
        """
        org = self._org_param(org)
        result = self._query_api.post_query(org=org, query=self._create_query(query, dialect, params), async_req=False,
                                            _preload_content=False)

        return result

    def query(self, query: str, org=None, params: dict = None) -> List['FluxTable']:
        """
        Execute synchronous Flux query and return result as a List['FluxTable'].

        :param query: the Flux query
        :param str, Organization org: specifies the organization for executing the query;
                                      Take the ``ID``, ``Name`` or ``Organization``.
                                      If not specified the default value from ``InfluxDBClient.org`` is used.
        :param params: bind parameters
        :return:
        """
        org = self._org_param(org)

        response = self._query_api.post_query(org=org, query=self._create_query(query, self.default_dialect, params),
                                              async_req=False, _preload_content=False, _return_http_data_only=False)

        return self._to_tables(response, query_options=self._get_query_options())

    def query_stream(self, query: str, org=None, params: dict = None) -> Generator['FluxRecord', Any, None]:
        """
        Execute synchronous Flux query and return stream of FluxRecord as a Generator['FluxRecord'].

        :param query: the Flux query
        :param str, Organization org: specifies the organization for executing the query;
                                      Take the ``ID``, ``Name`` or ``Organization``.
                                      If not specified the default value from ``InfluxDBClient.org`` is used.
        :param params: bind parameters
        :return:
        """
        org = self._org_param(org)

        response = self._query_api.post_query(org=org, query=self._create_query(query, self.default_dialect, params),
                                              async_req=False, _preload_content=False, _return_http_data_only=False)
        return self._to_flux_record_stream(response, query_options=self._get_query_options())

    def query_data_frame(self, query: str, org=None, data_frame_index: List[str] = None, params: dict = None):
        """
        Execute synchronous Flux query and return Pandas DataFrame.

        Note that if a query returns tables with differing schemas than the client generates
        a DataFrame for each of them.

        :param query: the Flux query
        :param str, Organization org: specifies the organization for executing the query;
                                      Take the ``ID``, ``Name`` or ``Organization``.
                                      If not specified the default value from ``InfluxDBClient.org`` is used.
        :param data_frame_index: the list of columns that are used as DataFrame index
        :param params: bind parameters
        :return:
        """
        _generator = self.query_data_frame_stream(query, org=org, data_frame_index=data_frame_index, params=params)
        return self._to_data_frames(_generator)

    def query_data_frame_stream(self, query: str, org=None, data_frame_index: List[str] = None, params: dict = None):
        """
        Execute synchronous Flux query and return stream of Pandas DataFrame as a Generator['pd.DataFrame'].

        Note that if a query returns tables with differing schemas than the client generates
        a DataFrame for each of them.

        :param query: the Flux query
        :param str, Organization org: specifies the organization for executing the query;
                                      Take the ``ID``, ``Name`` or ``Organization``.
                                      If not specified the default value from ``InfluxDBClient.org`` is used.
        :param data_frame_index: the list of columns that are used as DataFrame index
        :param params: bind parameters
        :return:
        """
        org = self._org_param(org)

        response = self._query_api.post_query(org=org, query=self._create_query(query, self.default_dialect, params),
                                              async_req=False, _preload_content=False, _return_http_data_only=False)

        return self._to_data_frame_stream(data_frame_index=data_frame_index,
                                          response=response,
                                          query_options=self._get_query_options())

    def _get_query_options(self):
        if self._query_options and self._query_options.profilers:
            return self._query_options
        elif self._influxdb_client.profilers:
            return QueryOptions(profilers=self._influxdb_client.profilers)

    def _create_query(self, query, dialect=default_dialect, params: dict = None):
        query_options = self._get_query_options()
        profilers = query_options.profilers if query_options is not None else None
        q = Query(query=query, dialect=dialect, extern=QueryApi._build_flux_ast(params, profilers))

        if profilers:
            print("\n===============")
            print("Profiler: query")
            print("===============")
            print(query)

        return q

    def _org_param(self, org):
        return get_org_query_param(org=org, client=self._influxdb_client)

    @staticmethod
    def _params_to_extern_ast(params: dict) -> List['OptionStatement']:

        statements = []
        for key, value in params.items():
            expression = QueryApi._parm_to_extern_ast(value)
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
                                   elements=list(map(lambda it: QueryApi._parm_to_extern_ast(it), value)))
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
            body.extend(QueryApi._params_to_extern_ast(params))

        return File(package=None, name=None, type=None, imports=imports, body=body)

    def __del__(self):
        """Close QueryAPI."""
        pass
