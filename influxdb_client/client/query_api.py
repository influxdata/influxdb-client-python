"""
Querying InfluxDB bu FluxLang.

Flux is InfluxData’s functional data scripting language designed for querying, analyzing, and acting on data.
"""

import codecs
import csv
from datetime import datetime, timedelta
from typing import List, Generator, Any

from influxdb_client import Dialect, IntegerLiteral, BooleanLiteral, FloatLiteral, DateTimeLiteral, StringLiteral, \
    VariableAssignment, Identifier, OptionStatement, File, DurationLiteral, Duration, UnaryExpression, \
    ImportDeclaration, MemberAssignment, MemberExpression, ArrayExpression
from influxdb_client import Query, QueryService
from influxdb_client.client.flux_csv_parser import FluxCsvParser, FluxSerializationMode
from influxdb_client.client.flux_table import FluxTable, FluxRecord
from influxdb_client.client.util.date_utils import get_date_helper


class QueryOptions(object):
    """Query options."""

    def __init__(self, profilers: List[str] = None) -> None:
        """
        Initialize query options.

        :param profilers: list of enabled flux profilers
        """
        self.profilers = profilers


class QueryApi(object):
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
        :param org: organization name (optional if already specified in InfluxDBClient)
        :param dialect: csv dialect format
        :param params: bind parameters
        :return: The returned object is an iterator.  Each iteration returns a row of the CSV file
                 (which can span multiple input lines).
        """
        if org is None:
            org = self._influxdb_client.org
        response = self._query_api.post_query(
            org=org,
            query=self._create_query(query, dialect, params, self._query_options),
            async_req=False, _preload_content=False)

        return csv.reader(codecs.iterdecode(response, 'utf-8'))

    def query_raw(self, query: str, org=None, dialect=default_dialect, params: dict = None):
        """
        Execute synchronous Flux query and return result as raw unprocessed result as a str.

        :param query: a Flux query
        :param org: organization name (optional if already specified in InfluxDBClient)
        :param dialect: csv dialect format
        :param params: bind parameters
        :return: str
        """
        if org is None:
            org = self._influxdb_client.org
        result = self._query_api.post_query(
            org=org,
            query=self._create_query(query, dialect, params, self._query_options),
            async_req=False, _preload_content=False)

        return result

    def query(self, query: str, org=None, params: dict = None) -> List['FluxTable']:
        """
        Execute synchronous Flux query and return result as a List['FluxTable'].

        :param query: the Flux query
        :param org: organization name (optional if already specified in InfluxDBClient)
        :param params: bind parameters
        :return:
        """
        if org is None:
            org = self._influxdb_client.org

        response = self._query_api.post_query(org=org, query=self._create_query(query, self.default_dialect, params,
                                                                                self._query_options),
                                              async_req=False, _preload_content=False, _return_http_data_only=False)

        _parser = FluxCsvParser(
            response=response,
            serialization_mode=FluxSerializationMode.tables,
            query_options=self._query_options)

        list(_parser.generator())

        if self._query_options.profilers is not None and len(self._query_options.profilers) > 0:
            return list(filter(lambda table: not self._is_profiler_table(table), _parser.tables))
        else:
            return _parser.tables

    @staticmethod
    def _is_profiler_table(table: FluxTable) -> bool:
        return any(filter(lambda column: (column.default_value == "_profiler" and column.label == "result"),
                          table.columns))

    def query_stream(self, query: str, org=None, params: dict = None) -> Generator['FluxRecord', Any, None]:
        """
        Execute synchronous Flux query and return stream of FluxRecord as a Generator['FluxRecord'].

        :param query: the Flux query
        :param org: organization name (optional if already specified in InfluxDBClient)
        :param params: bind parameters
        :return:
        """
        if org is None:
            org = self._influxdb_client.org

        response = self._query_api.post_query(org=org, query=self._create_query(query, self.default_dialect, params,
                                                                                self._query_options), async_req=False,
                                              _preload_content=False, _return_http_data_only=False)
        _parser = FluxCsvParser(
            response=response,
            serialization_mode=FluxSerializationMode.stream,
            query_options=self._query_options)

        return _parser.generator()

    def query_data_frame(self, query: str, org=None, data_frame_index: List[str] = None, params: dict = None):
        """
        Execute synchronous Flux query and return Pandas DataFrame.

        Note that if a query returns more then one table than the client generates a DataFrame for each of them.

        :param query: the Flux query
        :param org: organization name (optional if already specified in InfluxDBClient)
        :param data_frame_index: the list of columns that are used as DataFrame index
        :param params: bind parameters
        :return:
        """
        from ..extras import pd

        _generator = self.query_data_frame_stream(query, org=org, data_frame_index=data_frame_index, params=params)
        _dataFrames = list(_generator)

        if len(_dataFrames) == 0:
            return pd.DataFrame(columns=[], index=None)
        elif len(_dataFrames) == 1:
            return _dataFrames[0]
        else:
            return _dataFrames

    def query_data_frame_stream(self, query: str, org=None, data_frame_index: List[str] = None, params: dict = None):
        """
        Execute synchronous Flux query and return stream of Pandas DataFrame as a Generator['pd.DataFrame'].

        Note that if a query returns more then one table than the client generates a DataFrame for each of them.

        :param query: the Flux query
        :param org: organization name (optional if already specified in InfluxDBClient)
        :param data_frame_index: the list of columns that are used as DataFrame index
        :param params: bind parameters
        :return:
        """
        if org is None:
            org = self._influxdb_client.org

        response = self._query_api.post_query(
            org=org,
            query=self._create_query(query, self.default_dialect, params, self._query_options),
            async_req=False,
            _preload_content=False,
            _return_http_data_only=False)

        _parser = FluxCsvParser(
            response=response,
            serialization_mode=FluxSerializationMode.dataFrame,
            data_frame_index=data_frame_index,
            query_options=self._query_options)

        return _parser.generator()

    @staticmethod
    def _create_query(query, dialect=default_dialect, params: dict = None, query_options: QueryOptions = None):
        q = Query(query=query, dialect=dialect, extern=QueryApi._build_flux_ast(params, query_options.profilers))

        if query_options is not None \
                and query_options.profilers is not None \
                and len(query_options.profilers) > 0:
            print("\n===============")
            print("Profiler: query")
            print("===============")
            print(query)

        return q

    @staticmethod
    def _params_to_extern_ast(params: dict) -> List['OptionStatement']:

        statements = []
        for key, value in params.items():
            if value is None:
                continue

            if isinstance(value, bool):
                literal = BooleanLiteral("BooleanLiteral", value)
            elif isinstance(value, int):
                literal = IntegerLiteral("IntegerLiteral", str(value))
            elif isinstance(value, float):
                literal = FloatLiteral("FloatLiteral", value)
            elif isinstance(value, datetime):
                value = get_date_helper().to_utc(value)
                literal = DateTimeLiteral("DateTimeLiteral", value.strftime('%Y-%m-%dT%H:%M:%S.%fZ'))
            elif isinstance(value, timedelta):
                _micro_delta = int(value / timedelta(microseconds=1))
                if _micro_delta < 0:
                    literal = UnaryExpression("UnaryExpression", argument=DurationLiteral("DurationLiteral", [
                        Duration(magnitude=-_micro_delta, unit="us")]), operator="-")
                else:
                    literal = DurationLiteral("DurationLiteral", [Duration(magnitude=_micro_delta, unit="us")])
            elif isinstance(value, str):
                literal = StringLiteral("StringLiteral", str(value))
            else:
                literal = value

            statements.append(OptionStatement("OptionStatement",
                                              VariableAssignment("VariableAssignment", Identifier("Identifier", key),
                                                                 literal)))
        return statements

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
