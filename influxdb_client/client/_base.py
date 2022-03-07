"""Commons function for Sync and Async client."""
from __future__ import absolute_import

import base64
import codecs
import csv
from typing import Iterator, List, Generator, Any

from urllib3 import HTTPResponse

from influxdb_client import Configuration
from influxdb_client.client.flux_csv_parser import FluxResponseMetadataMode, FluxCsvParser, FluxSerializationMode
from influxdb_client.client.flux_table import FluxTable, FluxRecord


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
    """Base implementation for Queryable API."""

    def _to_tables(self, response: HTTPResponse, query_options=None,
                   response_metadata_mode: FluxResponseMetadataMode = FluxResponseMetadataMode.full) -> List[FluxTable]:
        """Parse HTTP response to FluxTables."""
        _parser = FluxCsvParser(response=response, serialization_mode=FluxSerializationMode.tables,
                                query_options=query_options, response_metadata_mode=response_metadata_mode)
        list(_parser.generator())
        return _parser.table_list()

    def _to_csv(self, response: HTTPResponse) -> Iterator[List[str]]:
        """Parse HTTP response to CSV."""
        return csv.reader(codecs.iterdecode(response, 'utf-8'))

    def _to_flux_record_stream(self, response, query_options=None,
                               response_metadata_mode: FluxResponseMetadataMode = FluxResponseMetadataMode.full) -> \
            Generator[FluxRecord, Any, None]:
        """Parse HTTP response to FluxRecord stream."""
        _parser = FluxCsvParser(response=response, serialization_mode=FluxSerializationMode.stream,
                                query_options=query_options, response_metadata_mode=response_metadata_mode)
        return _parser.generator()

    def _to_data_frame_stream(self, data_frame_index, response, query_options=None,
                              response_metadata_mode: FluxResponseMetadataMode = FluxResponseMetadataMode.full):
        """Parse HTTP response to DataFrame stream."""
        _parser = FluxCsvParser(response=response, serialization_mode=FluxSerializationMode.dataFrame,
                                data_frame_index=data_frame_index, query_options=query_options,
                                response_metadata_mode=response_metadata_mode)
        return _parser.generator()

    def _to_data_frames(self, _generator):
        """Parse stream of DataFrames into expected type."""
        from ..extras import pd
        _dataFrames = list(_generator)
        if len(_dataFrames) == 0:
            return pd.DataFrame(columns=[], index=None)
        elif len(_dataFrames) == 1:
            return _dataFrames[0]
        else:
            return _dataFrames


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
