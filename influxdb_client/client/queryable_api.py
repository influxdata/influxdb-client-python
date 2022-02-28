"""Helper to share queryable function between APIs - query_api, invocable_scripts_api."""
import codecs
import csv
from typing import List, Iterator, Generator, Any

from urllib3 import HTTPResponse

from influxdb_client.client.flux_csv_parser import FluxCsvParser, FluxSerializationMode, FluxResponseMetadataMode
from influxdb_client.client.flux_table import FluxTable, FluxRecord


# noinspection PyMethodMayBeStatic
class QueryableApi(object):
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
            Generator['FluxRecord', Any, None]:
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
