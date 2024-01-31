"""Parsing response from InfluxDB to FluxStructures or DataFrame."""


import base64
import codecs
import csv as csv_parser
import warnings
from enum import Enum
from typing import List

from influxdb_client.client.flux_table import FluxTable, FluxColumn, FluxRecord, TableList
from influxdb_client.client.util.date_utils import get_date_helper
from influxdb_client.rest import _UTF_8_encoding

ANNOTATION_DEFAULT = "#default"
ANNOTATION_GROUP = "#group"
ANNOTATION_DATATYPE = "#datatype"
ANNOTATIONS = [ANNOTATION_DEFAULT, ANNOTATION_GROUP, ANNOTATION_DATATYPE]


class FluxQueryException(Exception):
    """The exception from InfluxDB."""

    def __init__(self, message, reference) -> None:
        """Initialize defaults."""
        self.message = message
        self.reference = reference


class FluxCsvParserException(Exception):
    """The exception for not parsable data."""

    pass


class FluxSerializationMode(Enum):
    """The type how we want to serialize data."""

    tables = 1
    stream = 2
    dataFrame = 3


class FluxResponseMetadataMode(Enum):
    """The configuration for expected amount of metadata response from InfluxDB."""

    full = 1
    # useful for Invokable scripts
    only_names = 2


class _FluxCsvParserMetadata(object):
    def __init__(self):
        self.table_index = 0
        self.table_id = -1
        self.start_new_table = False
        self.table = None
        self.groups = []
        self.parsing_state_error = False


class FluxCsvParser(object):
    """Parse to processing response from InfluxDB to FluxStructures or DataFrame."""

    def __init__(self, response, serialization_mode: FluxSerializationMode,
                 data_frame_index: List[str] = None, query_options=None,
                 response_metadata_mode: FluxResponseMetadataMode = FluxResponseMetadataMode.full,
                 use_extension_dtypes=False) -> None:
        """
        Initialize defaults.

        :param response: HTTP response from a HTTP client.
                         Acceptable types: `urllib3.response.HTTPResponse`, `aiohttp.client_reqrep.ClientResponse`.
        """
        self._response = response
        self.tables = TableList()
        self._serialization_mode = serialization_mode
        self._response_metadata_mode = response_metadata_mode
        self._use_extension_dtypes = use_extension_dtypes
        self._data_frame_index = data_frame_index
        self._data_frame_values = []
        self._profilers = query_options.profilers if query_options is not None else None
        self._profiler_callback = query_options.profiler_callback if query_options is not None else None
        self._async_mode = True if 'ClientResponse' in type(response).__name__ else False

    def _close(self):
        self._response.close()

    def __enter__(self):
        """Initialize CSV reader."""
        # response can be exhausted by logger, so we have to use data that has already been read
        if hasattr(self._response, 'closed') and self._response.closed:
            from io import StringIO
            self._reader = csv_parser.reader(StringIO(self._response.data.decode(_UTF_8_encoding)))
        else:
            self._reader = csv_parser.reader(codecs.iterdecode(self._response, _UTF_8_encoding))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close HTTP response."""
        self._close()

    async def __aenter__(self) -> 'FluxCsvParser':
        """Initialize CSV reader."""
        from aiocsv import AsyncReader
        self._reader = AsyncReader(_StreamReaderToWithAsyncRead(self._response.content))

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Shutdown the client."""
        self.__exit__(exc_type, exc_val, exc_tb)

    def generator(self):
        """Return Python generator."""
        with self as parser:
            for val in parser._parse_flux_response():
                yield val

    def generator_async(self):
        """Return Python async-generator."""
        return self._parse_flux_response_async()

    def _parse_flux_response(self):
        metadata = _FluxCsvParserMetadata()

        for csv in self._reader:
            for val in self._parse_flux_response_row(metadata, csv):
                yield val

        # Return latest DataFrame
        if (self._serialization_mode is FluxSerializationMode.dataFrame) & hasattr(self, '_data_frame'):
            df = self._prepare_data_frame()
            if not self._is_profiler_table(metadata.table):
                yield df

    async def _parse_flux_response_async(self):
        metadata = _FluxCsvParserMetadata()

        try:
            async for csv in self._reader:
                for val in self._parse_flux_response_row(metadata, csv):
                    yield val

            # Return latest DataFrame
            if (self._serialization_mode is FluxSerializationMode.dataFrame) & hasattr(self, '_data_frame'):
                df = self._prepare_data_frame()
                if not self._is_profiler_table(metadata.table):
                    yield df
        finally:
            self._close()

    def _parse_flux_response_row(self, metadata, csv):
        if len(csv) < 1:
            # Skip empty line in results (new line is used as a delimiter between tables or table and error)
            pass

        elif "error" == csv[1] and "reference" == csv[2]:
            metadata.parsing_state_error = True

        else:
            # Throw  InfluxException with error response
            if metadata.parsing_state_error:
                error = csv[1]
                reference_value = csv[2]
                raise FluxQueryException(error, reference_value)

            token = csv[0]
            # start new table
            if (token in ANNOTATIONS and not metadata.start_new_table) or \
                    (self._response_metadata_mode is FluxResponseMetadataMode.only_names and not metadata.table):

                # Return already parsed DataFrame
                if (self._serialization_mode is FluxSerializationMode.dataFrame) & hasattr(self, '_data_frame'):
                    df = self._prepare_data_frame()
                    if not self._is_profiler_table(metadata.table):
                        yield df

                metadata.start_new_table = True
                metadata.table = FluxTable()
                self._insert_table(metadata.table, metadata.table_index)
                metadata.table_index = metadata.table_index + 1
                metadata.table_id = -1
            elif metadata.table is None:
                raise FluxCsvParserException("Unable to parse CSV response. FluxTable definition was not found.")

            #  # datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string
            if ANNOTATION_DATATYPE == token:
                self.add_data_types(metadata.table, csv)

            elif ANNOTATION_GROUP == token:
                metadata.groups = csv

            elif ANNOTATION_DEFAULT == token:
                self.add_default_empty_values(metadata.table, csv)

            else:
                # parse column names
                if metadata.start_new_table:
                    # Invokable scripts doesn't supports dialect => all columns are string
                    if not metadata.table.columns and \
                            self._response_metadata_mode is FluxResponseMetadataMode.only_names:
                        self.add_data_types(metadata.table, list(map(lambda column: 'string', csv)))
                        metadata.groups = list(map(lambda column: 'false', csv))
                    self.add_groups(metadata.table, metadata.groups)
                    self.add_column_names_and_tags(metadata.table, csv)
                    metadata.start_new_table = False
                    # Create DataFrame with default values
                    if self._serialization_mode is FluxSerializationMode.dataFrame:
                        from ..extras import pd
                        labels = list(map(lambda it: it.label, metadata.table.columns))
                        self._data_frame = pd.DataFrame(data=[], columns=labels, index=None)
                        pass
                else:

                    # to int conversions todo
                    current_id = int(csv[2])
                    if metadata.table_id == -1:
                        metadata.table_id = current_id

                    if metadata.table_id != current_id:
                        # create    new        table       with previous column headers settings
                        flux_columns = metadata.table.columns
                        metadata.table = FluxTable()
                        metadata.table.columns.extend(flux_columns)
                        self._insert_table(metadata.table, metadata.table_index)
                        metadata.table_index = metadata.table_index + 1
                        metadata.table_id = current_id

                    flux_record = self.parse_record(metadata.table_index - 1, metadata.table, csv)

                    if self._is_profiler_record(flux_record):
                        self._print_profiler_info(flux_record)
                    else:
                        if self._serialization_mode is FluxSerializationMode.tables:
                            self.tables[metadata.table_index - 1].records.append(flux_record)

                        if self._serialization_mode is FluxSerializationMode.stream:
                            yield flux_record

                        if self._serialization_mode is FluxSerializationMode.dataFrame:
                            self._data_frame_values.append(flux_record.values)
                            pass

    def _prepare_data_frame(self):
        from ..extras import pd

        # We have to create temporary DataFrame because we want to preserve default column values
        _temp_df = pd.DataFrame(self._data_frame_values)
        self._data_frame_values = []

        # Custom DataFrame index
        if self._data_frame_index:
            self._data_frame = self._data_frame.set_index(self._data_frame_index)
            _temp_df = _temp_df.set_index(self._data_frame_index)

        # Append data
        df = pd.concat([self._data_frame.astype(_temp_df.dtypes), _temp_df])

        if self._use_extension_dtypes:
            return df.convert_dtypes()
        return df

    def parse_record(self, table_index, table, csv):
        """Parse one record."""
        record = FluxRecord(table_index)

        for fluxColumn in table.columns:
            column_name = fluxColumn.label
            str_val = csv[fluxColumn.index + 1]
            record.values[column_name] = self._to_value(str_val, fluxColumn)
            record.row.append(record.values[column_name])

        return record

    def _to_value(self, str_val, column):

        if str_val == '' or str_val is None:
            default_value = column.default_value
            if default_value == '' or default_value is None:
                if self._serialization_mode is FluxSerializationMode.dataFrame:
                    if self._use_extension_dtypes:
                        from ..extras import pd
                        return pd.NA
                    return None
                return None
            return self._to_value(default_value, column)

        if "string" == column.data_type:
            return str_val

        if "boolean" == column.data_type:
            return "true" == str_val

        if "unsignedLong" == column.data_type or "long" == column.data_type:
            return int(str_val)

        if "double" == column.data_type:
            return float(str_val)

        if "base64Binary" == column.data_type:
            return base64.b64decode(str_val)

        if "dateTime:RFC3339" == column.data_type or "dateTime:RFC3339Nano" == column.data_type:
            return get_date_helper().parse_date(str_val)

        if "duration" == column.data_type:
            # todo better type ?
            return int(str_val)

    @staticmethod
    def add_data_types(table, data_types):
        """Add data types to columns."""
        for index in range(1, len(data_types)):
            column_def = FluxColumn(index=index - 1, data_type=data_types[index])
            table.columns.append(column_def)

    @staticmethod
    def add_groups(table, csv):
        """Add group keys to columns."""
        i = 1
        for column in table.columns:
            column.group = csv[i] == "true"
            i += 1

    @staticmethod
    def add_default_empty_values(table, default_values):
        """Add default values to columns."""
        i = 1
        for column in table.columns:
            column.default_value = default_values[i]
            i += 1

    @staticmethod
    def add_column_names_and_tags(table, csv):
        """Add labels to columns."""
        if len(csv) != len(set(csv)):
            message = f"""The response contains columns with duplicated names: '{csv}'.

You should use the 'record.row' to access your data instead of 'record.values' dictionary.
"""
            warnings.warn(message, UserWarning)
            print(message)
        i = 1
        for column in table.columns:
            column.label = csv[i]
            i += 1

    def _insert_table(self, table, table_index):
        if self._serialization_mode is FluxSerializationMode.tables:
            self.tables.insert(table_index, table)

    def _is_profiler_record(self, flux_record: FluxRecord) -> bool:
        if not self._profilers:
            return False

        for profiler in self._profilers:
            if "_measurement" in flux_record.values and flux_record["_measurement"] == "profiler/" + profiler:
                return True

        return False

    def _is_profiler_table(self, table: FluxTable) -> bool:

        if not self._profilers:
            return False

        return any(filter(lambda column: (column.default_value == "_profiler" and column.label == "result"),
                          table.columns))

    def table_list(self) -> TableList:
        """Get the list of flux tables."""
        if not self._profilers:
            return self.tables
        else:
            return TableList(filter(lambda table: not self._is_profiler_table(table), self.tables))

    def _print_profiler_info(self, flux_record: FluxRecord):
        if flux_record.get_measurement().startswith("profiler/"):
            if self._profiler_callback:
                self._profiler_callback(flux_record)
            else:
                msg = "Profiler: " + flux_record.get_measurement()
                print("\n" + len(msg) * "=")
                print(msg)
                print(len(msg) * "=")
                for name in flux_record.values:
                    val = flux_record[name]
                    if isinstance(val, str) and len(val) > 50:
                        print(f"{name:<20}: \n\n{val}")
                    elif val is not None:
                        print(f"{name:<20}: {val:<20}")


class _StreamReaderToWithAsyncRead:
    def __init__(self, response):
        self.response = response
        self.decoder = codecs.getincrementaldecoder(_UTF_8_encoding)()

    async def read(self, size: int) -> str:
        raw_bytes = (await self.response.read(size))
        if not raw_bytes:
            return self.decoder.decode(b'', final=True)
        return self.decoder.decode(raw_bytes, final=False)
