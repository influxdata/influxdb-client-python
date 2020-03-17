import base64
import codecs
import csv as csv_parser
from enum import Enum
from typing import List

import ciso8601
from urllib3 import HTTPResponse

from influxdb_client.client.flux_table import FluxTable, FluxColumn, FluxRecord


class FluxQueryException(Exception):
    def __init__(self, message, reference) -> None:
        self.message = message
        self.reference = reference


class FluxCsvParserException(Exception):
    pass


class FluxSerializationMode(Enum):
    tables = 1
    stream = 2
    dataFrame = 3


class FluxCsvParser(object):

    def __init__(self, response: HTTPResponse, serialization_mode: FluxSerializationMode,
                 data_frame_index: List[str] = None) -> None:
        self._response = response
        self.tables = []
        self._serialization_mode = serialization_mode
        self._data_frame_index = data_frame_index
        self._data_frame_values = []
        pass

    def __enter__(self):
        self._reader = csv_parser.reader(codecs.iterdecode(self._response, 'utf-8'))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._response.close()

    def generator(self):
        with self as parser:
            yield from parser._parse_flux_response()

    def _parse_flux_response(self):
        table_index = 0
        table_id = -1
        start_new_table = False
        table = None
        parsing_state_error = False

        for csv in self._reader:
            # debug
            # print("parsing: ", csv)

            # Response has HTTP status ok, but response is error.
            if len(csv) < 1:
                continue

            if "error" == csv[1] and "reference" == csv[2]:
                parsing_state_error = True
                continue

            # Throw  InfluxException with error response
            if parsing_state_error:
                error = csv[1]
                reference_value = csv[2]
                raise FluxQueryException(error, reference_value)

            token = csv[0]
            # start    new    table
            if "#datatype" == token:

                # Return already parsed DataFrame
                if (self._serialization_mode is FluxSerializationMode.dataFrame) & hasattr(self, '_data_frame'):
                    yield self._prepare_data_frame()

                start_new_table = True
                table = FluxTable()
                self._insert_table(table, table_index)
                table_index = table_index + 1
                table_id = -1
            elif table is None:
                raise FluxCsvParserException("Unable to parse CSV response. FluxTable definition was not found.")

            #  # datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string
            if "#datatype" == token:
                self.add_data_types(table, csv)

            elif "#group" == token:
                self.add_groups(table, csv)

            elif "#default" == token:
                self.add_default_empty_values(table, csv)

            else:
                # parse column names
                if start_new_table:
                    self.add_column_names_and_tags(table, csv)
                    start_new_table = False
                    # Create DataFrame with default values
                    if self._serialization_mode is FluxSerializationMode.dataFrame:
                        from ..extras import pd
                        self._data_frame = pd.DataFrame(data=[], columns=[], index=None)
                        for column in table.columns:
                            self._data_frame[column.label] = column.default_value
                        pass
                    continue

                # to int converions todo
                current_id = int(csv[2])
                if table_id == -1:
                    table_id = current_id

                if table_id != current_id:
                    # create    new        table       with previous column headers settings
                    flux_columns = table.columns
                    table = FluxTable()
                    table.columns.extend(flux_columns)
                    self._insert_table(table, table_index)
                    table_index = table_index + 1
                    table_id = current_id

                flux_record = self.parse_record(table_index - 1, table, csv)

                if self._serialization_mode is FluxSerializationMode.tables:
                    self.tables[table_index - 1].records.append(flux_record)

                if self._serialization_mode is FluxSerializationMode.stream:
                    yield flux_record

                if self._serialization_mode is FluxSerializationMode.dataFrame:
                    self._data_frame_values.append(flux_record.values)
                    pass

        # Return latest DataFrame
        if (self._serialization_mode is FluxSerializationMode.dataFrame) & hasattr(self, '_data_frame'):
            yield self._prepare_data_frame()

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
        return self._data_frame.append(_temp_df)

    def parse_record(self, table_index, table, csv):
        record = FluxRecord(table_index)

        for fluxColumn in table.columns:
            column_name = fluxColumn.label
            str_val = csv[fluxColumn.index + 1]
            record.values[column_name] = self._to_value(str_val, fluxColumn)

        return record

    def _to_value(self, str_val, column):

        if str_val == '' or str_val is None:
            default_value = column.default_value
            if default_value == '' or default_value is None:
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
            # todo nanosecods precision
            # return str_val
            return ciso8601.parse_datetime(str_val)
            # return timestamp_parser(str_val)

        if "duration" == column.data_type:
            # todo better type ?
            return int(str_val)

    @staticmethod
    def add_data_types(table, data_types):
        for index in range(1, len(data_types)):
            column_def = FluxColumn(index=index - 1, data_type=data_types[index])
            table.columns.append(column_def)

    @staticmethod
    def add_groups(table, csv):
        i = 1
        for column in table.columns:
            column.group = csv[i] == "true"
            i += 1

    @staticmethod
    def add_default_empty_values(table, default_values):
        i = 1
        for column in table.columns:
            column.default_value = default_values[i]
            i += 1

    @staticmethod
    def add_column_names_and_tags(table, csv):
        i = 1
        for column in table.columns:
            column.label = csv[i]
            i += 1

    def _insert_table(self, table, table_index):
        if self._serialization_mode is FluxSerializationMode.tables:
            self.tables.insert(table_index, table)