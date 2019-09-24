import base64
import codecs
import csv as csv_parser

from dateutil.parser import parse as timestamp_parser
import ciso8601

from influxdb_client.client.flux_table import FluxTable, FluxColumn, FluxRecord


class FluxQueryException(Exception):
    def __init__(self, message, reference) -> None:
        self.message = message
        self.reference = reference


class FluxCsvParserException(Exception):
    pass


class FluxCsvParser(object):

    def __init__(self) -> None:
        pass

    def parse_flux_response(self, response, cancellable, consumer):
        table_index = 0
        start_new_table = False
        table = None
        parsing_state_error = False
        reader = csv_parser.reader(codecs.iterdecode(response, 'utf-8'))

        for csv in reader:
            # debug
            # print("parsing: ", csv)
            if (cancellable is not None) and cancellable.canceled:
                return

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
                start_new_table = True
                table = FluxTable()
                consumer.accept_table(index=table_index, cancellable=cancellable, flux_table=table)
                table_index = table_index + 1
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
                    continue

                # to int converions todo
                current_index = int(csv[2])

                if current_index > (table_index - 1):
                    # create    new        table       with previous column headers settings
                    flux_columns = table.columns
                    table = FluxTable()
                    table.columns.extend(flux_columns)
                    consumer.accept_table(table_index, cancellable, table)
                    table_index = table_index + 1

                flux_record = self.parse_record(table_index - 1, table, csv)
                consumer.accept_record(table_index - 1, cancellable, flux_record)
                # debug
                # print(flux_record)

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


class FluxResponseConsumerTable:

    def __init__(self) -> None:
        self.tables = []

    def accept_table(self, index, cancellable, flux_table):
        self.tables.insert(index, flux_table)

    def accept_record(self, index, cancellable, flux_record):
        self.tables[index].records.append(flux_record)
