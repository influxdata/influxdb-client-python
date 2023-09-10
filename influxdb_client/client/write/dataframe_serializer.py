"""
Functions for serialize Pandas DataFrame.

Much of the code here is inspired by that in the aioinflux packet found here: https://github.com/gusutabopb/aioinflux
"""

import logging
import math
import re

from influxdb_client import WritePrecision
from influxdb_client.client.write.point import _ESCAPE_KEY, _ESCAPE_STRING, _ESCAPE_MEASUREMENT, DEFAULT_WRITE_PRECISION

logger = logging.getLogger('influxdb_client.client.write.dataframe_serializer')


def _itertuples(data_frame):
    cols = [data_frame.iloc[:, k] for k in range(len(data_frame.columns))]
    return zip(data_frame.index, *cols)


def _not_nan(x):
    return x == x


def _any_not_nan(p, indexes):
    return any(map(lambda x: _not_nan(p[x]), indexes))


class DataframeSerializer:
    """Serialize DataFrame into LineProtocols."""

    def __init__(self, data_frame, point_settings, precision=DEFAULT_WRITE_PRECISION, chunk_size: int = None,
                 **kwargs) -> None:
        """
        Init serializer.

        :param data_frame: Pandas DataFrame to serialize
        :param point_settings: Default Tags
        :param precision: The precision for the unix timestamps within the body line-protocol.
        :param chunk_size: The size of chunk for serializing into chunks.
        :key data_frame_measurement_name: name of measurement for writing Pandas DataFrame
        :key data_frame_tag_columns: list of DataFrame columns which are tags, rest columns will be fields
        :key data_frame_timestamp_column: name of DataFrame column which contains a timestamp. The column can be defined as a :class:`~str` value
                                          formatted as `2018-10-26`, `2018-10-26 12:00`, `2018-10-26 12:00:00-05:00`
                                          or other formats and types supported by `pandas.to_datetime <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.to_datetime.html#pandas.to_datetime>`_ - ``DataFrame``
        :key data_frame_timestamp_timezone: name of the timezone which is used for timestamp column - ``DataFrame``
        """  # noqa: E501
        # This function is hard to understand but for good reason:
        # the approach used here is considerably more efficient
        # than the alternatives.
        #
        # We build up a Python expression that efficiently converts a data point
        # tuple into line-protocol entry, and then evaluate the expression
        # as a lambda so that we can call it. This avoids the overhead of
        # invoking a function on every data value - we only have one function
        # call per row instead. The expression consists of exactly
        # one f-string, so we build up the parts of it as segments
        # that are concatenated together to make the full f-string inside
        # the lambda.
        #
        # Things are made a little more complex because fields and tags with NaN
        # values and empty tags are omitted from the generated line-protocol
        # output.
        #
        # As an example, say we have a data frame with two value columns:
        #        a float
        #        b int
        #
        # This will generate a lambda expression to be evaluated that looks like
        # this:
        #
        #     lambda p: f"""{measurement_name} {keys[0]}={p[1]},{keys[1]}={p[2]}i {p[0].value}"""
        #
        # This lambda is then executed for each row p.
        #
        # When NaNs are present, the expression looks like this (split
        # across two lines to satisfy the code-style checker)
        #
        #    lambda p: f"""{measurement_name} {"" if math.isnan(p[1])
        #    else f"{keys[0]}={p[1]}"},{keys[1]}={p[2]}i {p[0].value}"""
        #
        # When there's a NaN value in column a, we'll end up with a comma at the start of the
        # fields, so we run a regexp substitution after generating the line-protocol entries
        # to remove this.
        #
        # We're careful to run these potentially costly extra steps only when NaN values actually
        # exist in the data.

        from ...extras import pd, np
        if not isinstance(data_frame, pd.DataFrame):
            raise TypeError('Must be DataFrame, but type was: {0}.'
                            .format(type(data_frame)))

        data_frame_measurement_name = kwargs.get('data_frame_measurement_name')
        if data_frame_measurement_name is None:
            raise TypeError('"data_frame_measurement_name" is a Required Argument')

        timestamp_column = kwargs.get('data_frame_timestamp_column', None)
        timestamp_timezone = kwargs.get('data_frame_timestamp_timezone', None)
        data_frame = data_frame.copy(deep=False)
        data_frame_timestamp = data_frame.index if timestamp_column is None else data_frame[timestamp_column]
        if isinstance(data_frame_timestamp, pd.PeriodIndex):
            data_frame_timestamp = data_frame_timestamp.to_timestamp()
        else:
            # TODO: this is almost certainly not what you want
            # when the index is the default RangeIndex.
            # Instead, it would probably be better to leave
            # out the timestamp unless a time column is explicitly
            # enabled.
            data_frame_timestamp = pd.to_datetime(data_frame_timestamp, unit=precision)

        if timestamp_timezone:
            if isinstance(data_frame_timestamp, pd.DatetimeIndex):
                data_frame_timestamp = data_frame_timestamp.tz_localize(timestamp_timezone)
            else:
                data_frame_timestamp = data_frame_timestamp.dt.tz_localize(timestamp_timezone)

        if hasattr(data_frame_timestamp, 'tzinfo') and data_frame_timestamp.tzinfo is None:
            data_frame_timestamp = data_frame_timestamp.tz_localize('UTC')
        if timestamp_column is None:
            data_frame.index = data_frame_timestamp
        else:
            data_frame[timestamp_column] = data_frame_timestamp

        data_frame_tag_columns = kwargs.get('data_frame_tag_columns')
        data_frame_tag_columns = set(data_frame_tag_columns or [])

        # keys holds a list of string keys.
        keys = []
        # tags holds a list of tag f-string segments ordered alphabetically by tag key.
        tags = []
        # fields holds a list of field f-string segments  ordered alphebetically by field key
        fields = []
        # field_indexes holds the index into each row of all the fields.
        field_indexes = []

        if point_settings.defaultTags:
            for key, value in point_settings.defaultTags.items():
                # Avoid overwriting existing data if there's a column
                # that already exists with the default tag's name.
                # Note: when a new column is added, the old DataFrame
                # that we've made a shallow copy of is unaffected.
                # TODO: when there are NaN or empty values in
                # the column, we could make a deep copy of the
                # data and fill in those values with the default tag value.
                if key not in data_frame.columns:
                    data_frame[key] = value
                    data_frame_tag_columns.add(key)

        # Get a list of all the columns sorted by field/tag key.
        # We want to iterate through the columns in sorted order
        # so that we know when we're on the first field so we
        # can know whether a comma is needed for that
        # field.
        columns = sorted(enumerate(data_frame.dtypes.items()), key=lambda col: col[1][0])

        # null_columns has a bool value for each column holding
        # whether that column contains any null (NaN or None) values.
        null_columns = data_frame.isnull().any()
        timestamp_index = 0

        # Iterate through the columns building up the expression for each column.
        for index, (key, value) in columns:
            key = str(key)
            key_format = f'{{keys[{len(keys)}]}}'
            keys.append(key.translate(_ESCAPE_KEY))
            # The field index is one more than the column index because the
            # time index is at column zero in the finally zipped-together
            # result columns.
            field_index = index + 1
            val_format = f'p[{field_index}]'

            if key in data_frame_tag_columns:
                # This column is a tag column.
                if null_columns.iloc[index]:
                    key_value = f"""{{
                            '' if {val_format} == '' or type({val_format}) == float and math.isnan({val_format}) else
                            f',{key_format}={{str({val_format}).translate(_ESCAPE_STRING)}}'
                        }}"""
                else:
                    key_value = f',{key_format}={{str({val_format}).translate(_ESCAPE_KEY)}}'
                tags.append(key_value)
                continue
            elif timestamp_column is not None and key in timestamp_column:
                timestamp_index = field_index
                continue

            # This column is a field column.
            # Note: no comma separator is needed for the first field.
            # It's important to omit it because when the first
            # field column has no nulls, we don't run the comma-removal
            # regexp substitution step.
            sep = '' if len(field_indexes) == 0 else ','
            if issubclass(value.type, np.integer):
                field_value = f"{sep}{key_format}={{{val_format}}}i"
            elif issubclass(value.type, np.bool_):
                field_value = f'{sep}{key_format}={{{val_format}}}'
            elif issubclass(value.type, np.floating):
                if null_columns.iloc[index]:
                    field_value = f"""{{"" if math.isnan({val_format}) else f"{sep}{key_format}={{{val_format}}}"}}"""
                else:
                    field_value = f'{sep}{key_format}={{{val_format}}}'
            else:
                if null_columns.iloc[index]:
                    field_value = f"""{{
                            '' if type({val_format}) == float and math.isnan({val_format}) else
                            f'{sep}{key_format}="{{str({val_format}).translate(_ESCAPE_STRING)}}"'
                        }}"""
                else:
                    field_value = f'''{sep}{key_format}="{{str({val_format}).translate(_ESCAPE_STRING)}}"'''
            field_indexes.append(field_index)
            fields.append(field_value)

        measurement_name = str(data_frame_measurement_name).translate(_ESCAPE_MEASUREMENT)

        tags = ''.join(tags)
        fields = ''.join(fields)
        timestamp = '{p[%s].value}' % timestamp_index
        if precision == WritePrecision.US:
            timestamp = '{int(p[%s].value / 1e3)}' % timestamp_index
        elif precision == WritePrecision.MS:
            timestamp = '{int(p[%s].value / 1e6)}' % timestamp_index
        elif precision == WritePrecision.S:
            timestamp = '{int(p[%s].value / 1e9)}' % timestamp_index

        f = eval(f'lambda p: f"""{{measurement_name}}{tags} {fields} {timestamp}"""', {
            'measurement_name': measurement_name,
            '_ESCAPE_KEY': _ESCAPE_KEY,
            '_ESCAPE_STRING': _ESCAPE_STRING,
            'keys': keys,
            'math': math,
        })

        for k, v in dict(data_frame.dtypes).items():
            if k in data_frame_tag_columns:
                data_frame[k].replace('', np.nan, inplace=True)

        self.data_frame = data_frame
        self.f = f
        self.field_indexes = field_indexes
        self.first_field_maybe_null = null_columns.iloc[field_indexes[0] - 1]

        #
        # prepare chunks
        #
        if chunk_size is not None:
            self.number_of_chunks = int(math.ceil(len(data_frame) / float(chunk_size)))
            self.chunk_size = chunk_size
        else:
            self.number_of_chunks = None

    def serialize(self, chunk_idx: int = None):
        """
        Serialize chunk into LineProtocols.

        :param chunk_idx: The index of chunk to serialize. If `None` then serialize whole dataframe.
        """
        if chunk_idx is None:
            chunk = self.data_frame
        else:
            logger.debug("Serialize chunk %s/%s ...", chunk_idx + 1, self.number_of_chunks)
            chunk = self.data_frame[chunk_idx * self.chunk_size:(chunk_idx + 1) * self.chunk_size]

        if self.first_field_maybe_null:
            # When the first field is null (None/NaN), we'll have
            # a spurious leading comma which needs to be removed.
            lp = (re.sub('^(( |[^ ])* ),([a-zA-Z0-9])(.*)', '\\1\\3\\4', self.f(p))
                  for p in filter(lambda x: _any_not_nan(x, self.field_indexes), _itertuples(chunk)))
            return list(lp)
        else:
            return list(map(self.f, _itertuples(chunk)))

    def number_of_chunks(self):
        """
        Return the number of chunks.

        :return: number of chunks or None if chunk_size is not specified.
        """
        return self.number_of_chunks


def data_frame_to_list_of_points(data_frame, point_settings, precision=DEFAULT_WRITE_PRECISION, **kwargs):
    """
    Serialize DataFrame into LineProtocols.

    :param data_frame: Pandas DataFrame to serialize
    :param point_settings: Default Tags
    :param precision: The precision for the unix timestamps within the body line-protocol.
    :key data_frame_measurement_name: name of measurement for writing Pandas DataFrame
    :key data_frame_tag_columns: list of DataFrame columns which are tags, rest columns will be fields
    :key data_frame_timestamp_column: name of DataFrame column which contains a timestamp. The column can be defined as a :class:`~str` value
                                      formatted as `2018-10-26`, `2018-10-26 12:00`, `2018-10-26 12:00:00-05:00`
                                      or other formats and types supported by `pandas.to_datetime <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.to_datetime.html#pandas.to_datetime>`_ - ``DataFrame``
    :key data_frame_timestamp_timezone: name of the timezone which is used for timestamp column - ``DataFrame``
    """  # noqa: E501
    return DataframeSerializer(data_frame, point_settings, precision, **kwargs).serialize()
