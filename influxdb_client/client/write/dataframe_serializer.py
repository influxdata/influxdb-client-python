"""
Functions for serialize Pandas DataFrame.

Much of the code here is inspired by that in the aioinflux packet found here: https://github.com/gusutabopb/aioinflux
"""

import re
import math

from influxdb_client import WritePrecision
from influxdb_client.client.write.point import _ESCAPE_KEY, _ESCAPE_STRING, _ESCAPE_MEASUREMENT, DEFAULT_WRITE_PRECISION


def _itertuples(data_frame):
    cols = [data_frame.iloc[:, k] for k in range(len(data_frame.columns))]
    return zip(data_frame.index, *cols)


def _not_nan(x):
    return x == x


def _any_not_nan(p, indexes):
    return any(map(lambda x: _not_nan(p[x]), indexes))


def data_frame_to_list_of_points(data_frame, point_settings, precision=DEFAULT_WRITE_PRECISION, **kwargs):
    """
    Serialize DataFrame into LineProtocols.

    :param data_frame: Pandas DataFrame to serialize
    :param point_settings: Default Tags
    :param precision: The precision for the unix timestamps within the body line-protocol.
    :key data_frame_measurement_name: name of measurement for writing Pandas DataFrame
    :key data_frame_tag_columns: list of DataFrame columns which are tags, rest columns will be fields
    """
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

    data_frame = data_frame.copy(deep=False)
    if isinstance(data_frame.index, pd.PeriodIndex):
        data_frame.index = data_frame.index.to_timestamp()
    else:
        # TODO: this is almost certainly not what you want
        # when the index is the default RangeIndex.
        # Instead, it would probably be better to leave
        # out the timestamp unless a time column is explicitly
        # enabled.
        data_frame.index = pd.to_datetime(data_frame.index)

    if data_frame.index.tzinfo is None:
        data_frame.index = data_frame.index.tz_localize('UTC')

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
            if null_columns[index]:
                key_value = f"""{{
                    '' if {val_format} == '' or type({val_format}) == float and math.isnan({val_format}) else
                    f',{key_format}={{str({val_format}).translate(_ESCAPE_STRING)}}'
                }}"""
            else:
                key_value = f',{key_format}={{str({val_format}).translate(_ESCAPE_KEY)}}'
            tags.append(key_value)
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
            if null_columns[index]:
                field_value = f"""{{"" if math.isnan({val_format}) else f"{sep}{key_format}={{{val_format}}}"}}"""
            else:
                field_value = f'{sep}{key_format}={{{val_format}}}'
        else:
            if null_columns[index]:
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
    timestamp = '{p[0].value}'
    if precision == WritePrecision.US:
        timestamp = '{int(p[0].value / 1e3)}'
    elif precision == WritePrecision.MS:
        timestamp = '{int(p[0].value / 1e6)}'
    elif precision == WritePrecision.S:
        timestamp = '{int(p[0].value / 1e9)}'

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

    first_field_maybe_null = null_columns[field_indexes[0] - 1]
    if first_field_maybe_null:
        # When the first field is null (None/NaN), we'll have
        # a spurious leading comma which needs to be removed.
        lp = (re.sub('^((\\ |[^ ])* ),', '\\1', f(p))
              for p in filter(lambda x: _any_not_nan(x, field_indexes), _itertuples(data_frame)))
        return list(lp)
    else:
        return list(map(f, _itertuples(data_frame)))
