"""
Functions for serialize Pandas DataFrame.

Much of the code here is inspired by that in the aioinflux packet found here: https://github.com/gusutabopb/aioinflux
"""

import re
from functools import reduce
from itertools import chain

from influxdb_client.client.write.point import _ESCAPE_KEY, _ESCAPE_MEASUREMENT


def _replace(data_frame):
    from ...extras import np

    # string columns
    obj_cols = {k for k, v in dict(data_frame.dtypes).items() if v is np.dtype('O')}

    # number columns
    other_cols = set(data_frame.columns) - obj_cols

    obj_nans = (f'{k}=nan' for k in obj_cols)
    other_nans = (f'{k}=nani?' for k in other_cols)

    replacements = [
        ('|'.join(chain(obj_nans, other_nans)), ''),
        (',{2,}', ','),
        ('|'.join([', ,', ', ', ' ,']), ' '),
    ]

    return replacements


def _itertuples(data_frame):
    cols = [data_frame.iloc[:, k] for k in range(len(data_frame.columns))]
    return zip(data_frame.index, *cols)


def data_frame_to_list_of_points(data_frame, point_settings, **kwargs):
    """Serialize DataFrame into LineProtocols."""
    from ...extras import pd, np
    if not isinstance(data_frame, pd.DataFrame):
        raise TypeError('Must be DataFrame, but type was: {0}.'
                        .format(type(data_frame)))

    if 'data_frame_measurement_name' not in kwargs:
        raise TypeError('"data_frame_measurement_name" is a Required Argument')

    if isinstance(data_frame.index, pd.PeriodIndex):
        data_frame.index = data_frame.index.to_timestamp()
    else:
        data_frame.index = pd.to_datetime(data_frame.index)

    if data_frame.index.tzinfo is None:
        data_frame.index = data_frame.index.tz_localize('UTC')

    measurement_name = str(kwargs.get('data_frame_measurement_name')).translate(_ESCAPE_MEASUREMENT)
    data_frame_tag_columns = kwargs.get('data_frame_tag_columns')
    data_frame_tag_columns = set(data_frame_tag_columns or [])

    tags = []
    fields = []
    keys = []

    if point_settings.defaultTags:
        for key, value in point_settings.defaultTags.items():
            data_frame[key] = value
            data_frame_tag_columns.add(key)

    for index, (key, value) in enumerate(data_frame.dtypes.items()):
        key = str(key)
        keys.append(key.translate(_ESCAPE_KEY))
        key_format = f'{{keys[{index}]}}'

        if key in data_frame_tag_columns:
            tags.append({'key': key, 'value': f"{key_format}={{str(p[{index + 1}]).translate(_ESCAPE_KEY)}}"})
        elif issubclass(value.type, np.integer):
            fields.append(f"{key_format}={{p[{index + 1}]}}i")
        elif issubclass(value.type, (np.float, np.bool_)):
            fields.append(f"{key_format}={{p[{index + 1}]}}")
        else:
            fields.append(f"{key_format}=\"{{str(p[{index + 1}]).translate(_ESCAPE_KEY)}}\"")

    tags.sort(key=lambda x: x['key'])
    tags = ','.join(map(lambda y: y['value'], tags))

    fmt = ('{measurement_name}', f'{"," if tags else ""}', tags,
           ' ', ','.join(fields), ' {p[0].value}')
    f = eval("lambda p: f'{}'".format(''.join(fmt)),
             {'measurement_name': measurement_name, '_ESCAPE_KEY': _ESCAPE_KEY, 'keys': keys})

    for k, v in dict(data_frame.dtypes).items():
        if k in data_frame_tag_columns:
            data_frame[k].replace('', np.nan, inplace=True)

    isnull = data_frame.isnull().any(axis=1)

    if isnull.any():
        rep = _replace(data_frame)
        lp = (reduce(lambda a, b: re.sub(*b, a), rep, f(p))
              for p in _itertuples(data_frame))
        return list(lp)
    else:
        return list(map(f, _itertuples(data_frame)))
