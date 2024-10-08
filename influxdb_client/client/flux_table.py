"""
Flux employs a basic data model built from basic data types.

The data model consists of tables, records, columns.
"""
import codecs
import csv
from http.client import HTTPResponse
from json import JSONEncoder
from typing import List, Iterator
from influxdb_client.rest import _UTF_8_encoding


class FluxStructure:
    """The data model consists of tables, records, columns."""

    pass


class FluxStructureEncoder(JSONEncoder):
    """The FluxStructure encoder to encode query results to JSON."""

    def default(self, obj):
        """Return serializable objects for JSONEncoder."""
        import datetime
        if isinstance(obj, FluxStructure):
            return obj.__dict__
        elif isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        return super().default(obj)


class FluxTable(FluxStructure):
    """
    A table is set of records with a common set of columns and a group key.

    The table can be serialized into JSON by::

        import json
        from influxdb_client.client.flux_table import FluxStructureEncoder

        output = json.dumps(tables, cls=FluxStructureEncoder, indent=2)
        print(output)

    """

    def __init__(self) -> None:
        """Initialize defaults."""
        self.columns: List[FluxColumn] = []
        self.records: List[FluxRecord] = []

    def get_group_key(self):
        """
        Group key is a list of columns.

        A table’s group key denotes which subset of the entire dataset is assigned to the table.
        """
        return list(filter(lambda column: (column.group is True), self.columns))

    def __str__(self):
        """Return formatted output."""
        cls_name = type(self).__name__
        return cls_name + "() columns: " + str(len(self.columns)) + ", records: " + str(len(self.records))

    def __repr__(self):
        """Format for inspection."""
        return f"<{type(self).__name__}: {len(self.columns)} columns, {len(self.records)} records>"

    def __iter__(self):
        """Iterate over records."""
        return iter(self.records)


class FluxColumn(FluxStructure):
    """A column has a label and a data type."""

    def __init__(self, index=None, label=None, data_type=None, group=None, default_value=None) -> None:
        """Initialize defaults."""
        self.default_value = default_value
        self.group = group
        self.data_type = data_type
        self.label = label
        self.index = index

    def __repr__(self):
        """Format for inspection."""
        fields = [repr(self.index)] + [
            f'{name}={getattr(self, name)!r}' for name in (
                'label', 'data_type', 'group', 'default_value'
            ) if getattr(self, name) is not None
        ]
        return f"{type(self).__name__}({', '.join(fields)})"


class FluxRecord(FluxStructure):
    """A record is a tuple of named values and is represented using an object type."""

    def __init__(self, table, values=None) -> None:
        """Initialize defaults."""
        if values is None:
            values = {}
        self.table = table
        self.values = values
        self.row = []

    def get_start(self):
        """Get '_start' value."""
        return self["_start"]

    def get_stop(self):
        """Get '_stop' value."""
        return self["_stop"]

    def get_time(self):
        """Get timestamp."""
        return self["_time"]

    def get_value(self):
        """Get field value."""
        return self["_value"]

    def get_field(self):
        """Get field name."""
        return self["_field"]

    def get_measurement(self):
        """Get measurement name."""
        return self["_measurement"]

    def __getitem__(self, key):
        """Get value by key."""
        return self.values.__getitem__(key)

    def __setitem__(self, key, value):
        """Set value with key and value."""
        return self.values.__setitem__(key, value)

    def __str__(self):
        """Return formatted output."""
        cls_name = type(self).__name__
        return cls_name + "() table: " + str(self.table) + ", " + str(self.values)

    def __repr__(self):
        """Format for inspection."""
        return f"<{type(self).__name__}: field={self.values.get('_field')}, value={self.values.get('_value')}>"


class TableList(List[FluxTable]):
    """:class:`~influxdb_client.client.flux_table.FluxTable` list with additionally functional to better handle of query result."""  # noqa: E501

    def to_values(self, columns: List['str'] = None) -> List[List[object]]:
        """
        Serialize query results to a flattened list of values.

        :param columns: if not ``None`` then only specified columns are presented in results
        :return: :class:`~list` of values

        Output example:

        .. code-block:: python

            [
                ['New York', datetime.datetime(2022, 6, 7, 11, 3, 22, 917593, tzinfo=tzutc()), 24.3],
                ['Prague', datetime.datetime(2022, 6, 7, 11, 3, 22, 917593, tzinfo=tzutc()), 25.3],
                ...
            ]

        Configure required columns:

        .. code-block:: python

            from influxdb_client import InfluxDBClient

                with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org") as client:

                # Query: using Table structure
                tables = client.query_api().query('from(bucket:"my-bucket") |> range(start: -10m)')

                # Serialize to values
                output = tables.to_values(columns=['location', '_time', '_value'])
                print(output)
        """

        def filter_values(record):
            if columns is not None:
                return [record.values.get(k) for k in columns]
            return record.values.values()

        return self._to_values(filter_values)

    def to_json(self, columns: List['str'] = None, **kwargs) -> str:
        """
        Serialize query results to a JSON formatted :class:`~str`.

        :param columns: if not ``None`` then only specified columns are presented in results
        :return: :class:`~str`

        The query results is flattened to array:

        .. code-block:: javascript

            [
                {
                    "_measurement": "mem",
                    "_start": "2021-06-23T06:50:11.897825+00:00",
                    "_stop": "2021-06-25T06:50:11.897825+00:00",
                    "_time": "2020-02-27T16:20:00.897825+00:00",
                    "region": "north",
                     "_field": "usage",
                    "_value": 15
                },
                {
                    "_measurement": "mem",
                    "_start": "2021-06-23T06:50:11.897825+00:00",
                    "_stop": "2021-06-25T06:50:11.897825+00:00",
                    "_time": "2020-02-27T16:20:01.897825+00:00",
                    "region": "west",
                     "_field": "usage",
                    "_value": 10
                },
                ...
            ]

        The JSON format could be configured via ``**kwargs`` arguments:

        .. code-block:: python

            from influxdb_client import InfluxDBClient

            with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org") as client:

                # Query: using Table structure
                tables = client.query_api().query('from(bucket:"my-bucket") |> range(start: -10m)')

                # Serialize to JSON
                output = tables.to_json(indent=5)
                print(output)

        For all available options see - `json.dump <https://docs.python.org/3/library/json.html#json.dump>`_.
        """
        if 'indent' not in kwargs:
            kwargs['indent'] = 2

        def filter_values(record):
            if columns is not None:
                return {k: v for (k, v) in record.values.items() if k in columns}
            return record.values

        import json
        return json.dumps(self._to_values(filter_values), cls=FluxStructureEncoder, **kwargs)

    def _to_values(self, mapping):
        return [mapping(record) for table in self for record in table.records]


class CSVIterator(Iterator[List[str]]):
    """:class:`Iterator[List[str]]` with additionally functional to better handle of query result."""

    def __init__(self, response: HTTPResponse) -> None:
        """Initialize ``csv.reader``."""
        self.delegate = csv.reader(codecs.iterdecode(response, _UTF_8_encoding))

    def __iter__(self):
        """Return an iterator object."""
        return self

    def __next__(self):
        """Retrieve the next item from the iterator."""
        row = self.delegate.__next__()
        while not row:
            row = self.delegate.__next__()
        return row

    def to_values(self) -> List[List[str]]:
        """
        Serialize query results to a flattened list of values.

        :return: :class:`~list` of values

        Output example:

        .. code-block:: python

            [
                ['New York', '2022-06-14T08:00:51.749072045Z', '24.3'],
                ['Prague', '2022-06-14T08:00:51.749072045Z', '25.3'],
                ...
            ]
        """
        return list(self.__iter__())
