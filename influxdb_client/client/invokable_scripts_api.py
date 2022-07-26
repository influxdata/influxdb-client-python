"""
Use API invokable scripts to create custom InfluxDB API endpoints that query, process, and shape data.

API invokable scripts let you assign scripts to API endpoints and then execute them as standard REST operations
in InfluxDB Cloud.
"""

from typing import List, Iterator, Generator, Any

from influxdb_client import Script, InvokableScriptsService, ScriptCreateRequest, ScriptUpdateRequest, \
    ScriptInvocationParams
from influxdb_client.client._base import _BaseQueryApi
from influxdb_client.client.flux_csv_parser import FluxResponseMetadataMode
from influxdb_client.client.flux_table import FluxRecord, TableList, CSVIterator


class InvokableScriptsApi(_BaseQueryApi):
    """Use API invokable scripts to create custom InfluxDB API endpoints that query, process, and shape data."""

    def __init__(self, influxdb_client):
        """Initialize defaults."""
        self._influxdb_client = influxdb_client
        self._invokable_scripts_service = InvokableScriptsService(influxdb_client.api_client)

    def create_script(self, create_request: ScriptCreateRequest) -> Script:
        """Create a script.

        :param ScriptCreateRequest create_request: The script to create. (required)
        :return: The created script.
        """
        return self._invokable_scripts_service.post_scripts(script_create_request=create_request)

    def update_script(self, script_id: str, update_request: ScriptUpdateRequest) -> Script:
        """Update a script.

        :param str script_id: The ID of the script to update. (required)
        :param ScriptUpdateRequest update_request: Script updates to apply (required)
        :return: The updated.
        """
        return self._invokable_scripts_service.patch_scripts_id(script_id=script_id,
                                                                script_update_request=update_request)

    def delete_script(self, script_id: str) -> None:
        """Delete a script.

        :param str script_id: The ID of the script to delete. (required)
        :return: None
        """
        self._invokable_scripts_service.delete_scripts_id(script_id=script_id)

    def find_scripts(self, **kwargs):
        """List scripts.

        :key int limit: The number of scripts to return.
        :key int offset: The offset for pagination.
        :return: List of scripts.
        :rtype: list[Script]
        """
        return self._invokable_scripts_service.get_scripts(**kwargs).scripts

    def invoke_script(self, script_id: str, params: dict = None) -> TableList:
        """
        Invoke synchronously a script and return result as a TableList.

        The bind parameters referenced in the script are substitutes with `params` key-values sent in the request body.

        :param str script_id: The ID of the script to invoke. (required)
        :param params: bind parameters
        :return: :class:`~influxdb_client.client.flux_table.FluxTable` list wrapped into
                 :class:`~influxdb_client.client.flux_table.TableList`
        :rtype: TableList

        Serialization the query results to flattened list of values via :func:`~influxdb_client.client.flux_table.TableList.to_values`:

        .. code-block:: python

            from influxdb_client import InfluxDBClient

            with InfluxDBClient(url="https://us-west-2-1.aws.cloud2.influxdata.com", token="my-token", org="my-org") as client:

                # Query: using Table structure
                tables = client.invokable_scripts_api().invoke_script(script_id="script-id")

                # Serialize to values
                output = tables.to_values(columns=['location', '_time', '_value'])
                print(output)

        .. code-block:: python

            [
                ['New York', datetime.datetime(2022, 6, 7, 11, 3, 22, 917593, tzinfo=tzutc()), 24.3],
                ['Prague', datetime.datetime(2022, 6, 7, 11, 3, 22, 917593, tzinfo=tzutc()), 25.3],
                ...
            ]

        Serialization the query results to JSON via :func:`~influxdb_client.client.flux_table.TableList.to_json`:

        .. code-block:: python

            from influxdb_client import InfluxDBClient

            with InfluxDBClient(url="https://us-west-2-1.aws.cloud2.influxdata.com", token="my-token", org="my-org") as client:

                # Query: using Table structure
                tables = client.invokable_scripts_api().invoke_script(script_id="script-id")

                # Serialize to JSON
                output = tables.to_json(indent=5)
                print(output)

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
        """  # noqa: E501
        response = self._invokable_scripts_service \
            .post_scripts_id_invoke(script_id=script_id,
                                    script_invocation_params=ScriptInvocationParams(params=params),
                                    async_req=False,
                                    _preload_content=False,
                                    _return_http_data_only=False)
        return self._to_tables(response, query_options=None, response_metadata_mode=FluxResponseMetadataMode.only_names)

    def invoke_script_stream(self, script_id: str, params: dict = None) -> Generator['FluxRecord', Any, None]:
        """
        Invoke synchronously a script and return result as a Generator['FluxRecord'].

        The bind parameters referenced in the script are substitutes with `params` key-values sent in the request body.

        :param str script_id: The ID of the script to invoke. (required)
        :param params: bind parameters
        :return: Stream of FluxRecord.
        :rtype: Generator['FluxRecord']
        """
        response = self._invokable_scripts_service \
            .post_scripts_id_invoke(script_id=script_id,
                                    script_invocation_params=ScriptInvocationParams(params=params),
                                    async_req=False,
                                    _preload_content=False,
                                    _return_http_data_only=False)

        return self._to_flux_record_stream(response, query_options=None,
                                           response_metadata_mode=FluxResponseMetadataMode.only_names)

    def invoke_script_data_frame(self, script_id: str, params: dict = None, data_frame_index: List[str] = None):
        """
        Invoke synchronously a script and return Pandas DataFrame.

        The bind parameters referenced in the script are substitutes with `params` key-values sent in the request body.

        .. note:: If the ``script`` returns tables with differing schemas than the client generates a :class:`~DataFrame` for each of them.

        :param str script_id: The ID of the script to invoke. (required)
        :param List[str] data_frame_index: The list of columns that are used as DataFrame index.
        :param params: bind parameters
        :return: :class:`~DataFrame` or :class:`~List[DataFrame]`

        .. warning:: For the optimal processing of the query results use the ``pivot() function`` which align results as a table.

            .. code-block:: text

                from(bucket:"my-bucket")
                    |> range(start: -5m, stop: now())
                    |> filter(fn: (r) => r._measurement == "mem")
                    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")

            For more info see:
                - https://docs.influxdata.com/resources/videos/pivots-in-flux/
                - https://docs.influxdata.com/flux/latest/stdlib/universe/pivot/
                - https://docs.influxdata.com/flux/latest/stdlib/influxdata/influxdb/schema/fieldsascols/
        """  # noqa: E501
        _generator = self.invoke_script_data_frame_stream(script_id=script_id,
                                                          params=params,
                                                          data_frame_index=data_frame_index)
        return self._to_data_frames(_generator)

    def invoke_script_data_frame_stream(self, script_id: str, params: dict = None, data_frame_index: List[str] = None):
        """
        Invoke synchronously a script and return stream of Pandas DataFrame as a Generator['pd.DataFrame'].

        The bind parameters referenced in the script are substitutes with `params` key-values sent in the request body.

        .. note:: If the ``script`` returns tables with differing schemas than the client generates a :class:`~DataFrame` for each of them.

        :param str script_id: The ID of the script to invoke. (required)
        :param List[str] data_frame_index: The list of columns that are used as DataFrame index.
        :param params: bind parameters
        :return: :class:`~Generator[DataFrame]`

        .. warning:: For the optimal processing of the query results use the ``pivot() function`` which align results as a table.

            .. code-block:: text

                from(bucket:"my-bucket")
                    |> range(start: -5m, stop: now())
                    |> filter(fn: (r) => r._measurement == "mem")
                    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")

            For more info see:
                - https://docs.influxdata.com/resources/videos/pivots-in-flux/
                - https://docs.influxdata.com/flux/latest/stdlib/universe/pivot/
                - https://docs.influxdata.com/flux/latest/stdlib/influxdata/influxdb/schema/fieldsascols/
        """  # noqa: E501
        response = self._invokable_scripts_service \
            .post_scripts_id_invoke(script_id=script_id,
                                    script_invocation_params=ScriptInvocationParams(params=params),
                                    async_req=False,
                                    _preload_content=False,
                                    _return_http_data_only=False)

        return self._to_data_frame_stream(data_frame_index, response, query_options=None,
                                          response_metadata_mode=FluxResponseMetadataMode.only_names)

    def invoke_script_csv(self, script_id: str, params: dict = None) -> CSVIterator:
        """
        Invoke synchronously a script and return result as a CSV iterator. Each iteration returns a row of the CSV file.

        The bind parameters referenced in the script are substitutes with `params` key-values sent in the request body.

        :param str script_id: The ID of the script to invoke. (required)
        :param params: bind parameters
        :return: :class:`~Iterator[List[str]]` wrapped into :class:`~influxdb_client.client.flux_table.CSVIterator`
        :rtype: CSVIterator

        Serialization the query results to flattened list of values via :func:`~influxdb_client.client.flux_table.CSVIterator.to_values`:

        .. code-block:: python

            from influxdb_client import InfluxDBClient

            with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org") as client:

                # Query: using CSV iterator
                csv_iterator = client.invokable_scripts_api().invoke_script_csv(script_id="script-id")

                # Serialize to values
                output = csv_iterator.to_values()
                print(output)

        .. code-block:: python

            [
                ['', 'result', 'table', '_start', '_stop', '_time', '_value', '_field', '_measurement', 'location']
                ['', '', '0', '2022-06-16', '2022-06-16', '2022-06-16', '24.3', 'temperature', 'my_measurement', 'New York']
                ['', '', '1', '2022-06-16', '2022-06-16', '2022-06-16', '25.3', 'temperature', 'my_measurement', 'Prague']
                ...
            ]

        """  # noqa: E501
        response = self._invokable_scripts_service \
            .post_scripts_id_invoke(script_id=script_id,
                                    script_invocation_params=ScriptInvocationParams(params=params),
                                    async_req=False,
                                    _preload_content=False)

        return self._to_csv(response)

    def invoke_script_raw(self, script_id: str, params: dict = None) -> Iterator[List[str]]:
        """
        Invoke synchronously a script and return result as raw unprocessed result as a str.

        The bind parameters referenced in the script are substitutes with `params` key-values sent in the request body.

        :param str script_id: The ID of the script to invoke. (required)
        :param params: bind parameters
        :return: Result as a str.
        """
        response = self._invokable_scripts_service \
            .post_scripts_id_invoke(script_id=script_id,
                                    script_invocation_params=ScriptInvocationParams(params=params),
                                    async_req=False,
                                    _preload_content=True)

        return response
