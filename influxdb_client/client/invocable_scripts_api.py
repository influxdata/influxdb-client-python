"""
Use API invokable scripts to create custom InfluxDB API endpoints that query, process, and shape data.

API invokable scripts let you assign scripts to API endpoints and then execute them as standard REST operations
in InfluxDB Cloud.
"""

from typing import List, Iterator, Generator, Any

from influxdb_client import Script, InvocableScriptsService, ScriptCreateRequest, ScriptUpdateRequest, \
    ScriptInvocationParams
from influxdb_client.client.flux_csv_parser import FluxResponseMetadataMode
from influxdb_client.client.flux_table import FluxTable, FluxRecord
from influxdb_client.client.queryable_api import QueryableApi


class InvocableScriptsApi(QueryableApi):
    """Use API invokable scripts to create custom InfluxDB API endpoints that query, process, and shape data."""

    def __init__(self, influxdb_client):
        """Initialize defaults."""
        self._influxdb_client = influxdb_client
        self._invocable_scripts_service = InvocableScriptsService(influxdb_client.api_client)

    def create_script(self, create_request: ScriptCreateRequest) -> Script:
        """Create a script.

        :param ScriptCreateRequest create_request: The script to create. (required)
        :return: The created script.
        """
        return self._invocable_scripts_service.post_scripts(script_create_request=create_request)

    def update_script(self, script_id: str, update_request: ScriptUpdateRequest) -> Script:
        """Update a script.

        :param str script_id: The ID of the script to update. (required)
        :param ScriptUpdateRequest update_request: Script updates to apply (required)
        :return: The updated.
        """
        return self._invocable_scripts_service.patch_scripts_id(script_id=script_id,
                                                                script_update_request=update_request)

    def delete_script(self, script_id: str) -> None:
        """Delete a script.

        :param str script_id: The ID of the script to delete. (required)
        :return: None
        """
        self._invocable_scripts_service.delete_scripts_id(script_id=script_id)

    def find_scripts(self, **kwargs):
        """List scripts.

        :key int limit: The number of scripts to return.
        :key int offset: The offset for pagination.
        :return: List of scripts.
        :rtype: list[Script]
        """
        return self._invocable_scripts_service.get_scripts(**kwargs).scripts

    def invoke_scripts(self, script_id: str, params: dict = None) -> List['FluxTable']:
        """
        Invoke synchronously a script and return result as a List['FluxTable'].

        The bind parameters referenced in the script are substitutes with `params` key-values sent in the request body.

        :param str script_id: The ID of the script to invoke. (required)
        :param params: bind parameters
        :return: List of FluxTable.
        :rtype: list[FluxTable]
        """
        response = self._invocable_scripts_service \
            .post_scripts_id_invoke(script_id=script_id,
                                    script_invocation_params=ScriptInvocationParams(params=params),
                                    async_req=False,
                                    _preload_content=False,
                                    _return_http_data_only=False)
        return self._to_tables(response, query_options=None, response_metadata_mode=FluxResponseMetadataMode.only_names)

    def invoke_scripts_stream(self, script_id: str, params: dict = None) -> Generator['FluxRecord', Any, None]:
        """
        Invoke synchronously a script and return result as a Generator['FluxRecord'].

        The bind parameters referenced in the script are substitutes with `params` key-values sent in the request body.

        :param str script_id: The ID of the script to invoke. (required)
        :param params: bind parameters
        :return: Stream of FluxRecord.
        :rtype: Generator['FluxRecord']
        """
        response = self._invocable_scripts_service \
            .post_scripts_id_invoke(script_id=script_id,
                                    script_invocation_params=ScriptInvocationParams(params=params),
                                    async_req=False,
                                    _preload_content=False,
                                    _return_http_data_only=False)

        return self._to_flux_record_stream(response, query_options=None,
                                           response_metadata_mode=FluxResponseMetadataMode.only_names)

    def invoke_scripts_data_frame(self, script_id: str, params: dict = None, data_frame_index: List[str] = None):
        """
        Invoke synchronously a script and return Pandas DataFrame.

        Note that if a script returns tables with differing schemas than the client generates
        a DataFrame for each of them.

        The bind parameters referenced in the script are substitutes with `params` key-values sent in the request body.

        :param str script_id: The ID of the script to invoke. (required)
        :param List[str] data_frame_index: The list of columns that are used as DataFrame index.
        :param params: bind parameters
        :return: Pandas DataFrame.
        """
        _generator = self.invoke_scripts_data_frame_stream(script_id=script_id,
                                                           params=params,
                                                           data_frame_index=data_frame_index)
        return self._to_data_frames(_generator)

    def invoke_scripts_data_frame_stream(self, script_id: str, params: dict = None, data_frame_index: List[str] = None):
        """
        Invoke synchronously a script and return stream of Pandas DataFrame as a Generator['pd.DataFrame'].

        The bind parameters referenced in the script are substitutes with `params` key-values sent in the request body.

        :param str script_id: The ID of the script to invoke. (required)
        :param List[str] data_frame_index: The list of columns that are used as DataFrame index.
        :param params: bind parameters
        :return: Stream of Pandas DataFrames.
        :rtype: Generator['pd.DataFrame']
        """
        response = self._invocable_scripts_service \
            .post_scripts_id_invoke(script_id=script_id,
                                    script_invocation_params=ScriptInvocationParams(params=params),
                                    async_req=False,
                                    _preload_content=False,
                                    _return_http_data_only=False)

        return self._to_data_frame_stream(data_frame_index, response, query_options=None,
                                          response_metadata_mode=FluxResponseMetadataMode.only_names)

    def invoke_scripts_csv(self, script_id: str, params: dict = None) -> Iterator[List[str]]:
        """
        Invoke synchronously a script and return result as a CSV iterator.

        The bind parameters referenced in the script are substitutes with `params` key-values sent in the request body.

        :param str script_id: The ID of the script to invoke. (required)
        :param params: bind parameters
        :return: The returned object is an iterator. Each iteration returns a row of the CSV file
                 (which can span multiple input lines).
        """
        response = self._invocable_scripts_service \
            .post_scripts_id_invoke(script_id=script_id,
                                    script_invocation_params=ScriptInvocationParams(params=params),
                                    async_req=False,
                                    _preload_content=False)

        return self._to_csv(response)

    def invoke_scripts_raw(self, script_id: str, params: dict = None) -> Iterator[List[str]]:
        """
        Invoke synchronously a script and return result as raw unprocessed result as a str.

        The bind parameters referenced in the script are substitutes with `params` key-values sent in the request body.

        :param str script_id: The ID of the script to invoke. (required)
        :param params: bind parameters
        :return: Result as a str.
        """
        response = self._invocable_scripts_service \
            .post_scripts_id_invoke(script_id=script_id,
                                    script_invocation_params=ScriptInvocationParams(params=params),
                                    async_req=False,
                                    _preload_content=True)

        return response
