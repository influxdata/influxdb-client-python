

# noinspection PyMethodMayBeStatic
class _BaseService(object):

    def __init__(self, api_client=None):
        """Init common services operation."""
        if api_client is None:
            raise ValueError("Invalid value for `api_client`, must be defined.")
        self.api_client = api_client
        self._build_type = None

    def _check_operation_params(self, operation_id, supported_params, local_params):
        supported_params.append('async_req')
        supported_params.append('_return_http_data_only')
        supported_params.append('_preload_content')
        supported_params.append('_request_timeout')
        supported_params.append('urlopen_kw')
        for key, val in local_params['kwargs'].items():
            if key not in supported_params:
                raise TypeError(
                    f"Got an unexpected keyword argument '{key}'"
                    f" to method {operation_id}"
                )
            local_params[key] = val
        del local_params['kwargs']

    def _is_cloud_instance(self) -> bool:
        if not self._build_type:
            self._build_type = self.build_type()
        return 'cloud' in self._build_type.lower()

    async def _is_cloud_instance_async(self) -> bool:
        if not self._build_type:
            self._build_type = await self.build_type_async()
        return 'cloud' in self._build_type.lower()

    def build_type(self) -> str:
        """
        Return the build type of the connected InfluxDB Server.

        :return: The type of InfluxDB build.
        """
        from influxdb_client import PingService
        ping_service = PingService(self.api_client)

        response = ping_service.get_ping_with_http_info(_return_http_data_only=False)
        return self.response_header(response, header_name='X-Influxdb-Build')

    async def build_type_async(self) -> str:
        """
        Return the build type of the connected InfluxDB Server.

        :return: The type of InfluxDB build.
        """
        from influxdb_client import PingService
        ping_service = PingService(self.api_client)

        response = await ping_service.get_ping_async(_return_http_data_only=False)
        return self.response_header(response, header_name='X-Influxdb-Build')

    def response_header(self, response, header_name='X-Influxdb-Version') -> str:
        if response is not None and len(response) >= 3:
            if header_name in response[2]:
                return response[2][header_name]

        return "unknown"
