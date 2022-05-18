

# noinspection PyMethodMayBeStatic
class _BaseService(object):
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
