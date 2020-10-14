"""Exceptions utils for InfluxDB."""

import logging

from urllib3 import HTTPResponse

logger = logging.getLogger(__name__)


class InfluxDBError(Exception):
    """Raised when a server error occurs."""

    def __init__(self, response: HTTPResponse):
        """Initialize the InfluxDBError handler."""
        self.response = response
        self.message = self._get_message(response)
        self.retry_after = response.getheader('Retry-After')
        super().__init__(self.message)

    def _get_message(self, response):
        # Body
        if response.data:
            import json
            try:
                return json.loads(response.data)["message"]
            except Exception as e:
                logging.debug(f"Cannot parse error response to JSON: {response.data}, {e}")
                return response.data

        # Header
        for header_key in ["X-Platform-Error-Code", "X-Influx-Error", "X-InfluxDb-Error"]:
            header_value = response.getheader(header_key)
            if header_value is not None:
                return header_value

        # Http Status
        return response.reason
