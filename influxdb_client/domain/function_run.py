# coding: utf-8

"""
Influx OSS API Service.

No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)  # noqa: E501

OpenAPI spec version: 2.0.0
Generated by: https://openapi-generator.tech
"""


import pprint
import re  # noqa: F401

import six
from influxdb_client.domain.function_run_base import FunctionRunBase


class FunctionRun(FunctionRunBase):
    """NOTE: This class is auto generated by OpenAPI Generator.

    Ref: https://openapi-generator.tech

    Do not edit the class manually.
    """

    """
    Attributes:
      openapi_types (dict): The key is attribute name
                            and the value is attribute type.
      attribute_map (dict): The key is attribute name
                            and the value is json key in definition.
    """
    openapi_types = {
        'response': 'FunctionHTTPResponseNoData',
        'id': 'str',
        'status': 'str',
        'error': 'str',
        'logs': 'list[FunctionRunLog]',
        'started_at': 'datetime'
    }

    attribute_map = {
        'response': 'response',
        'id': 'id',
        'status': 'status',
        'error': 'error',
        'logs': 'logs',
        'started_at': 'startedAt'
    }

    def __init__(self, response=None, id=None, status=None, error=None, logs=None, started_at=None):  # noqa: E501,D401,D403
        """FunctionRun - a model defined in OpenAPI."""  # noqa: E501
        FunctionRunBase.__init__(self, id=id, status=status, error=error, logs=logs, started_at=started_at)  # noqa: E501

        self._response = None
        self.discriminator = None

        if response is not None:
            self.response = response

    @property
    def response(self):
        """Get the response of this FunctionRun.

        :return: The response of this FunctionRun.
        :rtype: FunctionHTTPResponseNoData
        """  # noqa: E501
        return self._response

    @response.setter
    def response(self, response):
        """Set the response of this FunctionRun.

        :param response: The response of this FunctionRun.
        :type: FunctionHTTPResponseNoData
        """  # noqa: E501
        self._response = response

    def to_dict(self):
        """Return the model properties as a dict."""
        result = {}

        for attr, _ in six.iteritems(self.openapi_types):
            value = getattr(self, attr)
            if isinstance(value, list):
                result[attr] = list(map(
                    lambda x: x.to_dict() if hasattr(x, "to_dict") else x,
                    value
                ))
            elif hasattr(value, "to_dict"):
                result[attr] = value.to_dict()
            elif isinstance(value, dict):
                result[attr] = dict(map(
                    lambda item: (item[0], item[1].to_dict())
                    if hasattr(item[1], "to_dict") else item,
                    value.items()
                ))
            else:
                result[attr] = value

        return result

    def to_str(self):
        """Return the string representation of the model."""
        return pprint.pformat(self.to_dict())

    def __repr__(self):
        """For `print` and `pprint`."""
        return self.to_str()

    def __eq__(self, other):
        """Return true if both objects are equal."""
        if not isinstance(other, FunctionRun):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Return true if both objects are not equal."""
        return not self == other
