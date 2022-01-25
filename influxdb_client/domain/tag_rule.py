# coding: utf-8

"""
InfluxDB OSS API Service.

The InfluxDB v2 API provides a programmatic interface for all interactions with InfluxDB. Access the InfluxDB API using the `/api/v2/` endpoint.   # noqa: E501

OpenAPI spec version: 2.0.0
Generated by: https://openapi-generator.tech
"""


import pprint
import re  # noqa: F401

import six


class TagRule(object):
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
        'key': 'str',
        'value': 'str',
        'operator': 'str'
    }

    attribute_map = {
        'key': 'key',
        'value': 'value',
        'operator': 'operator'
    }

    def __init__(self, key=None, value=None, operator=None):  # noqa: E501,D401,D403
        """TagRule - a model defined in OpenAPI."""  # noqa: E501
        self._key = None
        self._value = None
        self._operator = None
        self.discriminator = None

        if key is not None:
            self.key = key
        if value is not None:
            self.value = value
        if operator is not None:
            self.operator = operator

    @property
    def key(self):
        """Get the key of this TagRule.

        :return: The key of this TagRule.
        :rtype: str
        """  # noqa: E501
        return self._key

    @key.setter
    def key(self, key):
        """Set the key of this TagRule.

        :param key: The key of this TagRule.
        :type: str
        """  # noqa: E501
        self._key = key

    @property
    def value(self):
        """Get the value of this TagRule.

        :return: The value of this TagRule.
        :rtype: str
        """  # noqa: E501
        return self._value

    @value.setter
    def value(self, value):
        """Set the value of this TagRule.

        :param value: The value of this TagRule.
        :type: str
        """  # noqa: E501
        self._value = value

    @property
    def operator(self):
        """Get the operator of this TagRule.

        :return: The operator of this TagRule.
        :rtype: str
        """  # noqa: E501
        return self._operator

    @operator.setter
    def operator(self, operator):
        """Set the operator of this TagRule.

        :param operator: The operator of this TagRule.
        :type: str
        """  # noqa: E501
        self._operator = operator

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
        if not isinstance(other, TagRule):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Return true if both objects are not equal."""
        return not self == other
