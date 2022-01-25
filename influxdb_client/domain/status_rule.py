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


class StatusRule(object):
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
        'current_level': 'RuleStatusLevel',
        'previous_level': 'RuleStatusLevel',
        'count': 'int',
        'period': 'str'
    }

    attribute_map = {
        'current_level': 'currentLevel',
        'previous_level': 'previousLevel',
        'count': 'count',
        'period': 'period'
    }

    def __init__(self, current_level=None, previous_level=None, count=None, period=None):  # noqa: E501,D401,D403
        """StatusRule - a model defined in OpenAPI."""  # noqa: E501
        self._current_level = None
        self._previous_level = None
        self._count = None
        self._period = None
        self.discriminator = None

        if current_level is not None:
            self.current_level = current_level
        if previous_level is not None:
            self.previous_level = previous_level
        if count is not None:
            self.count = count
        if period is not None:
            self.period = period

    @property
    def current_level(self):
        """Get the current_level of this StatusRule.

        :return: The current_level of this StatusRule.
        :rtype: RuleStatusLevel
        """  # noqa: E501
        return self._current_level

    @current_level.setter
    def current_level(self, current_level):
        """Set the current_level of this StatusRule.

        :param current_level: The current_level of this StatusRule.
        :type: RuleStatusLevel
        """  # noqa: E501
        self._current_level = current_level

    @property
    def previous_level(self):
        """Get the previous_level of this StatusRule.

        :return: The previous_level of this StatusRule.
        :rtype: RuleStatusLevel
        """  # noqa: E501
        return self._previous_level

    @previous_level.setter
    def previous_level(self, previous_level):
        """Set the previous_level of this StatusRule.

        :param previous_level: The previous_level of this StatusRule.
        :type: RuleStatusLevel
        """  # noqa: E501
        self._previous_level = previous_level

    @property
    def count(self):
        """Get the count of this StatusRule.

        :return: The count of this StatusRule.
        :rtype: int
        """  # noqa: E501
        return self._count

    @count.setter
    def count(self, count):
        """Set the count of this StatusRule.

        :param count: The count of this StatusRule.
        :type: int
        """  # noqa: E501
        self._count = count

    @property
    def period(self):
        """Get the period of this StatusRule.

        :return: The period of this StatusRule.
        :rtype: str
        """  # noqa: E501
        return self._period

    @period.setter
    def period(self, period):
        """Set the period of this StatusRule.

        :param period: The period of this StatusRule.
        :type: str
        """  # noqa: E501
        self._period = period

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
        if not isinstance(other, StatusRule):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Return true if both objects are not equal."""
        return not self == other
