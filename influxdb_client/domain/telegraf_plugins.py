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


class TelegrafPlugins(object):
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
        'version': 'str',
        'os': 'str',
        'plugins': 'list[TelegrafPlugin]'
    }

    attribute_map = {
        'version': 'version',
        'os': 'os',
        'plugins': 'plugins'
    }

    def __init__(self, version=None, os=None, plugins=None):  # noqa: E501,D401,D403
        """TelegrafPlugins - a model defined in OpenAPI."""  # noqa: E501
        self._version = None
        self._os = None
        self._plugins = None
        self.discriminator = None

        if version is not None:
            self.version = version
        if os is not None:
            self.os = os
        if plugins is not None:
            self.plugins = plugins

    @property
    def version(self):
        """Get the version of this TelegrafPlugins.

        :return: The version of this TelegrafPlugins.
        :rtype: str
        """  # noqa: E501
        return self._version

    @version.setter
    def version(self, version):
        """Set the version of this TelegrafPlugins.

        :param version: The version of this TelegrafPlugins.
        :type: str
        """  # noqa: E501
        self._version = version

    @property
    def os(self):
        """Get the os of this TelegrafPlugins.

        :return: The os of this TelegrafPlugins.
        :rtype: str
        """  # noqa: E501
        return self._os

    @os.setter
    def os(self, os):
        """Set the os of this TelegrafPlugins.

        :param os: The os of this TelegrafPlugins.
        :type: str
        """  # noqa: E501
        self._os = os

    @property
    def plugins(self):
        """Get the plugins of this TelegrafPlugins.

        :return: The plugins of this TelegrafPlugins.
        :rtype: list[TelegrafPlugin]
        """  # noqa: E501
        return self._plugins

    @plugins.setter
    def plugins(self, plugins):
        """Set the plugins of this TelegrafPlugins.

        :param plugins: The plugins of this TelegrafPlugins.
        :type: list[TelegrafPlugin]
        """  # noqa: E501
        self._plugins = plugins

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
        if not isinstance(other, TelegrafPlugins):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Return true if both objects are not equal."""
        return not self == other
