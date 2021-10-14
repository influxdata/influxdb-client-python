# coding: utf-8

"""
InfluxData Managed Functions CRUD API.

No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)  # noqa: E501

OpenAPI spec version: 0.1.0
Generated by: https://openapi-generator.tech
"""


import pprint
import re  # noqa: F401

import six


class Functions(object):
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
        'functions': 'list[Function]'
    }

    attribute_map = {
        'functions': 'functions'
    }

    def __init__(self, functions=None):  # noqa: E501,D401,D403
        """Functions - a model defined in OpenAPI."""  # noqa: E501
        self._functions = None
        self.discriminator = None

        if functions is not None:
            self.functions = functions

    @property
    def functions(self):
        """Get the functions of this Functions.

        :return: The functions of this Functions.
        :rtype: list[Function]
        """  # noqa: E501
        return self._functions

    @functions.setter
    def functions(self, functions):
        """Set the functions of this Functions.

        :param functions: The functions of this Functions.
        :type: list[Function]
        """  # noqa: E501
        self._functions = functions

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
        if not isinstance(other, Functions):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Return true if both objects are not equal."""
        return not self == other
