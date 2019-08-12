# coding: utf-8

"""
    Influx API Service

    No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)  # noqa: E501

    OpenAPI spec version: 0.1.0
    Generated by: https://openapi-generator.tech
"""


import pprint
import re  # noqa: F401

import six
from influxdb2.domain.view_properties import ViewProperties


class MarkdownViewProperties(ViewProperties):
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
        'type': 'str',
        'shape': 'str',
        'note': 'str'
    }

    attribute_map = {
        'type': 'type',
        'shape': 'shape',
        'note': 'note'
    }

    def __init__(self, type=None, shape=None, note=None):  # noqa: E501
        """MarkdownViewProperties - a model defined in OpenAPI"""  # noqa: E501
        ViewProperties.__init__(self)

        self._type = None
        self._shape = None
        self._note = None
        self.discriminator = None

        self.type = type
        self.shape = shape
        self.note = note

    @property
    def type(self):
        """Gets the type of this MarkdownViewProperties.  # noqa: E501


        :return: The type of this MarkdownViewProperties.  # noqa: E501
        :rtype: str
        """
        return self._type

    @type.setter
    def type(self, type):
        """Sets the type of this MarkdownViewProperties.


        :param type: The type of this MarkdownViewProperties.  # noqa: E501
        :type: str
        """
        if type is None:
            raise ValueError("Invalid value for `type`, must not be `None`")  # noqa: E501
        allowed_values = ["markdown"]  # noqa: E501
        if type not in allowed_values:
            raise ValueError(
                "Invalid value for `type` ({0}), must be one of {1}"  # noqa: E501
                .format(type, allowed_values)
            )

        self._type = type

    @property
    def shape(self):
        """Gets the shape of this MarkdownViewProperties.  # noqa: E501


        :return: The shape of this MarkdownViewProperties.  # noqa: E501
        :rtype: str
        """
        return self._shape

    @shape.setter
    def shape(self, shape):
        """Sets the shape of this MarkdownViewProperties.


        :param shape: The shape of this MarkdownViewProperties.  # noqa: E501
        :type: str
        """
        if shape is None:
            raise ValueError("Invalid value for `shape`, must not be `None`")  # noqa: E501
        allowed_values = ["chronograf-v2"]  # noqa: E501
        if shape not in allowed_values:
            raise ValueError(
                "Invalid value for `shape` ({0}), must be one of {1}"  # noqa: E501
                .format(shape, allowed_values)
            )

        self._shape = shape

    @property
    def note(self):
        """Gets the note of this MarkdownViewProperties.  # noqa: E501


        :return: The note of this MarkdownViewProperties.  # noqa: E501
        :rtype: str
        """
        return self._note

    @note.setter
    def note(self, note):
        """Sets the note of this MarkdownViewProperties.


        :param note: The note of this MarkdownViewProperties.  # noqa: E501
        :type: str
        """
        if note is None:
            raise ValueError("Invalid value for `note`, must not be `None`")  # noqa: E501

        self._note = note

    def to_dict(self):
        """Returns the model properties as a dict"""
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
        """Returns the string representation of the model"""
        return pprint.pformat(self.to_dict())

    def __repr__(self):
        """For `print` and `pprint`"""
        return self.to_str()

    def __eq__(self, other):
        """Returns true if both objects are equal"""
        if not isinstance(other, MarkdownViewProperties):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        return not self == other
