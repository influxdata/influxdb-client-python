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


class SingleStatViewProperties(ViewProperties):
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
        'queries': 'list[DashboardQuery]',
        'colors': 'list[DashboardColor]',
        'shape': 'str',
        'note': 'str',
        'show_note_when_empty': 'bool',
        'prefix': 'str',
        'suffix': 'str',
        'legend': 'Legend',
        'decimal_places': 'DecimalPlaces'
    }

    attribute_map = {
        'type': 'type',
        'queries': 'queries',
        'colors': 'colors',
        'shape': 'shape',
        'note': 'note',
        'show_note_when_empty': 'showNoteWhenEmpty',
        'prefix': 'prefix',
        'suffix': 'suffix',
        'legend': 'legend',
        'decimal_places': 'decimalPlaces'
    }

    def __init__(self, type=None, queries=None, colors=None, shape=None, note=None, show_note_when_empty=None, prefix=None, suffix=None, legend=None, decimal_places=None):  # noqa: E501
        """SingleStatViewProperties - a model defined in OpenAPI"""  # noqa: E501
        ViewProperties.__init__(self)

        self._type = None
        self._queries = None
        self._colors = None
        self._shape = None
        self._note = None
        self._show_note_when_empty = None
        self._prefix = None
        self._suffix = None
        self._legend = None
        self._decimal_places = None
        self.discriminator = None

        self.type = type
        self.queries = queries
        self.colors = colors
        self.shape = shape
        self.note = note
        self.show_note_when_empty = show_note_when_empty
        self.prefix = prefix
        self.suffix = suffix
        self.legend = legend
        self.decimal_places = decimal_places

    @property
    def type(self):
        """Gets the type of this SingleStatViewProperties.  # noqa: E501


        :return: The type of this SingleStatViewProperties.  # noqa: E501
        :rtype: str
        """
        return self._type

    @type.setter
    def type(self, type):
        """Sets the type of this SingleStatViewProperties.


        :param type: The type of this SingleStatViewProperties.  # noqa: E501
        :type: str
        """
        if type is None:
            raise ValueError("Invalid value for `type`, must not be `None`")  # noqa: E501
        allowed_values = ["single-stat"]  # noqa: E501
        if type not in allowed_values:
            raise ValueError(
                "Invalid value for `type` ({0}), must be one of {1}"  # noqa: E501
                .format(type, allowed_values)
            )

        self._type = type

    @property
    def queries(self):
        """Gets the queries of this SingleStatViewProperties.  # noqa: E501


        :return: The queries of this SingleStatViewProperties.  # noqa: E501
        :rtype: list[DashboardQuery]
        """
        return self._queries

    @queries.setter
    def queries(self, queries):
        """Sets the queries of this SingleStatViewProperties.


        :param queries: The queries of this SingleStatViewProperties.  # noqa: E501
        :type: list[DashboardQuery]
        """
        if queries is None:
            raise ValueError("Invalid value for `queries`, must not be `None`")  # noqa: E501

        self._queries = queries

    @property
    def colors(self):
        """Gets the colors of this SingleStatViewProperties.  # noqa: E501

        Colors define color encoding of data into a visualization  # noqa: E501

        :return: The colors of this SingleStatViewProperties.  # noqa: E501
        :rtype: list[DashboardColor]
        """
        return self._colors

    @colors.setter
    def colors(self, colors):
        """Sets the colors of this SingleStatViewProperties.

        Colors define color encoding of data into a visualization  # noqa: E501

        :param colors: The colors of this SingleStatViewProperties.  # noqa: E501
        :type: list[DashboardColor]
        """
        if colors is None:
            raise ValueError("Invalid value for `colors`, must not be `None`")  # noqa: E501

        self._colors = colors

    @property
    def shape(self):
        """Gets the shape of this SingleStatViewProperties.  # noqa: E501


        :return: The shape of this SingleStatViewProperties.  # noqa: E501
        :rtype: str
        """
        return self._shape

    @shape.setter
    def shape(self, shape):
        """Sets the shape of this SingleStatViewProperties.


        :param shape: The shape of this SingleStatViewProperties.  # noqa: E501
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
        """Gets the note of this SingleStatViewProperties.  # noqa: E501


        :return: The note of this SingleStatViewProperties.  # noqa: E501
        :rtype: str
        """
        return self._note

    @note.setter
    def note(self, note):
        """Sets the note of this SingleStatViewProperties.


        :param note: The note of this SingleStatViewProperties.  # noqa: E501
        :type: str
        """
        if note is None:
            raise ValueError("Invalid value for `note`, must not be `None`")  # noqa: E501

        self._note = note

    @property
    def show_note_when_empty(self):
        """Gets the show_note_when_empty of this SingleStatViewProperties.  # noqa: E501

        if true, will display note when empty  # noqa: E501

        :return: The show_note_when_empty of this SingleStatViewProperties.  # noqa: E501
        :rtype: bool
        """
        return self._show_note_when_empty

    @show_note_when_empty.setter
    def show_note_when_empty(self, show_note_when_empty):
        """Sets the show_note_when_empty of this SingleStatViewProperties.

        if true, will display note when empty  # noqa: E501

        :param show_note_when_empty: The show_note_when_empty of this SingleStatViewProperties.  # noqa: E501
        :type: bool
        """
        if show_note_when_empty is None:
            raise ValueError("Invalid value for `show_note_when_empty`, must not be `None`")  # noqa: E501

        self._show_note_when_empty = show_note_when_empty

    @property
    def prefix(self):
        """Gets the prefix of this SingleStatViewProperties.  # noqa: E501


        :return: The prefix of this SingleStatViewProperties.  # noqa: E501
        :rtype: str
        """
        return self._prefix

    @prefix.setter
    def prefix(self, prefix):
        """Sets the prefix of this SingleStatViewProperties.


        :param prefix: The prefix of this SingleStatViewProperties.  # noqa: E501
        :type: str
        """
        if prefix is None:
            raise ValueError("Invalid value for `prefix`, must not be `None`")  # noqa: E501

        self._prefix = prefix

    @property
    def suffix(self):
        """Gets the suffix of this SingleStatViewProperties.  # noqa: E501


        :return: The suffix of this SingleStatViewProperties.  # noqa: E501
        :rtype: str
        """
        return self._suffix

    @suffix.setter
    def suffix(self, suffix):
        """Sets the suffix of this SingleStatViewProperties.


        :param suffix: The suffix of this SingleStatViewProperties.  # noqa: E501
        :type: str
        """
        if suffix is None:
            raise ValueError("Invalid value for `suffix`, must not be `None`")  # noqa: E501

        self._suffix = suffix

    @property
    def legend(self):
        """Gets the legend of this SingleStatViewProperties.  # noqa: E501


        :return: The legend of this SingleStatViewProperties.  # noqa: E501
        :rtype: Legend
        """
        return self._legend

    @legend.setter
    def legend(self, legend):
        """Sets the legend of this SingleStatViewProperties.


        :param legend: The legend of this SingleStatViewProperties.  # noqa: E501
        :type: Legend
        """
        if legend is None:
            raise ValueError("Invalid value for `legend`, must not be `None`")  # noqa: E501

        self._legend = legend

    @property
    def decimal_places(self):
        """Gets the decimal_places of this SingleStatViewProperties.  # noqa: E501


        :return: The decimal_places of this SingleStatViewProperties.  # noqa: E501
        :rtype: DecimalPlaces
        """
        return self._decimal_places

    @decimal_places.setter
    def decimal_places(self, decimal_places):
        """Sets the decimal_places of this SingleStatViewProperties.


        :param decimal_places: The decimal_places of this SingleStatViewProperties.  # noqa: E501
        :type: DecimalPlaces
        """
        if decimal_places is None:
            raise ValueError("Invalid value for `decimal_places`, must not be `None`")  # noqa: E501

        self._decimal_places = decimal_places

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
        if not isinstance(other, SingleStatViewProperties):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        return not self == other
