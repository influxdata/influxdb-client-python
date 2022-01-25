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
from influxdb_client.domain.expression import Expression


class ParenExpression(Expression):
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
        'expression': 'Expression'
    }

    attribute_map = {
        'type': 'type',
        'expression': 'expression'
    }

    def __init__(self, type=None, expression=None):  # noqa: E501,D401,D403
        """ParenExpression - a model defined in OpenAPI."""  # noqa: E501
        Expression.__init__(self)  # noqa: E501

        self._type = None
        self._expression = None
        self.discriminator = None

        if type is not None:
            self.type = type
        if expression is not None:
            self.expression = expression

    @property
    def type(self):
        """Get the type of this ParenExpression.

        Type of AST node

        :return: The type of this ParenExpression.
        :rtype: str
        """  # noqa: E501
        return self._type

    @type.setter
    def type(self, type):
        """Set the type of this ParenExpression.

        Type of AST node

        :param type: The type of this ParenExpression.
        :type: str
        """  # noqa: E501
        self._type = type

    @property
    def expression(self):
        """Get the expression of this ParenExpression.

        :return: The expression of this ParenExpression.
        :rtype: Expression
        """  # noqa: E501
        return self._expression

    @expression.setter
    def expression(self, expression):
        """Set the expression of this ParenExpression.

        :param expression: The expression of this ParenExpression.
        :type: Expression
        """  # noqa: E501
        self._expression = expression

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
        if not isinstance(other, ParenExpression):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Return true if both objects are not equal."""
        return not self == other
