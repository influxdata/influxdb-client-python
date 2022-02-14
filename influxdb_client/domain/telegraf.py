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
from influxdb_client.domain.telegraf_request import TelegrafRequest


class Telegraf(TelegrafRequest):
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
        'id': 'str',
        'links': 'object',
        'labels': 'list[Label]',
        'name': 'str',
        'description': 'str',
        'metadata': 'TelegrafRequestMetadata',
        'config': 'str',
        'org_id': 'str'
    }

    attribute_map = {
        'id': 'id',
        'links': 'links',
        'labels': 'labels',
        'name': 'name',
        'description': 'description',
        'metadata': 'metadata',
        'config': 'config',
        'org_id': 'orgID'
    }

    def __init__(self, id=None, links=None, labels=None, name=None, description=None, metadata=None, config=None, org_id=None):  # noqa: E501,D401,D403
        """Telegraf - a model defined in OpenAPI."""  # noqa: E501
        TelegrafRequest.__init__(self, name=name, description=description, metadata=metadata, config=config, org_id=org_id)  # noqa: E501

        self._id = None
        self._links = None
        self._labels = None
        self.discriminator = None

        if id is not None:
            self.id = id
        if links is not None:
            self.links = links
        if labels is not None:
            self.labels = labels

    @property
    def id(self):
        """Get the id of this Telegraf.

        :return: The id of this Telegraf.
        :rtype: str
        """  # noqa: E501
        return self._id

    @id.setter
    def id(self, id):
        """Set the id of this Telegraf.

        :param id: The id of this Telegraf.
        :type: str
        """  # noqa: E501
        self._id = id

    @property
    def links(self):
        """Get the links of this Telegraf.

        :return: The links of this Telegraf.
        :rtype: object
        """  # noqa: E501
        return self._links

    @links.setter
    def links(self, links):
        """Set the links of this Telegraf.

        :param links: The links of this Telegraf.
        :type: object
        """  # noqa: E501
        self._links = links

    @property
    def labels(self):
        """Get the labels of this Telegraf.

        :return: The labels of this Telegraf.
        :rtype: list[Label]
        """  # noqa: E501
        return self._labels

    @labels.setter
    def labels(self, labels):
        """Set the labels of this Telegraf.

        :param labels: The labels of this Telegraf.
        :type: list[Label]
        """  # noqa: E501
        self._labels = labels

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
        if not isinstance(other, Telegraf):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Return true if both objects are not equal."""
        return not self == other
