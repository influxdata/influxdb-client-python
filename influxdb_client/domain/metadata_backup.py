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


class MetadataBackup(object):
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
        'kv': 'file',
        'sql': 'file',
        'buckets': 'list[BucketMetadataManifest]'
    }

    attribute_map = {
        'kv': 'kv',
        'sql': 'sql',
        'buckets': 'buckets'
    }

    def __init__(self, kv=None, sql=None, buckets=None):  # noqa: E501,D401,D403
        """MetadataBackup - a model defined in OpenAPI."""  # noqa: E501
        self._kv = None
        self._sql = None
        self._buckets = None
        self.discriminator = None

        self.kv = kv
        self.sql = sql
        self.buckets = buckets

    @property
    def kv(self):
        """Get the kv of this MetadataBackup.

        :return: The kv of this MetadataBackup.
        :rtype: file
        """  # noqa: E501
        return self._kv

    @kv.setter
    def kv(self, kv):
        """Set the kv of this MetadataBackup.

        :param kv: The kv of this MetadataBackup.
        :type: file
        """  # noqa: E501
        if kv is None:
            raise ValueError("Invalid value for `kv`, must not be `None`")  # noqa: E501
        self._kv = kv

    @property
    def sql(self):
        """Get the sql of this MetadataBackup.

        :return: The sql of this MetadataBackup.
        :rtype: file
        """  # noqa: E501
        return self._sql

    @sql.setter
    def sql(self, sql):
        """Set the sql of this MetadataBackup.

        :param sql: The sql of this MetadataBackup.
        :type: file
        """  # noqa: E501
        if sql is None:
            raise ValueError("Invalid value for `sql`, must not be `None`")  # noqa: E501
        self._sql = sql

    @property
    def buckets(self):
        """Get the buckets of this MetadataBackup.

        :return: The buckets of this MetadataBackup.
        :rtype: list[BucketMetadataManifest]
        """  # noqa: E501
        return self._buckets

    @buckets.setter
    def buckets(self, buckets):
        """Set the buckets of this MetadataBackup.

        :param buckets: The buckets of this MetadataBackup.
        :type: list[BucketMetadataManifest]
        """  # noqa: E501
        if buckets is None:
            raise ValueError("Invalid value for `buckets`, must not be `None`")  # noqa: E501
        self._buckets = buckets

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
        if not isinstance(other, MetadataBackup):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Return true if both objects are not equal."""
        return not self == other
