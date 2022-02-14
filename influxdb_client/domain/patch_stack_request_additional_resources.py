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


class PatchStackRequestAdditionalResources(object):
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
        'resource_id': 'str',
        'kind': 'str',
        'template_meta_name': 'str'
    }

    attribute_map = {
        'resource_id': 'resourceID',
        'kind': 'kind',
        'template_meta_name': 'templateMetaName'
    }

    def __init__(self, resource_id=None, kind=None, template_meta_name=None):  # noqa: E501,D401,D403
        """PatchStackRequestAdditionalResources - a model defined in OpenAPI."""  # noqa: E501
        self._resource_id = None
        self._kind = None
        self._template_meta_name = None
        self.discriminator = None

        self.resource_id = resource_id
        self.kind = kind
        if template_meta_name is not None:
            self.template_meta_name = template_meta_name

    @property
    def resource_id(self):
        """Get the resource_id of this PatchStackRequestAdditionalResources.

        :return: The resource_id of this PatchStackRequestAdditionalResources.
        :rtype: str
        """  # noqa: E501
        return self._resource_id

    @resource_id.setter
    def resource_id(self, resource_id):
        """Set the resource_id of this PatchStackRequestAdditionalResources.

        :param resource_id: The resource_id of this PatchStackRequestAdditionalResources.
        :type: str
        """  # noqa: E501
        if resource_id is None:
            raise ValueError("Invalid value for `resource_id`, must not be `None`")  # noqa: E501
        self._resource_id = resource_id

    @property
    def kind(self):
        """Get the kind of this PatchStackRequestAdditionalResources.

        :return: The kind of this PatchStackRequestAdditionalResources.
        :rtype: str
        """  # noqa: E501
        return self._kind

    @kind.setter
    def kind(self, kind):
        """Set the kind of this PatchStackRequestAdditionalResources.

        :param kind: The kind of this PatchStackRequestAdditionalResources.
        :type: str
        """  # noqa: E501
        if kind is None:
            raise ValueError("Invalid value for `kind`, must not be `None`")  # noqa: E501
        self._kind = kind

    @property
    def template_meta_name(self):
        """Get the template_meta_name of this PatchStackRequestAdditionalResources.

        :return: The template_meta_name of this PatchStackRequestAdditionalResources.
        :rtype: str
        """  # noqa: E501
        return self._template_meta_name

    @template_meta_name.setter
    def template_meta_name(self, template_meta_name):
        """Set the template_meta_name of this PatchStackRequestAdditionalResources.

        :param template_meta_name: The template_meta_name of this PatchStackRequestAdditionalResources.
        :type: str
        """  # noqa: E501
        self._template_meta_name = template_meta_name

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
        if not isinstance(other, PatchStackRequestAdditionalResources):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Return true if both objects are not equal."""
        return not self == other
