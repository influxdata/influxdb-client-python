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


class TemplateSummaryDiffLabels(object):
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
        'state_status': 'str',
        'kind': 'TemplateKind',
        'id': 'str',
        'template_meta_name': 'str',
        'new': 'TemplateSummaryDiffLabelsNewOld',
        'old': 'TemplateSummaryDiffLabelsNewOld'
    }

    attribute_map = {
        'state_status': 'stateStatus',
        'kind': 'kind',
        'id': 'id',
        'template_meta_name': 'templateMetaName',
        'new': 'new',
        'old': 'old'
    }

    def __init__(self, state_status=None, kind=None, id=None, template_meta_name=None, new=None, old=None):  # noqa: E501,D401,D403
        """TemplateSummaryDiffLabels - a model defined in OpenAPI."""  # noqa: E501
        self._state_status = None
        self._kind = None
        self._id = None
        self._template_meta_name = None
        self._new = None
        self._old = None
        self.discriminator = None

        if state_status is not None:
            self.state_status = state_status
        if kind is not None:
            self.kind = kind
        if id is not None:
            self.id = id
        if template_meta_name is not None:
            self.template_meta_name = template_meta_name
        if new is not None:
            self.new = new
        if old is not None:
            self.old = old

    @property
    def state_status(self):
        """Get the state_status of this TemplateSummaryDiffLabels.

        :return: The state_status of this TemplateSummaryDiffLabels.
        :rtype: str
        """  # noqa: E501
        return self._state_status

    @state_status.setter
    def state_status(self, state_status):
        """Set the state_status of this TemplateSummaryDiffLabels.

        :param state_status: The state_status of this TemplateSummaryDiffLabels.
        :type: str
        """  # noqa: E501
        self._state_status = state_status

    @property
    def kind(self):
        """Get the kind of this TemplateSummaryDiffLabels.

        :return: The kind of this TemplateSummaryDiffLabels.
        :rtype: TemplateKind
        """  # noqa: E501
        return self._kind

    @kind.setter
    def kind(self, kind):
        """Set the kind of this TemplateSummaryDiffLabels.

        :param kind: The kind of this TemplateSummaryDiffLabels.
        :type: TemplateKind
        """  # noqa: E501
        self._kind = kind

    @property
    def id(self):
        """Get the id of this TemplateSummaryDiffLabels.

        :return: The id of this TemplateSummaryDiffLabels.
        :rtype: str
        """  # noqa: E501
        return self._id

    @id.setter
    def id(self, id):
        """Set the id of this TemplateSummaryDiffLabels.

        :param id: The id of this TemplateSummaryDiffLabels.
        :type: str
        """  # noqa: E501
        self._id = id

    @property
    def template_meta_name(self):
        """Get the template_meta_name of this TemplateSummaryDiffLabels.

        :return: The template_meta_name of this TemplateSummaryDiffLabels.
        :rtype: str
        """  # noqa: E501
        return self._template_meta_name

    @template_meta_name.setter
    def template_meta_name(self, template_meta_name):
        """Set the template_meta_name of this TemplateSummaryDiffLabels.

        :param template_meta_name: The template_meta_name of this TemplateSummaryDiffLabels.
        :type: str
        """  # noqa: E501
        self._template_meta_name = template_meta_name

    @property
    def new(self):
        """Get the new of this TemplateSummaryDiffLabels.

        :return: The new of this TemplateSummaryDiffLabels.
        :rtype: TemplateSummaryDiffLabelsNewOld
        """  # noqa: E501
        return self._new

    @new.setter
    def new(self, new):
        """Set the new of this TemplateSummaryDiffLabels.

        :param new: The new of this TemplateSummaryDiffLabels.
        :type: TemplateSummaryDiffLabelsNewOld
        """  # noqa: E501
        self._new = new

    @property
    def old(self):
        """Get the old of this TemplateSummaryDiffLabels.

        :return: The old of this TemplateSummaryDiffLabels.
        :rtype: TemplateSummaryDiffLabelsNewOld
        """  # noqa: E501
        return self._old

    @old.setter
    def old(self, old):
        """Set the old of this TemplateSummaryDiffLabels.

        :param old: The old of this TemplateSummaryDiffLabels.
        :type: TemplateSummaryDiffLabelsNewOld
        """  # noqa: E501
        self._old = old

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
        if not isinstance(other, TemplateSummaryDiffLabels):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Return true if both objects are not equal."""
        return not self == other
