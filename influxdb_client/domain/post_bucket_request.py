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


class PostBucketRequest(object):
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
        'org_id': 'str',
        'name': 'str',
        'description': 'str',
        'rp': 'str',
        'retention_rules': 'list[BucketRetentionRules]'
    }

    attribute_map = {
        'org_id': 'orgID',
        'name': 'name',
        'description': 'description',
        'rp': 'rp',
        'retention_rules': 'retentionRules'
    }

    def __init__(self, org_id=None, name=None, description=None, rp=None, retention_rules=None):  # noqa: E501
        """PostBucketRequest - a model defined in OpenAPI"""  # noqa: E501

        self._org_id = None
        self._name = None
        self._description = None
        self._rp = None
        self._retention_rules = None
        self.discriminator = None

        if org_id is not None:
            self.org_id = org_id
        self.name = name
        if description is not None:
            self.description = description
        if rp is not None:
            self.rp = rp
        self.retention_rules = retention_rules

    @property
    def org_id(self):
        """Gets the org_id of this PostBucketRequest.  # noqa: E501


        :return: The org_id of this PostBucketRequest.  # noqa: E501
        :rtype: str
        """
        return self._org_id

    @org_id.setter
    def org_id(self, org_id):
        """Sets the org_id of this PostBucketRequest.


        :param org_id: The org_id of this PostBucketRequest.  # noqa: E501
        :type: str
        """

        self._org_id = org_id

    @property
    def name(self):
        """Gets the name of this PostBucketRequest.  # noqa: E501


        :return: The name of this PostBucketRequest.  # noqa: E501
        :rtype: str
        """
        return self._name

    @name.setter
    def name(self, name):
        """Sets the name of this PostBucketRequest.


        :param name: The name of this PostBucketRequest.  # noqa: E501
        :type: str
        """
        if name is None:
            raise ValueError("Invalid value for `name`, must not be `None`")  # noqa: E501

        self._name = name

    @property
    def description(self):
        """Gets the description of this PostBucketRequest.  # noqa: E501


        :return: The description of this PostBucketRequest.  # noqa: E501
        :rtype: str
        """
        return self._description

    @description.setter
    def description(self, description):
        """Sets the description of this PostBucketRequest.


        :param description: The description of this PostBucketRequest.  # noqa: E501
        :type: str
        """

        self._description = description

    @property
    def rp(self):
        """Gets the rp of this PostBucketRequest.  # noqa: E501


        :return: The rp of this PostBucketRequest.  # noqa: E501
        :rtype: str
        """
        return self._rp

    @rp.setter
    def rp(self, rp):
        """Sets the rp of this PostBucketRequest.


        :param rp: The rp of this PostBucketRequest.  # noqa: E501
        :type: str
        """

        self._rp = rp

    @property
    def retention_rules(self):
        """Gets the retention_rules of this PostBucketRequest.  # noqa: E501

        Rules to expire or retain data.  No rules means data never expires.  # noqa: E501

        :return: The retention_rules of this PostBucketRequest.  # noqa: E501
        :rtype: list[BucketRetentionRules]
        """
        return self._retention_rules

    @retention_rules.setter
    def retention_rules(self, retention_rules):
        """Sets the retention_rules of this PostBucketRequest.

        Rules to expire or retain data.  No rules means data never expires.  # noqa: E501

        :param retention_rules: The retention_rules of this PostBucketRequest.  # noqa: E501
        :type: list[BucketRetentionRules]
        """
        if retention_rules is None:
            raise ValueError("Invalid value for `retention_rules`, must not be `None`")  # noqa: E501

        self._retention_rules = retention_rules

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
        if not isinstance(other, PostBucketRequest):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        return not self == other