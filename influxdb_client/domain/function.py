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


class Function(object):
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
        'name': 'str',
        'description': 'str',
        'org_id': 'str',
        'script': 'str',
        'language': 'FunctionLanguage',
        'url': 'str',
        'created_at': 'datetime',
        'updated_at': 'datetime'
    }

    attribute_map = {
        'id': 'id',
        'name': 'name',
        'description': 'description',
        'org_id': 'orgID',
        'script': 'script',
        'language': 'language',
        'url': 'url',
        'created_at': 'createdAt',
        'updated_at': 'updatedAt'
    }

    def __init__(self, id=None, name=None, description=None, org_id=None, script=None, language=None, url=None, created_at=None, updated_at=None):  # noqa: E501,D401,D403
        """Function - a model defined in OpenAPI."""  # noqa: E501
        self._id = None
        self._name = None
        self._description = None
        self._org_id = None
        self._script = None
        self._language = None
        self._url = None
        self._created_at = None
        self._updated_at = None
        self.discriminator = None

        if id is not None:
            self.id = id
        self.name = name
        if description is not None:
            self.description = description
        self.org_id = org_id
        self.script = script
        if language is not None:
            self.language = language
        if url is not None:
            self.url = url
        if created_at is not None:
            self.created_at = created_at
        if updated_at is not None:
            self.updated_at = updated_at

    @property
    def id(self):
        """Get the id of this Function.

        :return: The id of this Function.
        :rtype: str
        """  # noqa: E501
        return self._id

    @id.setter
    def id(self, id):
        """Set the id of this Function.

        :param id: The id of this Function.
        :type: str
        """  # noqa: E501
        self._id = id

    @property
    def name(self):
        """Get the name of this Function.

        :return: The name of this Function.
        :rtype: str
        """  # noqa: E501
        return self._name

    @name.setter
    def name(self, name):
        """Set the name of this Function.

        :param name: The name of this Function.
        :type: str
        """  # noqa: E501
        if name is None:
            raise ValueError("Invalid value for `name`, must not be `None`")  # noqa: E501
        self._name = name

    @property
    def description(self):
        """Get the description of this Function.

        :return: The description of this Function.
        :rtype: str
        """  # noqa: E501
        return self._description

    @description.setter
    def description(self, description):
        """Set the description of this Function.

        :param description: The description of this Function.
        :type: str
        """  # noqa: E501
        self._description = description

    @property
    def org_id(self):
        """Get the org_id of this Function.

        :return: The org_id of this Function.
        :rtype: str
        """  # noqa: E501
        return self._org_id

    @org_id.setter
    def org_id(self, org_id):
        """Set the org_id of this Function.

        :param org_id: The org_id of this Function.
        :type: str
        """  # noqa: E501
        if org_id is None:
            raise ValueError("Invalid value for `org_id`, must not be `None`")  # noqa: E501
        self._org_id = org_id

    @property
    def script(self):
        """Get the script of this Function.

        script is script to be executed

        :return: The script of this Function.
        :rtype: str
        """  # noqa: E501
        return self._script

    @script.setter
    def script(self, script):
        """Set the script of this Function.

        script is script to be executed

        :param script: The script of this Function.
        :type: str
        """  # noqa: E501
        if script is None:
            raise ValueError("Invalid value for `script`, must not be `None`")  # noqa: E501
        self._script = script

    @property
    def language(self):
        """Get the language of this Function.

        :return: The language of this Function.
        :rtype: FunctionLanguage
        """  # noqa: E501
        return self._language

    @language.setter
    def language(self, language):
        """Set the language of this Function.

        :param language: The language of this Function.
        :type: FunctionLanguage
        """  # noqa: E501
        self._language = language

    @property
    def url(self):
        """Get the url of this Function.

        invocation endpoint address

        :return: The url of this Function.
        :rtype: str
        """  # noqa: E501
        return self._url

    @url.setter
    def url(self, url):
        """Set the url of this Function.

        invocation endpoint address

        :param url: The url of this Function.
        :type: str
        """  # noqa: E501
        self._url = url

    @property
    def created_at(self):
        """Get the created_at of this Function.

        :return: The created_at of this Function.
        :rtype: datetime
        """  # noqa: E501
        return self._created_at

    @created_at.setter
    def created_at(self, created_at):
        """Set the created_at of this Function.

        :param created_at: The created_at of this Function.
        :type: datetime
        """  # noqa: E501
        self._created_at = created_at

    @property
    def updated_at(self):
        """Get the updated_at of this Function.

        :return: The updated_at of this Function.
        :rtype: datetime
        """  # noqa: E501
        return self._updated_at

    @updated_at.setter
    def updated_at(self, updated_at):
        """Set the updated_at of this Function.

        :param updated_at: The updated_at of this Function.
        :type: datetime
        """  # noqa: E501
        self._updated_at = updated_at

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
        if not isinstance(other, Function):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Return true if both objects are not equal."""
        return not self == other
