"""Labels are a way to add visual metadata to dashboards, tasks, and other items in the InfluxDB UI."""

from typing import List, Dict, Union

from influxdb_client import LabelsService, LabelCreateRequest, Label, LabelUpdate


class LabelsApi(object):
    """Implementation for '/api/v2/labels' endpoint."""

    def __init__(self, influxdb_client):
        """Initialize defaults."""
        self._influxdb_client = influxdb_client
        self._service = LabelsService(influxdb_client.api_client)

    def create_label(self, name: str, org_id: str, properties: Dict[str, str] = None) -> Label:
        """
        Create a new label.

        :param name: label name
        :param org_id: organization id
        :param properties: optional label properties
        :return: created label
        """
        label_request = LabelCreateRequest(org_id=org_id, name=name, properties=properties)
        return self._service.post_labels(label_create_request=label_request).label

    def update_label(self, label: Label):
        """
        Update an existing label name and properties.

        :param label: label
        :return: the updated label
        """
        label_update = LabelUpdate()
        label_update.properties = label.properties
        label_update.name = label.name
        return self._service.patch_labels_id(label_id=label.id, label_update=label_update).label

    def delete_label(self, label: Union[str, Label]):
        """
        Delete the label.

        :param label: label id or Label
        """
        label_id = None

        if isinstance(label, str):
            label_id = label

        if isinstance(label, Label):
            label_id = label.id

        return self._service.delete_labels_id(label_id=label_id)

    def clone_label(self, cloned_name: str, label: Label) -> Label:
        """
        Create the new instance of the label as a copy existing label.

        :param cloned_name: new label name
        :param label: existing label
        :return: clonned Label
        """
        cloned_properties = None
        if label.properties is not None:
            cloned_properties = label.properties.copy()

        return self.create_label(name=cloned_name, properties=cloned_properties, org_id=label.org_id)

    def find_labels(self, **kwargs) -> List['Label']:
        """
        Get all available labels.

        :key str org_id: The organization ID.

        :return: labels
        """
        return self._service.get_labels(**kwargs).labels

    def find_label_by_id(self, label_id: str):
        """
        Retrieve the label by id.

        :param label_id:
        :return: Label
        """
        return self._service.get_labels_id(label_id=label_id).label

    def find_label_by_org(self, org_id) -> List['Label']:
        """
        Get the list of all labels for given organization.

        :param org_id: organization id
        :return: list of labels
        """
        return self._service.get_labels(org_id=org_id).labels
