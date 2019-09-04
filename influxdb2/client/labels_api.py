from typing import List

from influxdb2 import LabelsService, LabelCreateRequest, Label, LabelUpdate


class LabelsApi(object):

    def __init__(self, influxdb_client):
        self._influxdb_client = influxdb_client
        self._service = LabelsService(influxdb_client.api_client)

    def create_label(self, name, org_id, properties=None) -> Label:
        label_request = LabelCreateRequest(org_id=org_id, name=name, properties=properties)
        return self._service.post_labels(label_create_request=label_request).label

    def update_label(self, label: Label):
        """
        Update a label
        :param label: label
        :return:
        """
        label_update = LabelUpdate()
        label_update.properties = label.properties
        label_update.name = label.name
        return self._service.patch_labels_id(label_id=label.id, label_update=label_update).label

    def delete_label(self, label):
        """
        Delete the label
        :param label: label id or Label
        :type label str or Label
        :return:
        """
        label_id = None

        if isinstance(label, str):
            label_id = label

        if isinstance(label, Label):
            label_id = label.id

        return self._service.delete_labels_id(label_id=label_id)

    def clone_label(self, cloned_name: str, label: Label) -> Label:
        cloned_properties = None
        if label.properties is not None:
            cloned_properties = label.properties.copy()

        return self.create_label(name=cloned_name, properties=cloned_properties, org_id=label.org_id)

    def find_labels(self) -> List['Label']:
        return self._service.get_labels().labels

    def find_label_by_id(self, label_id: str):
        return self._service.get_labels_id(label_id=label_id).label

    def find_label_by_org(self, org_id) -> List['Label']:

        return self._service.get_labels(org_id=org_id).labels
