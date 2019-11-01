import pytest

from influxdb_client.rest import ApiException
from tests.base_test import BaseTest


class TestLabelsApi(BaseTest):

    def setUp(self) -> None:
        super(TestLabelsApi, self).setUp()
        self.organization = self.find_my_org()
        labels = self.labels_api.find_labels()
        for label in labels:
            self.labels_api.delete_label(label)

    def test_create_label_unicode(self):
        name = self.generate_name("Cool resource 🍺")
        properties = {"color": "red 🍺", "source": "remote api"}

        label = self.labels_api.create_label(name, self.organization.id, properties)
        self.assertIsNotNone(label)
        self.assertIsNotNone(label.id)
        self.assertEqual(label.name, name)
        self.assertEqual(label.properties["color"], "red 🍺")
        self.assertEqual(label.properties["source"], "remote api")

    def test_create_label_empty_properties(self):
        name = self.generate_name("Cool resource")
        label = self.labels_api.create_label(name=name, org_id=self.organization.id)
        self.assertIsNotNone(label)
        self.assertIsNotNone(label.id)
        self.assertEqual(label.name, name)
        self.assertEqual(label.properties, None)

    def test_update_label(self):
        label = self.labels_api.create_label(self.generate_name("Cool Resource"), org_id=self.organization.id)
        self.assertEqual(label.properties, None)

        label.properties = {"color": "blue"}

        label = self.labels_api.update_label(label)

        self.assertEqual(label.properties["color"], "blue")
        self.assertEqual(len(label.properties), 1)

        label.properties["type"] = "free"
        label = self.labels_api.update_label(label)
        self.assertEqual(label.properties["color"], "blue")
        self.assertEqual(label.properties["type"], "free")
        self.assertEqual(len(label.properties), 2)

        label.properties["type"] = "paid"
        label.properties["color"] = ""

        label = self.labels_api.update_label(label)
        self.assertEqual(label.properties["type"], "paid")
        self.assertEqual(len(label.properties), 1)

    def test_delete_label(self):
        created_label = self.labels_api.create_label(self.generate_name("Cool Resource"), org_id=self.organization.id)
        self.assertIsNotNone(created_label)

        found_label = self.labels_api.find_label_by_id(label_id=created_label.id)
        self.assertIsNotNone(found_label)

        self.labels_api.delete_label(created_label)

        with pytest.raises(ApiException) as e:
            assert self.labels_api.find_label_by_id(label_id=created_label.id)
        assert "label not found" in e.value.body

    def test_clone_label(self):
        name = self.generate_name("cloned")

        properties = {
            "color": "green",
            "location": "west"
        }

        label = self.labels_api.create_label(name, self.organization.id, properties)

        cloned = self.labels_api.clone_label(cloned_name=name+"_clone", label=label)
        self.assertEqual(cloned.name, name+"_clone")
        self.assertEqual(cloned.properties, properties)

    def test_find_label_by_id(self):
        label = self.labels_api.create_label(self.generate_name("Cool Resource"), org_id=self.organization.id)

        label_by_id = self.labels_api.find_label_by_id(label_id=label.id)

        self.assertEqual(label, label_by_id)

    def test_find_label_by_id_none(self):
        with pytest.raises(ApiException) as e:
            assert self.labels_api.find_label_by_id(label_id="020f755c3c082000")
        assert "label not found" in e.value.body

    def test_find_labels(self):
        size = len(self.labels_api.find_labels())
        self.labels_api.create_label(self.generate_name("Cool Resource"), org_id=self.organization.id)

        labels = self.labels_api.find_labels()
        self.assertEqual(len(labels), size + 1)

    def test_find_label_by_org(self):
        organization = self.organizations_api.create_organization(self.generate_name("org"))
        labels = self.labels_api.find_label_by_org(org_id=organization.id)
        self.assertEqual(len(labels), 0)

        self.labels_api.create_label(self.generate_name("Cool Resource"), org_id=organization.id)
        labels = self.labels_api.find_label_by_org(org_id=organization.id)
        self.assertEqual(len(labels), 1)

        self.organizations_api.delete_organization(organization.id)

        with pytest.raises(ApiException) as e:
            assert self.organizations_api.find_organization(org_id=organization.id)
        assert "organization not found" in e.value.body
