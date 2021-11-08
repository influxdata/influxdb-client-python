from tests.base_test import BaseTest, generate_name


class OrganizationsApiTests(BaseTest):

    def setUp(self) -> None:
        super(OrganizationsApiTests, self).setUp()
        organizations_api = self.client.organizations_api()
        organizations = organizations_api.find_organizations()

        for organization in organizations:
            if organization.name.endswith("_IT"):
                print("Delete organization: ", organization.name)
                organizations_api.delete_organization(org_id=organization.id)

    def test_update_organization(self):
        organizations_api = self.client.organizations_api()

        organization = organizations_api.create_organization(name=generate_name(key='org'))
        self.assertEqual("", organization.description)

        organization.description = "updated description"
        organization = organizations_api.update_organization(organization=organization)
        self.assertEqual("updated description", organization.description)
