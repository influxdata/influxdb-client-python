"""
An organization is a workspace for a group of users.

All dashboards, tasks, buckets, members, etc., belong to an organization.
"""

from influxdb_client import OrganizationsService, UsersService, Organization, PatchOrganizationRequest


class OrganizationsApi(object):
    """Implementation for '/api/v2/orgs' endpoint."""

    def __init__(self, influxdb_client):
        """Initialize defaults."""
        self._influxdb_client = influxdb_client
        self._organizations_service = OrganizationsService(influxdb_client.api_client)
        self._users_service = UsersService(influxdb_client.api_client)

    def me(self):
        """Return the current authenticated user."""
        user = self._users_service.get_me()
        return user

    def find_organization(self, org_id):
        """Retrieve an organization."""
        return self._organizations_service.get_orgs_id(org_id=org_id)

    def find_organizations(self, **kwargs):
        """
        List all organizations.

        :key int offset: Offset for pagination
        :key int limit: Limit for pagination
        :key bool descending:
        :key str org: Filter organizations to a specific organization name.
        :key str org_id: Filter organizations to a specific organization ID.
        :key str user_id: Filter organizations to a specific user ID.
        """
        return self._organizations_service.get_orgs(**kwargs).orgs

    def create_organization(self, name: str = None, organization: Organization = None) -> Organization:
        """Create an organization."""
        if organization is None:
            organization = Organization(name=name)
        return self._organizations_service.post_orgs(post_organization_request=organization)

    def update_organization(self, organization: Organization) -> Organization:
        """Update an organization.

        :param organization: Organization update to apply (required)
        :return: Organization
        """
        request = PatchOrganizationRequest(name=organization.name,
                                           description=organization.description)

        return self._organizations_service.patch_orgs_id(org_id=organization.id, patch_organization_request=request)

    def delete_organization(self, org_id: str):
        """Delete an organization."""
        return self._organizations_service.delete_orgs_id(org_id=org_id)
