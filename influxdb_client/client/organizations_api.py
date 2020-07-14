"""
An organization is a workspace for a group of users.

All dashboards, tasks, buckets, members, etc., belong to an organization.
"""


from influxdb_client import OrganizationsService, UsersService, Organization


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

    def find_organizations(self):
        """List all organizations."""
        return self._organizations_service.get_orgs().orgs

    def create_organization(self, name: str = None, organization: Organization = None) -> Organization:
        """Create an organization."""
        if organization is None:
            organization = Organization(name=name)
        return self._organizations_service.post_orgs(organization=organization)

    def delete_organization(self, org_id: str):
        """Delete an organization."""
        return self._organizations_service.delete_orgs_id(org_id=org_id)
