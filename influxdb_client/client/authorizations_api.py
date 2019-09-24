from influxdb_client import Authorization, AuthorizationsService, User, Organization


class AuthorizationsApi(object):

    def __init__(self, influxdb_client):
        self._influxdb_client = influxdb_client
        self._authorizations_service = AuthorizationsService(influxdb_client.api_client)

    def create_authorization(self, org_id=None, permissions: list = None,
                             authorization: Authorization = None) -> Authorization:
        """
        Creates an authorization
        :type permissions: list of Permission
        :param org_id: organization id
        :param permissions: list of permissions
        :type authorization: authorization object

        """
        if authorization is not None:
            return self._authorizations_service.post_authorizations(authorization=authorization)

            # if org_id is not None and permissions is not None:
        authorization = Authorization(org_id=org_id, permissions=permissions)
        return self._authorizations_service.post_authorizations(authorization=authorization)

    def find_authorization_by_id(self, auth_id: str) -> Authorization:
        """
        Finds authorization by id
        :param auth_id: authorization id
        :return: Authorization
        """
        return self._authorizations_service.get_authorizations_id(auth_id=auth_id)

    def find_authorizations(self, **kwargs):
        """
        Gets a list of all authorizations
        :param str user_id: filter authorizations belonging to a user id
        :param str user: filter authorizations belonging to a user name
        :param str org_id: filter authorizations belonging to a org id
        :param str org: filter authorizations belonging to a org name
        :return: Authorizations
        """
        authorizations = self._authorizations_service.get_authorizations(**kwargs)

        return authorizations.authorizations

    def find_authorizations_by_user(self, user: User):
        """
        Finds authorization by User
        :return: Authorization list
        """
        return self.find_authorizations(user_id=user.id)

    def find_authorizations_by_user_id(self, user_id: str):
        """
        Finds authorization by user id
        :return: Authorization list
        """
        return self.find_authorizations(user_id=user_id)

    def find_authorizations_by_user_name(self, user_name: str):
        """
        Finds authorization by user name
        :return: Authorization list
        """
        return self.find_authorizations(user=user_name)

    def find_authorizations_by_org(self, org: Organization):
        """
        Finds authorization by user name
        :return: Authorization list
        """

        if isinstance(org, Organization):
            return self.find_authorizations(org_id=org.id)

    def find_authorizations_by_org_name(self, org_name: str):
        """
        Finds authorization by org name
        :return: Authorization list
        """
        return self.find_authorizations(org=org_name)

    def find_authorizations_by_org_id(self, org_id: str):
        """
        Finds authorization by org id
        :return: Authorization list
        """
        return self.find_authorizations(org_id=org_id)

    def update_authorization(self, auth):
        """
        Updates authorization object
        :param auth:
        :return:
        """
        return self._authorizations_service.patch_authorizations_id(auth_id=auth.id, authorization_update_request=auth)

    def clone_authorization(self, auth) -> Authorization:

        if isinstance(auth, Authorization):
            cloned = Authorization(org_id=auth.org_id, permissions=auth.permissions)
            # cloned.description = auth.description
            # cloned.status = auth.status
            return self.create_authorization(authorization=cloned)

        if isinstance(auth, str):
            authorization = self.find_authorization_by_id(auth)
            return self.clone_authorization(auth=authorization)

        raise ValueError("Invalid argument")

    def delete_authorization(self, auth):
        if isinstance(auth, Authorization):
            return self._authorizations_service.delete_authorizations_id(auth_id=auth.id)

        if isinstance(auth, str):
            return self._authorizations_service.delete_authorizations_id(auth_id=auth)
        raise ValueError("Invalid argument")
