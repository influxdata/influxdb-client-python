from influxdb2 import Authorization, AuthorizationsApi


class AuthorizationsClient(object):

    def __init__(self, influxdb_client):
        self._influxdb_client = influxdb_client
        self._authorizations_client = AuthorizationsApi(influxdb_client.api_client)

    def create_authorization(self, org_id=None, permissions: list = None, authorization: Authorization = None):
        """
        Creates an authorization
        :type permissions: list of Permission
        :param org_id: organization id
        :param permissions: list of permissions
        :type authorization: authorization object

        """
        if authorization is not None:
            return self._authorizations_client.post_authorizations(authorization=authorization)

            # if org_id is not None and permissions is not None:
        authorization = Authorization(org_id=org_id, permissions=permissions)
        return self._authorizations_client.post_authorizations(authorization=authorization)
