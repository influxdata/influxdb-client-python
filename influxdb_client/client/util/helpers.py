"""Functions to share utility across client classes."""
from influxdb_client.rest import ApiException


def _is_id(value):
    """
    Check if the value is valid InfluxDB ID.

    :param value: to check
    :return: True if provided parameter is valid InfluxDB ID.
    """
    if value and len(value) == 16:
        try:
            int(value, 16)
            return True
        except ValueError:
            return False
    return False


def get_org_query_param(org, client, required_id=False):
    """
    Get required type of Org query parameter.

    :param str, Organization org: value provided as a parameter into API (optional)
    :param InfluxDBClient client: with default value for Org parameter
    :param bool required_id: true if the query param has to be a ID
    :return: request type of org query parameter or None
    """
    _org = client.org if org is None else org
    if 'Organization' in type(_org).__name__:
        _org = _org.id
    if required_id and _org and not _is_id(_org):
        try:
            return client.organizations_api().find_organizations(org=_org)[0].id
        except ApiException:
            return None

    return _org
