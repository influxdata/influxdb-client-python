"""
A bucket is a named location where time series data is stored.

All buckets have a retention policy, a duration of time that each data point persists.
A bucket belongs to an organization.
"""
import warnings
from influxdb_client.rest import ApiException
from influxdb_client import BucketsService, Bucket, PostBucketRequest, PatchBucketRequest
from influxdb_client.client.util.helpers import get_org_query_param


class BucketsApi(object):
    """Implementation for '/api/v2/buckets' endpoint."""

    def __init__(self, influxdb_client):
        """Initialize defaults."""
        self._influxdb_client = influxdb_client
        self._buckets_service = BucketsService(influxdb_client.api_client)

    def create_bucket(self, bucket=None, bucket_name=None, org_id=None, retention_rules=None,
                      description=None, org=None) -> Bucket:
        """Create a bucket. Database creation via v1 API as fallback.

        :param Bucket|PostBucketRequest bucket: bucket to create
        :param bucket_name: bucket name
        :param description: bucket description
        :param org_id: org_id
        :param retention_rules: retention rules array or single BucketRetentionRules
        :param str, Organization org: specifies the organization for create the bucket;
                                      Take the ``ID``, ``Name`` or ``Organization``.
                                      If not specified the default value from ``InfluxDBClient.org`` is used.
        :return: Bucket
                 If the method is called asynchronously,
                 returns the request thread.
        """
        if self._buckets_service._is_below_v2():
            # Fall back to v1 API if buckets are not supported
            warnings.warn("InfluxDB versions below v2.0 are deprecated. " + \
                          "Falling back to CREATE DATABASE statement", DeprecationWarning)
            database_name = bucket_name if bucket_name is not None else bucket
            return self._create_database(database=database_name)

        if retention_rules is None:
            retention_rules = []

        rules = []

        if isinstance(retention_rules, list):
            rules.extend(retention_rules)
        else:
            rules.append(retention_rules)

        if org_id is not None:
            warnings.warn("org_id is deprecated; use org", DeprecationWarning)

        if bucket is None:
            bucket = PostBucketRequest(name=bucket_name,
                                       retention_rules=rules,
                                       description=description,
                                       org_id=get_org_query_param(org=(org_id if org is None else org),
                                                                  client=self._influxdb_client,
                                                                  required_id=True))

        return self._buckets_service.post_buckets(post_bucket_request=bucket)

    def _create_database(self, database=None):
        """Create a database at the v1 api (legacy).

        :param database_name: name of the new database
        :return: tuple(response body, status code, header dict)
        """
        if database is None:
            raise ValueError("Invalid value for `database`, must be defined.")

        # Hedaer and local_var_params for standard procedures only
        header_params = {}
        header_params['Accept'] = self._influxdb_client.api_client.select_header_accept(
            ['application/json'])
        header_params['Content-Type'] = self._influxdb_client.api_client.select_header_content_type(
            ['application/json'])
        local_var_params = locals()
        local_var_params['kwargs'] = {}
        all_params = []
        self._buckets_service._check_operation_params(
            "create_database", all_params, local_var_params
        )

        return self._influxdb_client.api_client.call_api(
            '/query', 'POST',
            header_params=header_params,
            path_params={}, post_params=[],
            files={}, auth_settings=[], collection_formats={},
            query_params={'q': f'CREATE DATABASE {database}'},
            async_req=local_var_params.get('async_req'),
            _return_http_data_only=local_var_params.get('_return_http_data_only'),  # noqa: E501
            _preload_content=local_var_params.get('_preload_content', True),
            _request_timeout=local_var_params.get('_request_timeout'),
            urlopen_kw=None
        )

    def update_bucket(self, bucket: Bucket) -> Bucket:
        """Update a bucket.

        :param bucket: Bucket update to apply (required)
        :return: Bucket
        """
        request = PatchBucketRequest(name=bucket.name,
                                     description=bucket.description,
                                     retention_rules=bucket.retention_rules)

        return self._buckets_service.patch_buckets_id(bucket_id=bucket.id, patch_bucket_request=request)

    def delete_bucket(self, bucket):
        """Delete a bucket. Delete a database via v1 API as fallback.

        :param bucket: bucket id or Bucket
        :return: Bucket
        """
        if isinstance(bucket, Bucket):
            bucket_id = bucket.id
        else:
            bucket_id = bucket

        if self._buckets_service._is_below_v2():
            # Fall back to v1 API if buckets are not supported
            warnings.warn("InfluxDB versions below v2.0 are deprecated. " + \
                          "Falling back to DROP DATABASE statement", DeprecationWarning)
            return self._delete_database(database=bucket_id)

        return self._buckets_service.delete_buckets_id(bucket_id=bucket_id)

    def _delete_database(self, database=None):
        """Delete a database at the v1 api (legacy).

        :param database_name: name of the database to delete
        :return: tuple(response body, status code, header dict)
        """
        if database is None:
            raise ValueError("Invalid value for `database`, must be defined.")

        # Hedaer and local_var_params for standard procedures only
        header_params = {}
        header_params['Accept'] = self._influxdb_client.api_client.select_header_accept(
            ['application/json'])
        header_params['Content-Type'] = self._influxdb_client.api_client.select_header_content_type(
            ['application/json'])
        local_var_params = locals()
        local_var_params['kwargs'] = {}
        all_params = []
        self._buckets_service._check_operation_params(
            "drop_database", all_params, local_var_params
        )

        return self._influxdb_client.api_client.call_api(
            '/query', 'POST',
            header_params=header_params,
            path_params={}, post_params=[],
            files={}, auth_settings=[], collection_formats={},
            query_params={'q': f'DROP DATABASE {database}'},
            async_req=local_var_params.get('async_req'),
            _return_http_data_only=local_var_params.get('_return_http_data_only'),
            _preload_content=local_var_params.get('_preload_content', True),
            _request_timeout=local_var_params.get('_request_timeout'),
            urlopen_kw=None
        )

    def find_bucket_by_id(self, id):
        """Find bucket by ID.

        :param id:
        :return:
        """
        return self._buckets_service.get_buckets_id(id)

    def find_bucket_by_name(self, bucket_name):
        """Find bucket by name.

        :param bucket_name: bucket name
        :return: Bucket
        """
        buckets = self._buckets_service.get_buckets(name=bucket_name)

        if len(buckets.buckets) > 0:
            return buckets.buckets[0]
        else:
            return None

    def find_buckets(self, **kwargs):
        """List buckets.

        :key int offset: Offset for pagination
        :key int limit: Limit for pagination
        :key str after: The last resource ID from which to seek from (but not including).
                        This is to be used instead of `offset`.
        :key str org: The organization name.
        :key str org_id: The organization ID.
        :key str name: Only returns buckets with a specific name.
        :return: Buckets
        """
        return self._buckets_service.get_buckets(**kwargs)
