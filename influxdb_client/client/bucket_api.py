"""
A bucket is a named location where time series data is stored.

All buckets have a retention policy, a duration of time that each data point persists.
A bucket belongs to an organization.
"""
import warnings

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
        """Create a bucket.

        :param Bucket|PostBucketRequest bucket: bucket to create
        :param bucket_name: bucket name
        :param description: bucket description
        :param org_id: org_id
        :param bucket_name: bucket name
        :param retention_rules: retention rules array or single BucketRetentionRules
        :param str, Organization org: specifies the organization for create the bucket;
                                      Take the ``ID``, ``Name`` or ``Organization``.
                                      If not specified the default value from ``InfluxDBClient.org`` is used.
        :return: Bucket
                 If the method is called asynchronously,
                 returns the request thread.
        """
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
        """Delete a bucket.

        :param bucket: bucket id or Bucket
        :return: Bucket
        """
        if isinstance(bucket, Bucket):
            bucket_id = bucket.id
        else:
            bucket_id = bucket

        return self._buckets_service.delete_buckets_id(bucket_id=bucket_id)

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
