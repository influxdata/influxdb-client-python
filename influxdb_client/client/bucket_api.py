from influxdb_client import BucketsService, Bucket, PostBucketRequest


class BucketsApi(object):

    def __init__(self, influxdb_client):
        self._influxdb_client = influxdb_client
        self._buckets_service = BucketsService(influxdb_client.api_client)

    def create_bucket(self, bucket=None, bucket_name=None, org_id=None, retention_rules=None,
                      description=None) -> Bucket:
        """Create a bucket  # noqa: E501


        :param Bucket bucket: bucket to create (required)
        :param bucket_name: bucket name
        :param description: bucket description
        :param org_id: org_id
        :param bucket_name: bucket name
        :param retention_rules: retention rules array or single BucketRetentionRules
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

        if bucket is None:

            bucket = PostBucketRequest(name=bucket_name, retention_rules=rules, description=description)

            if org_id is None:
                org_id = self._influxdb_client.org
            bucket.org_id = org_id

        return self._buckets_service.post_buckets(post_bucket_request=bucket)

    def delete_bucket(self, bucket):
        """Delete a bucket  # noqa: E501

        :param bucket: bucket id or Bucket
        :return: Bucket
                 If the method is called asynchronously,
                 returns the request thread.
        """

        if isinstance(bucket, Bucket):
            bucket_id = bucket.id
        else:
            bucket_id = bucket

        return self._buckets_service.delete_buckets_id(bucket_id=bucket_id)

    def find_bucket_by_id(self, id):
        """Find bucket by ID

        :param id:
        :return:
        """
        return self._buckets_service.get_buckets_id(id)

    def find_bucket_by_name(self, bucket_name):
        """Find bucket by name

        :param bucket_name: bucket name
        :return: Bucket
        """

        buckets = self._buckets_service.get_buckets(name=bucket_name)

        if len(buckets.buckets) > 0:
            return buckets.buckets[0]
        else:
            return None

    def find_buckets(self):
        """Gets all buckets
        """
        return self._buckets_service.get_buckets()
