import unittest

import pytest

from influxdb_client import BucketRetentionRules
from influxdb_client.rest import ApiException
from tests.base_test import BaseTest, generate_bucket_name


class BucketsClientTest(BaseTest):

    def setUp(self) -> None:
        super(BucketsClientTest, self).setUp()
        response = self.buckets_api.find_buckets()

        for bucket in response.buckets:
            if bucket.name.endswith("_IT"):
                print("Delete bucket: ", bucket.name)
                self.buckets_api.delete_bucket(bucket)

    def test_create_delete_bucket(self):
        my_org = self.find_my_org()

        bucket_name = generate_bucket_name()
        my_bucket = self.buckets_api.create_bucket(bucket_name=bucket_name, org=my_org)
        self.assertEqual(my_bucket.name, bucket_name)
        self.assertEqual(my_bucket.org_id, my_org.id)
        print(my_bucket)

        result = self.buckets_api.find_bucket_by_id(my_bucket.id)
        print(result)
        self.assertEqual(my_bucket, result)

        self.delete_test_bucket(my_bucket)

        with pytest.raises(ApiException) as e:
            assert self.buckets_api.find_bucket_by_id(my_bucket.id)
        assert "bucket not found" in e.value.body

    def test_find_by_name(self):
        my_org = self.find_my_org()

        bucket_name = generate_bucket_name()
        my_bucket = self.buckets_api.create_bucket(bucket_name=bucket_name, org=my_org)

        bucket_by_name = self.buckets_api.find_bucket_by_name(bucket_name=my_bucket.name)

        self.assertEqual(my_bucket, bucket_by_name)

        none = self.buckets_api.find_bucket_by_name(bucket_name="non-existent-bucket")
        self.assertIsNone(none)
        self.buckets_api.delete_bucket(my_bucket)

    def test_create_bucket_retention(self):
        my_org = self.find_my_org()

        bucket_name = generate_bucket_name()

        retention = BucketRetentionRules(type="expire", every_seconds=3600)
        desc = "bucket with retention"
        my_bucket = self.buckets_api.create_bucket(bucket_name=bucket_name, org=my_org,
                                                   retention_rules=retention, description=desc)

        self.assertEqual(my_bucket.description, desc)

        print(my_bucket)
        self.buckets_api.delete_bucket(my_bucket)

    def test_create_bucket_retention_list(self):
        my_org = self.find_my_org()

        bucket_name = generate_bucket_name()

        ret_list = []
        retention = BucketRetentionRules(every_seconds=3600)
        retention.type = "expire"
        ret_list.append(retention)

        my_bucket = self.buckets_api.create_bucket(bucket_name=bucket_name, org=my_org,
                                                   retention_rules=ret_list)

        self.assertEqual(my_bucket.name, bucket_name)

        self.delete_test_bucket(my_bucket)

    def test_find_buckets(self):
        my_org = self.find_my_org()
        buckets = self.buckets_api.find_buckets(limit=100).buckets
        size = len(buckets)

        # create 2 buckets
        self.buckets_api.create_bucket(bucket_name=generate_bucket_name(), org=my_org)
        self.buckets_api.create_bucket(bucket_name=generate_bucket_name(), org=my_org)

        buckets = self.buckets_api.find_buckets(limit=size + 2).buckets
        self.assertEqual(size + 2, len(buckets))

        # offset 1
        buckets = self.buckets_api.find_buckets(offset=1, limit=size + 2).buckets
        self.assertEqual(size + 1, len(buckets))

        # count 1
        buckets = self.buckets_api.find_buckets(limit=1).buckets
        self.assertEqual(1, len(buckets))

    def test_find_buckets_iter(self):
        def count_unique_ids(items):
          return len(set(map(lambda item: item.id, items)))

        my_org = self.find_my_org()
        more_buckets = 10
        num_of_buckets = count_unique_ids(self.buckets_api.find_buckets_iter()) + more_buckets
        
        a_bucket_name = None
        for _ in range(more_buckets):
          bucket_name = self.generate_name("it find_buckets_iter")
          self.buckets_api.create_bucket(bucket_name=bucket_name, org=my_org)
          a_bucket_name = bucket_name

        # get no buckets
        buckets = self.buckets_api.find_buckets_iter(name=a_bucket_name + "blah")
        self.assertEqual(count_unique_ids(buckets), 0)

        # get bucket by name
        buckets = self.buckets_api.find_buckets_iter(name=a_bucket_name)
        self.assertEqual(count_unique_ids(buckets), 1)

        # get buckets in 3-4 batches
        buckets = self.buckets_api.find_buckets_iter(limit=num_of_buckets // 3)
        self.assertEqual(count_unique_ids(buckets), num_of_buckets)

        # get buckets in one batch
        buckets = self.buckets_api.find_buckets_iter(limit=num_of_buckets)
        self.assertEqual(count_unique_ids(buckets), num_of_buckets)

        # get buckets in one batch, requesting too much
        buckets = self.buckets_api.find_buckets_iter(limit=num_of_buckets + 1)
        self.assertEqual(count_unique_ids(buckets), num_of_buckets)

        # skip some buckets 
        *_, skip_bucket = self.buckets_api.find_buckets(limit=num_of_buckets // 3).buckets
        buckets = self.buckets_api.find_buckets_iter(after=skip_bucket.id)
        self.assertEqual(count_unique_ids(buckets), num_of_buckets - num_of_buckets // 3)
        
    def test_update_bucket(self):
        my_org = self.find_my_org()

        bucket = self.buckets_api.create_bucket(bucket_name=generate_bucket_name(),
                                                org=my_org,
                                                description="my description")
        self.assertEqual("my description", bucket.description)

        bucket.description = "updated description"
        self.buckets_api.update_bucket(bucket=bucket)
        self.assertEqual("updated description", bucket.description)


if __name__ == '__main__':
    unittest.main()
