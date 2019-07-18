import datetime
import unittest

from influxdb2 import BucketRetentionRules
from influxdb2.rest import ApiException
from influxdb2_test.base_test import BaseTest, generate_bucket_name


class BucketsClientTest(BaseTest):

    def test_create_delete_bucket(self):
        my_org = self.client.find_my_org()

        bucket_name = generate_bucket_name()
        my_bucket = self.buckets_client.create_bucket(bucket_name=bucket_name, org_id=my_org.id)
        self.assertEqual(my_bucket.name, bucket_name)
        self.assertEqual(my_bucket.org_id, my_org.id)
        print(my_bucket)

        result = self.buckets_client.find_bucket_by_id(my_bucket.id)
        print(result)
        self.assertEqual(my_bucket, result)

        self.buckets_client.delete_bucket(my_bucket.id)

        empty = None
        try:
            empty = self.buckets_client.find_bucket_by_id(my_bucket.id)
            self.fail("bucket is not deleted!")
        except ApiException as e:
            self.assertIsNone(empty)
            print("error: ", e)
            pass

    def test_find_by_name(self):
        my_org = self.client.find_my_org()

        bucket_name = generate_bucket_name()
        my_bucket = self.buckets_client.create_bucket(bucket_name=bucket_name, org_id=my_org.id)

        bucket_by_name = self.buckets_client.find_bucket_by_name(bucket_name=my_bucket.name)

        self.assertEqual(my_bucket, bucket_by_name)

        none = self.buckets_client.find_bucket_by_name(bucket_name="non-existent-bucket")
        self.assertIsNone(none)

    def test_create_bucket_retention(self):
        my_org = self.client.find_my_org()

        bucket_name = generate_bucket_name()

        retention = BucketRetentionRules(type="expire", every_seconds=3600)
        my_bucket = self.buckets_client.create_bucket(bucket_name=bucket_name, org_id=my_org.id,
                                                      retention_rules=retention)
        print(my_bucket)

    def test_create_bucket_retention_list(self):
        my_org = self.client.find_my_org()

        bucket_name = generate_bucket_name()

        retention = BucketRetentionRules
        ret_list = []
        retention.every_seconds = 3600
        retention.type = "expire"
        ret_list.append(retention)

        my_bucket = self.buckets_client.create_bucket(bucket_name=bucket_name, org_id=my_org.id,
                                                      retention_rules=ret_list)
        print(my_bucket)


if __name__ == '__main__':
    unittest.main()
