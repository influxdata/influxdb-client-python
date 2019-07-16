# coding: utf-8

from __future__ import absolute_import

import unittest

from influxdb2 import QueryApi, OrganizationsApi
from influxdb2_test.base_test import BaseTest


class SimpleWriteTest(BaseTest):

    def test_post_write(self):
        self.client.write_client().write(org="my-org", bucket="my-bucket", record="air,location=Python humidity=99")

    def test_WriteRecordsList(self):
        _bucketName = "test_bucket"
        _record1 = "h2o_feet,location=coyote_creek level\\ water_level=1.0 1"
        _record2 = "h2o_feet,location=coyote_creek level\\ water_level=2.0 2"

        self.client.write_client().write(self.org, _bucketName, _record1)
        self.client.write_client().write(self.org, _bucketName, _record2)

        self.client.write_client().flush()

        query = 'from(bucket:"' + _bucketName + '") |> range(start: 1970-01-01T00:00:00.000000001Z)'
        print(query)

        csv = self.client.query_client().query_csv(query)
        for line in csv:
            print(line)

        query_client = QueryApi(self.api_client)

        # query = await _queryApi.Query(
        #     "from(bucket:\"" + bucketName + "\") |> range(start: 1970-01-01T00:00:00.000000001Z)",
        #     _organization.Id);

        # Assert.AreEqual(1, query.Count);
        #
        # records = query[0].Records;
        # Assert.AreEqual(2, records.Count);
        #
        # Assert.AreEqual("h2o_feet", records[0].GetMeasurement());
        # Assert.AreEqual(1, records[0].GetValue());
        # Assert.AreEqual("level water_level", records[0].GetField());
        #
        # Assert.AreEqual("h2o_feet", records[1].GetMeasurement());
        # Assert.AreEqual(2, records[1].GetValue());
        # Assert.AreEqual("level water_level", records[1].GetField());

    def test_FindMyOrg(self):
        org_api = OrganizationsApi(self.api_client)
        orgs = org_api.post_orgs()
        print(orgs)


        # return (await Client.GetOrganizationsApi().FindOrganizations())
        # .First(organization= > organization.Name.Equals("my-org"));


if __name__ == '__main__':
    unittest.main()
