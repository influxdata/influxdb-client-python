from influxdb2 import PermissionResource, Permission, Task
from influxdb2.client.influxdb_client import InfluxDBClient
from influxdb2_test.base_test import BaseTest


class TasksApiTest(BaseTest):
    TASK_FLUX = 'from(bucket: "my-bucket") |> range(start: -2m) |> last()'

    def setUp(self) -> None:
        super(TasksApiTest, self).setUp()

        self.organization = self.find_my_org()
        self.authorization = self.add_tasks_authorization(self.organization)
        self.client.close()

        self.client = InfluxDBClient(self.host, self.authorization.token, debug=self.conf.debug, org=self.org)
        self.tasks_api = self.client.tasks_api()

        # self.tasks_api.find_tasks()

    def add_tasks_authorization(self, organization):
        resource = PermissionResource(org=organization.name, type="tasks")

        create_task = Permission(resource=resource, action="read")
        delete_task = Permission(resource=resource, action="write")

        org_resource = PermissionResource(type="orgs")
        create_org = Permission(resource=org_resource, action="write")
        read_org = Permission(resource=org_resource, action="read")

        user_resource = PermissionResource(type="users")
        create_users = Permission(resource=user_resource, action="write")

        label_resource = PermissionResource(type="labels")
        create_labels = Permission(resource=label_resource, action="write")

        auth_resource = PermissionResource(type="authorizations")
        create_auth = Permission(resource=auth_resource, action="write")

        bucket = self.client.buckets_api().find_bucket_by_name("my-bucket")
        bucket_resource = PermissionResource(org_id=organization.id, id=bucket.id, type="buckets")
        read_bucket = Permission(resource=bucket_resource, action="read")
        write_bucket = Permission(resource=bucket_resource, action="write")

        return self.client.authorizations_api().create_authorization(org_id=organization.id,
                                                                     permissions=[create_task, delete_task, create_org,
                                                                                  read_org, create_users, create_labels,
                                                                                  create_auth, read_bucket,

                                                                                  write_bucket])

    def test_create_task(self):
        task_name = self.generate_name("it_task")

        flux = \
            '''option task = {{ 
                name: "{task_name}",
                every: 1h
            }}
            {flux}
            '''.format(task_name=task_name, flux=self.TASK_FLUX)

        task = Task(id=0, name=task_name, org_id=self.organization.id, flux=flux, status="active",
                    description="Task Description")

        task = self.tasks_api.create_task(task)

        print(task)

        self.assertIsNotNone(task)
        self.assertGreater(len(task.id), 1)

        self.assertEqual(task.name, task_name)
        self.assertEqual(task.org_id, self.organization.id)
        self.assertEqual(task.status, "active")
        self.assertEqual(task.every, "1h")
        self.assertEqual(task.cron, None)
        self.assertEqualIgnoringWhitespace(task.flux, flux)

        self.assertEqual(task.description, "Task Description")

    def test_create_task_with_offset(self):
        task_name = self.generate_name("it_task")

        flux = \
            '''option task = {{ 
                name: "{task_name}",
                every: 1h,
                offset: 30m
            }}
            {flux}
            '''.format(task_name=task_name, flux=self.TASK_FLUX)

        task = Task(id=0, name=task_name, org_id=self.organization.id, flux=flux, status="active",
                    description="Task Description")

        task = self.tasks_api.create_task(task)

        print(task)

        self.assertIsNotNone(task)
        self.assertEqual(task.offset, "30m")

    def test_create_task_every(self):
        task_name = self.generate_name("it_task")
        task = self.tasks_api.create_task_every(task_name, self.TASK_FLUX, "1h", self.organization)
        print(task)

        self.assertIsNotNone(task)
        self.assertGreater(len(task.id), 1)

        self.assertEqual(task.name, task_name)
        self.assertEqual(task.org_id, self.organization.id)
        self.assertEqual(task.status, "active")
        self.assertEqual(task.every, "1h")
        self.assertEqual(task.cron, None)
        self.assertTrue(task.flux.endswith(self.TASK_FLUX))
