import datetime
import time

import pytest

from influxdb_client import PermissionResource, Permission, Task, InfluxDBClient
from influxdb_client.rest import ApiException
from tests.base_test import BaseTest

TASK_FLUX = 'from(bucket: "my-bucket") |> range(start: -2m) |> last()'


class TasksApiTest(BaseTest):

    def setUp(self) -> None:
        super(TasksApiTest, self).setUp()

        self.organization = self.find_my_org()
        self.authorization = self.add_tasks_authorization(self.organization)
        self.client.close()

        self.client = InfluxDBClient(self.host, self.authorization.token, debug=self.conf.debug)
        self.tasks_api = self.client.tasks_api()

        tasks = self.tasks_api.find_tasks()
        for task in tasks:
            if task.name.endswith("-IT"):
                self.tasks_api.delete_task(task.id)

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
            '''.format(task_name=task_name, flux=TASK_FLUX)

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
            '''.format(task_name=task_name, flux=TASK_FLUX)

        task = Task(id=0, name=task_name, org_id=self.organization.id, flux=flux, status="active",
                    description="Task Description")

        task = self.tasks_api.create_task(task)

        print(task)

        self.assertIsNotNone(task)
        self.assertEqual(task.offset, "30m")

    def test_create_task_every(self):
        task_name = self.generate_name("it_task")
        task = self.tasks_api.create_task_every(task_name, TASK_FLUX, "1h", self.organization)

        self.assertIsNotNone(task)
        self.assertGreater(len(task.id), 1)

        self.assertEqual(task.name, task_name)
        self.assertEqual(task.org_id, self.organization.id)
        self.assertEqual(task.status, "active")
        self.assertEqual(task.every, "1h")
        self.assertEqual(task.cron, None)
        self.assertTrue(task.flux.startswith(TASK_FLUX))

    def test_create_task_cron(self):
        task_name = self.generate_name("it task")
        task = self.tasks_api.create_task_cron(task_name, TASK_FLUX, "0 2 * * *", self.organization.id)

        self.assertIsNotNone(task)
        self.assertGreater(len(task.id), 1)

        self.assertEqual(task.name, task_name)
        self.assertEqual(task.org_id, self.organization.id)
        self.assertEqual(task.status, "active")
        self.assertEqual(task.every, None)
        self.assertEqual(task.cron, "0 2 * * *")
        # self.assertEqualIgnoringWhitespace(task.flux, flux)

        self.assertTrue(task.flux.startswith(TASK_FLUX))
        # self.assertEqual(task.links, "active")

        links = task.links
        self.assertIsNotNone(task.links)
        self.assertEqual(links.logs, "/api/v2/tasks/" + task.id + "/logs")
        self.assertEqual(links.members, "/api/v2/tasks/" + task.id + "/members")
        self.assertEqual(links.owners, "/api/v2/tasks/" + task.id + "/owners")
        self.assertEqual(links.runs, "/api/v2/tasks/" + task.id + "/runs")
        self.assertEqual(links._self, "/api/v2/tasks/" + task.id)

        # TODO missing get labels
        self.assertEqual(links.labels, "/api/v2/tasks/" + task.id + "/labels")

    def test_create_with_import(self):
        task_name = self.generate_name("it task")
        task_flux = 'import "http"\n\n' \
                    'from(bucket: "iot_center")\n' \
                    '    |> range(start: -30d)\n' \
                    '    |> filter(fn: (r) => r._measurement == "environment")\n' \
                    '    |> aggregateWindow(every: 1h, fn: mean)'
        task = self.tasks_api.create_task_cron(task_name, task_flux, "10 0 * * * *", self.organization.id)

        self.assertIsNotNone(task.id)
        self.assertEqual(task.name, task_name)
        self.assertEqual(task.org_id, self.organization.id)
        self.assertEqual(task.status, "active")
        self.assertEqual(task.cron, "10 0 * * * *")
        self.assertTrue(task.flux.startswith(task_flux))
        self.assertTrue(task.flux.splitlines()[-1].startswith('option task = '))

    def test_find_task_by_id(self):
        task_name = self.generate_name("it task")
        task = self.tasks_api.create_task_cron(task_name, TASK_FLUX, "0 2 * * *", self.organization.id)

        task_by_id = self.tasks_api.find_task_by_id(task.id)
        self.assertEqual(task, task_by_id)

    @pytest.mark.skip(reason="https://github.com/influxdata/influxdb/issues/11590")
    def test_find_task_by_user_id(self):
        task_user = self.users_api.create_user(self.generate_name("TaskUser"))
        self.tasks_api.create_task_cron(self.generate_name("it_task"), TASK_FLUX, "0 2 * * *",
                                        self.organization.id)
        tasks = self.tasks_api.find_tasks_by_user(task_user_id=task_user.id)
        print(tasks)
        self.assertEqual(len(tasks), 1)

    def test_find_tasks_iter(self):
        task_name = self.generate_name("it task")
        num_of_tasks = 10

        for _ in range(num_of_tasks):
            self.tasks_api.create_task_cron(task_name, TASK_FLUX, "0 2 * * *", self.organization.id)

        def count_unique_ids(tasks):
            return len(set(map(lambda task: task.id, tasks)))

        # get tasks in 3-4 batches
        tasks = self.tasks_api.find_tasks_iter(name= task_name, limit= num_of_tasks // 3)
        self.assertEqual(count_unique_ids(tasks), num_of_tasks)

        # get tasks in one equaly size batch
        tasks = self.tasks_api.find_tasks_iter(name= task_name, limit= num_of_tasks)
        self.assertEqual(count_unique_ids(tasks), num_of_tasks)

        # get tasks in one batch
        tasks = self.tasks_api.find_tasks_iter(name= task_name, limit= num_of_tasks + 1)
        self.assertEqual(count_unique_ids(tasks), num_of_tasks)

        # get no tasks
        tasks = self.tasks_api.find_tasks_iter(name= task_name + "blah")
        self.assertEqual(count_unique_ids(tasks), 0)

        # skip some tasks
        *_, split_task = self.tasks_api.find_tasks(name= task_name, limit= num_of_tasks // 3)
        tasks = self.tasks_api.find_tasks_iter(name= task_name, limit= 3, after= split_task.id)
        self.assertEqual(count_unique_ids(tasks), num_of_tasks - num_of_tasks // 3)

    def test_delete_task(self):
        task = self.tasks_api.create_task_cron(self.generate_name("it_task"), TASK_FLUX, "0 2 * * *",
                                               self.organization.id)
        self.assertIsNotNone(task)

        self.tasks_api.delete_task(task.id)
        with pytest.raises(ApiException) as e:
            assert self.tasks_api.find_task_by_id(task_id=task.id)
        assert "failed to find task" in e.value.body

    def test_update_task(self):
        task_name = self.generate_name("it task")
        cron_task = self.tasks_api.create_task_cron(task_name, TASK_FLUX, "0 2 * * *", self.organization.id)

        flux = '''
        {flux}
        
        option task = {{
            name: "{task_name}",
            every: 3m
        }}
        '''.format(task_name=task_name, flux=TASK_FLUX)

        cron_task.cron = None
        cron_task.every = "3m"
        cron_task.status = "inactive"
        cron_task.description = "Updated description"

        updated_task = self.tasks_api.update_task(cron_task)
        time.sleep(1)

        self.assertIsNotNone(updated_task)
        self.assertGreater(len(updated_task.id), 1)

        self.assertEqual(updated_task.name, task_name)
        self.assertEqual(updated_task.org_id, cron_task.org_id)
        self.assertEqual(updated_task.status, "inactive")
        self.assertEqual(updated_task.every, "3m")
        self.assertEqual(updated_task.cron, None)
        self.assertIsNotNone(updated_task.updated_at)
        now = datetime.datetime.now()
        now.astimezone()
        self.assertLess(updated_task.updated_at, now.astimezone(tz=datetime.timezone.utc))
        self.assertEqualIgnoringWhitespace(updated_task.flux, flux)

        self.assertEqual(updated_task.description, "Updated description")

    def test_member(self):
        task = self.tasks_api.create_task_cron(self.generate_name("it_task"), TASK_FLUX, "0 2 * * *",
                                               self.organization.id)
        members = self.tasks_api.get_members(task_id=task.id)
        self.assertEqual(len(members), 0)
        user = self.users_api.create_user(self.generate_name("Luke Health"))

        resource_member = self.tasks_api.add_member(member_id=user.id, task_id=task.id)
        self.assertIsNotNone(resource_member)
        self.assertEqual(resource_member.id, user.id)
        self.assertEqual(resource_member.name, user.name)
        self.assertEqual(resource_member.role, "member")

        members = self.tasks_api.get_members(task_id=task.id)
        resource_member = members[0]
        self.assertEqual(len(members), 1)
        self.assertEqual(resource_member.id, user.id)
        self.assertEqual(resource_member.name, user.name)
        self.assertEqual(resource_member.role, "member")

        self.tasks_api.delete_member(member_id=user.id, task_id=task.id)
        members = self.tasks_api.get_members(task_id=task.id)
        self.assertEqual(len(members), 0)

    @pytest.mark.skip(reason="https://github.com/influxdata/influxdb/issues/19234")
    def test_owner(self):
        task = self.tasks_api.create_task_cron(self.generate_name("it_task"), TASK_FLUX, "0 2 * * *",
                                               self.organization.id)
        owners = self.tasks_api.get_owners(task_id=task.id)
        self.assertEqual(len(owners), 1)

        user = self.users_api.create_user(self.generate_name("Luke Health"))
        resource_member = self.tasks_api.add_owner(owner_id=user.id, task_id=task.id)

        self.assertIsNotNone(resource_member)
        self.assertEqual(resource_member.id, user.id)
        self.assertEqual(resource_member.name, user.name)
        self.assertEqual(resource_member.role, "owner")

        owners = self.tasks_api.get_owners(task_id=task.id)
        self.assertEqual(len(owners), 2)
        resource_member = owners[1]
        self.assertEqual(resource_member.id, user.id)
        self.assertEqual(resource_member.name, user.name)
        self.assertEqual(resource_member.role, "owner")

        self.tasks_api.delete_owner(owner_id=user.id, task_id=task.id)
        owners = self.tasks_api.get_owners(task_id=task.id)
        self.assertEqual(len(owners), 1)

    def test_runs(self):
        task_name = self.generate_name("it task")
        task = self.tasks_api.create_task_every(task_name, TASK_FLUX, "1s", self.organization)
        time.sleep(5)

        runs = self.tasks_api.get_runs(task_id=task.id, limit=10)
        self.assertGreater(len(runs), 2)

        success_runs = list(filter(lambda x: x.status == "success", runs))
        run = success_runs[0]
        self.assertIsNotNone(run.id)
        self.assertEqual(run.task_id, task.id)
        self.assertEqual(run.status, "success")
        now = datetime.datetime.now()
        self.assertLess(run.scheduled_for, now.astimezone(tz=datetime.timezone.utc))
        self.assertLess(run.started_at, now.astimezone(tz=datetime.timezone.utc))
        self.assertLess(run.finished_at, now.astimezone(tz=datetime.timezone.utc))
        self.assertIsNone(run.requested_at)
        self.assertIsNotNone(run.links)

        self.assertEqual(run.links.retry, "/api/v2/tasks/" + task.id + "/runs/" + run.id + "/retry")
        self.assertEqual(run.links._self, "/api/v2/tasks/" + task.id + "/runs/" + run.id)
        self.assertEqual(run.links.task, "/api/v2/tasks/" + task.id)

    def test_runs_not_exist(self):
        with pytest.raises(ApiException) as e:
            assert self.tasks_api.get_runs("020f755c3c082000")
        assert "task not found" in e.value.body

    def test_run_task_manually(self):
        task = self.tasks_api.create_task_every(self.generate_name("it task"), TASK_FLUX, "1s", self.organization)

        run = self.tasks_api.run_manually(task_id=task.id)
        print(run)

        self.assertIsNotNone(run)
        self.assertTrue(run.status, "scheduled")

    def test_run_task_manually_not_exist(self):
        with pytest.raises(ApiException) as e:
            assert self.tasks_api.run_manually(task_id="020f755c3c082000")
        assert "failed to force run" in e.value.body

    def test_retry_run(self):
        task = self.tasks_api.create_task_every(self.generate_name("it task"), TASK_FLUX, "1s", self.organization)

        time.sleep(5)

        runs = self.tasks_api.get_runs(task.id)
        self.assertGreater(len(runs), 1)

        run = self.tasks_api.retry_run(task_id=runs[0].task_id, run_id=runs[0].id)
        self.assertIsNotNone(run)
        self.assertEqual(run.task_id, runs[0].task_id)

        self.assertEqual(run.status, "scheduled")
        self.assertEqual(run.task_id, task.id)

    def test_retry_run_not_exists(self):
        task = self.tasks_api.create_task_every(self.generate_name("it task"), TASK_FLUX, "5s", self.organization)
        with pytest.raises(ApiException) as e:
            assert self.tasks_api.retry_run(task_id=task.id, run_id="020f755c3c082000")
        assert "failed to retry run" in e.value.body

    def test_logs(self):
        task = self.tasks_api.create_task_every(self.generate_name("it task"), TASK_FLUX, "3s", self.organization)
        time.sleep(6)

        logs = self.tasks_api.get_logs(task_id=task.id)

        for log in logs:
            self.assertIsNotNone(log.time)
            self.assertIsNotNone(log.message)
            print(log)

        self.tasks_api.delete_task(task_id=task.id)

    def test_logs_not_exist(self):
        with pytest.raises(ApiException) as e:
            assert self.tasks_api.get_logs(task_id="020f755c3c082000")
        assert "failed to find task logs" in e.value.body

    def test_run_logs(self):
        task = self.tasks_api.create_task_every(self.generate_name("it task"), TASK_FLUX, "1s", self.organization)
        time.sleep(5)
        runs = self.tasks_api.get_runs(task_id=task.id)
        self.assertGreater(len(runs), 0)

        logs = self.tasks_api.get_run_logs(run_id=runs[-1].id, task_id=task.id)
        self.assertGreater(len(logs), 0)

        successes = list(filter(lambda log: log.message.endswith("Completed(success)"), logs))
        self.assertGreaterEqual(len(successes), 1)

    def test_runs_not_exists(self):
        task = self.tasks_api.create_task_every(self.generate_name("it task"), TASK_FLUX, "1s", self.organization)

        with pytest.raises(ApiException) as e:
            assert self.tasks_api.get_run_logs(task_id=task.id, run_id="020f755c3c082000")
        assert "failed to find task logs" in e.value.body

    def test_cancel_run_not_exist(self):
        task = self.tasks_api.create_task_every(self.generate_name("it task"), TASK_FLUX, "1s", self.organization)
        time.sleep(5)
        runs = self.tasks_api.get_runs(task.id)

        with pytest.raises(ApiException) as e:
            assert self.tasks_api.cancel_run(task_id=task.id, run_id=runs[-1].id)
        assert "failed to cancel run" in e.value.body
        assert "run not found" in e.value.body

    def test_cancel_task_not_exist(self):
        with pytest.raises(ApiException) as e:
            assert self.tasks_api.cancel_run("020f755c3c082000", "020f755c3c082000")
        assert "failed to cancel run" in e.value.body
        assert "task not found" in e.value.body

    def test_get_run(self):
        task = self.tasks_api.create_task_every(self.generate_name("it task"), TASK_FLUX, "1s", self.organization)
        time.sleep(5)
        run = self.tasks_api.get_runs(task_id=task.id)[0]
        self.assertIsNotNone(run)
        run_by_id = self.tasks_api.get_run(task.id, run.id)
        self.assertIsNotNone(run_by_id)
        self.assertEqual(run.id, run_by_id.id)

    def test_clone(self):
        task = self.tasks_api.create_task_every(self.generate_name("it_task"), TASK_FLUX, "1h", self.organization)
        label = self.labels_api.create_label(self.generate_name("it_task"), self.organization.id, {
            "color": "green",
            "location": "west"
        })
        self.tasks_api.add_label(label.id, task.id)
        cloned = self.tasks_api.clone_task(task)
        self.assertNotEqual(task.id, cloned.id)
        self.assertEqual(task.flux, cloned.flux)
        labels = self.tasks_api.get_labels(cloned.id).labels
        self.assertEqual(1, len(labels))
        self.assertEqual(label.id, labels[0].id)

    def test_clone_new(self):
        task = self.tasks_api._create_task(self.generate_name("it_task"), TASK_FLUX, "1h", None, self.organization.id)
        cloned = self.tasks_api.clone_task(task)
        self.assertNotEqual(task.id, cloned.id)
        self.assertEqual(task.flux, cloned.flux)
