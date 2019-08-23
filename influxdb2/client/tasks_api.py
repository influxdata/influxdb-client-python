from influxdb2 import TasksService, Task, TaskCreateRequest


class TasksApi(object):

    def __init__(self, influxdb_client):
        self._influxdb_client = influxdb_client
        self._service = TasksService(influxdb_client.api_client)

    def find_task_by_id(self, task_id):

        task = self._service.get_tasks_id(task_id)
        return task

    def find_tasks(self, **kwargs):
        return self._service.get_tasks(**kwargs)

    def create_task(self, task: Task = None, task_create_request: TaskCreateRequest = None) -> Task:

        if task_create_request is not None:
            return self._service.post_tasks(task_create_request)

        if task is not None:
            request = TaskCreateRequest(flux=task.flux, org_id=task.org_id, org=task.org, description=task.description,
                                        status=task.status)

            return self.create_task(task_create_request=request)

        raise ValueError("task or task_create_request must be not None")

    @staticmethod
    def _create_task(name: str, flux: str, every: str, cron: str, org_id: str) -> Task:

        task = Task(id=0, name=name, org_id=org_id, status="active", flux=flux)

        repetition = ""
        if every is not None:
            repetition += "every: "
            repetition += every

        if cron is not None:
            repetition += "cron"
            repetition += "\"" + cron + "\""

        flux_with_options = 'option task = {{name: "{}", {}}} \n {}'.format(name, repetition, flux)
        task.flux = flux_with_options

        return task

    def create_task_every(self, name, flux, every, organization):
        task = self._create_task(name, flux, every, None, organization.id)
        return self.create_task(task)
