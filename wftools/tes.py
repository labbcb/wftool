from .client import Client


class TesClient(Client):
    """
    TES API client
    Provides all methods available in the API
    """

    def __init__(self, host, api_version='v1'):
        """
        Initializes TesClient
        :param host: TES implementation server URL
        :param api_version: TES API version
        """
        super().__init__(host)
        self.api_version = api_version

    def create_task(self, task):
        """
        Create a new task
        :param task: Task object to be submitted
        :return:
        """

        data = dict(task=task)
        path = '/{version}/tasks'.format(version=self.api_version)
        return super().post(path, data)

    def get_task(self, task_id, view='MINIMAL'):
        """
        Get a task
        :param task_id: Task id
        :param view: Affects the fields included in the returned Task messages.
            MINIMAL: Task message will include ONLY the fields: ID, State
            BASIC: Task message will include all fields except: ExecutorLog.stdout, ExecutorLog.stderr, Input.content,
                system_logs
            FULL: Task message includes all fields
        :return: a Task object
        """
        data = dict(id=task_id, view=view)
        path = '/{version}/tasks/{id}'.format(version=self.api_version, id=task_id)
        return super().get(path, data)

    def list_tasks(self, view='MINIMAL', name_prefix=None, page_size=None, page_token=False):
        """
        List tasks
        :param view: Affects the fields included in the returned Task messages.
            MINIMAL: Task message will include ONLY the fields: ID, State
            BASIC: Task message will include all fields except: ExecutorLog.stdout, ExecutorLog.stderr, Input.content,
                system_logs
            FULL: Task message includes all fields
        :param name_prefix: Filter the list to include tasks where the name matches this prefix
        :param page_size: Number of tasks to return in one page
        :param page_token: Page token is used to retrieve the next page of results
        :return:
        """

        data = dict(view=view.upper(), name_prefix=name_prefix, page_size=page_size, page_token=page_token)
        path = '/{version}/tasks'.format(version=self.api_version)
        return super().get(path, data)

    def service_info(self):
        """
        Information about the service such as storage details, resource availability
        :return:
        """

        path = '/{version}/tasks/service-info'.format(version=self.api_version)
        return super().get(path)
