from wftool.client import Client
from wftool.tes import View, ListTasksResponse, Task


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

    def list_tasks(self, view=View.MINIMAL, name_prefix=None, page_size=None, page_token=False):
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

        data = dict(view=view, name_prefix=name_prefix, page_size=page_size, page_token=page_token)
        path = '/{version}/tasks'.format(version=self.api_version)
        response = super().get(path, data)

        return ListTasksResponse(tasks=[Task(**t) for t in response.get('tasks')],
                                 next_page_token=response.get('next_page_token'))

    def create_task(self, task):
        """
        Create a new task
        :param task: Task object to be submitted
        :return:
        """

        data = dict(task=task)
        path = '/{version}/tasks'.format(version=self.api_version)
        return super().post(path, data)

    def service_info(self):
        """
        Information about the service such as storage details, resource availability
        :return:
        """

        path = '/{version}/tasks/service-info'.format(version=self.api_version)
        return super().get(path)
