from wftool.client import Client
from enum import Enum


class FileType(Enum):
    FILE = 0
    DIRECTORY = 1


class State(Enum):
    UNKNOWN = 1
    QUEUED = 2
    INITIALIZING = 3
    RUNNING = 4
    PAUSED = 5
    COMPLETE = 6
    EXECUTOR_ERROR = 7
    SYSTEM_ERROR = 8
    CANCELED = 9


class View(Enum):
    MINIMAL = 0
    BASIC = 1
    FULL = 2


class Task:
    def __init__(self, id, executors, state=State.UNKNOWN, name=None, description=None, inputs=None, outputs=None,
                 resources=None, volumes=None, tags=None, logs=None, creation_time=None):
        self.id = id
        self.executors = executors
        self.state = state
        self.name = name
        self.description = description
        self.inputs = inputs
        self.resources = resources
        self.volumes = volumes
        self.tags = tags
        self.logs = logs
        self.creation_time = creation_time


class ServiceInfo:
    def __init__(self, name, doc, storage):
        self.name = name
        self.doc = doc
        self.storage = storage


class Resources:
    def __init__(self, cpu_cores, preemptible, ram_gb, disk_gb, zones):
        self.cpu_cores = cpu_cores
        self.preemptible = preemptible
        self.ram_gb = ram_gb
        self.disk_gb = disk_gb
        self.zones = zones


class TaskLog:
    def __init__(self, logs, outputs, metadata=None, start_time=None, end_time=None, system_logs=None):
        self.logs = logs
        self.outputs = outputs
        self.metadata = metadata
        self.start_time = start_time
        self.end_time = end_time
        self.system_logs = system_logs


class OutputFileLog:
    def __init__(self, url, path, size_bites):
        self.url = url
        self.path = path
        self.size_bites = size_bites


class Output:
    def __init__(self, url, path, type, name=None, description=None):
        self.url = url
        self.path = path
        self.type = type
        self.name = name
        self.description = description


class ListTasksResponse:
    def __init__(self, tasks, next_page_token=None):
        self.tasks = tasks
        self.next_page_token = next_page_token


class Input:
    def __init__(self, url, path, type, name=None, description=None):
        self.url = url
        self.path = path
        self.type = type
        self.name = name
        self.description = description


class ExecutorLog:
    def __init__(self, exit_code, start_time=None, end_time=None, stdout=None, stderr=None):
        self.exit_code = exit_code
        self.start_time = start_time
        self.end_time = end_time
        self.stdout = stdout
        self.stderr = stderr


class Executor:
    def __init__(self, image, command, workdir=None, stdin=None, stdout=None, stderr=None, env=None):
        self.image = image
        self.command = command
        self.workdir = workdir
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.env = env


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
