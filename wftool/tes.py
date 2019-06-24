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


class Executor:
    def __init__(self, image, command, workdir=None, stdin=None, stdout=None, stderr=None, env=None):
        self.image = image
        self.command = command
        self.workdir = workdir
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.env = env


class ExecutorLog:
    def __init__(self, exit_code, start_time=None, end_time=None, stdout=None, stderr=None):
        self.exit_code = exit_code
        self.start_time = start_time
        self.end_time = end_time
        self.stdout = stdout
        self.stderr = stderr


class Input:
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


class Resources:
    def __init__(self, cpu_cores, preemptible, ram_gb, disk_gb, zones):
        self.cpu_cores = cpu_cores
        self.preemptible = preemptible
        self.ram_gb = ram_gb
        self.disk_gb = disk_gb
        self.zones = zones


class ServiceInfo:
    def __init__(self, name, doc, storage):
        self.name = name
        self.doc = doc
        self.storage = storage


class Task:
    def __init__(self, id, executors, state=State.UNKNOWN, name=None, description=None, inputs=None, outputs=None,
                 resources=None, volumes=None, tags=None, logs=None, creation_time=None):
        self.id = id
        self.executors = executors
        self.state = state
        self.name = name
        self.description = description
        self.inputs = inputs
        self.outputs = outputs
        self.resources = resources
        self.volumes = volumes
        self.tags = tags
        self.logs = logs
        self.creation_time = creation_time


class TaskLog:
    def __init__(self, logs, outputs, metadata=None, start_time=None, end_time=None, system_logs=None):
        self.logs = logs
        self.outputs = outputs
        self.metadata = metadata
        self.start_time = start_time
        self.end_time = end_time
        self.system_logs = system_logs


class Output:
    def __init__(self, url, path, type, name=None, description=None):
        self.url = url
        self.path = path
        self.type = type
        self.name = name
        self.description = description


class OutputFileLog:
    def __init__(self, url, path, size_bites):
        self.url = url
        self.path = path
        self.size_bites = size_bites
