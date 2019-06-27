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
