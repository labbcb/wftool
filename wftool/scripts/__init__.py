from csv import DictWriter
from json import dump
import sys


def write_as_csv(data, file=sys.stdout):
    """Write a dict or list of dictionaries as CSV to stdout (default)"""
    is_list = isinstance(data, list)
    writer = DictWriter(file, frozenset().union(*data) if is_list else data.keys())
    writer.writeheader()
    if is_list:
        writer.writerows(data)
    else:
        writer.writerow(data)


def write_as_json(data, destination=sys.stdout):
    """
    Writes data as JSON format to file
    :param data: object to be serialized as JSON
    :param destination: destination file (stdout by default)
    """
    dump(data, destination, default=lambda o: o.__dict__)
