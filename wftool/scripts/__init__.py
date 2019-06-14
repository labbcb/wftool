from csv import DictWriter
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
