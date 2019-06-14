from wftool.cromwell import CromwellClient
from wftool.tes import TesClient
from wftool.wes import WesClient


def cromwell_client(host='http://localhost:8000', api_version='v1'):
    """
    Checks Cromwell server and creates CromwellClient object.
    Use this function instead of instantiating the class.
    :param host: Cromwell Server location
    :param api_version: Cromwell API version
    :return: CromwellClient object
    """
    return CromwellClient(host, api_version)


def tes_client(host):
    return TesClient(host)


def wes_client(host):
    return WesClient(host)
