import requests
import validators


def describe(host, workflow, inputs, language, language_version, version):
    """Describe a workflow (Cromwell)"""
    url = host + '/api/womtool/' + version + '/describe'
    data = {}

    if validators.url(workflow):
        data.workflowUrl = workflow
    else:
        data.workflowSource = open(workflow, 'rb')

    if inputs is not None:
        data.workflowInputs = open(inputs, 'rb')

    if language is not None:
        data.workflowType = language

    if language_version is not None:
        if language == 'CWL':
            language_version = "v" + language_version
        data.workflowTypeVersion = language_version

    return requests.post(url, files=data)


def query(host, version='v1'):
    """Get workflows matching some criteria (Cromwell)"""
    url = '{}/api/workflows/{}/query'.format(host, version)
    return requests.get(url)


def logs(host, job_id, version='v1'):
    """Get the logs for a workflow"""
    url = '{}/api/workflows/{}/{}/logs'.format(host, version, job_id)
    return requests.get(url)


def status(host, job_id, version='v1'):
    """Retrieves the current state for a workflow (Cromwell)"""
    url = '{}/api/workflows/{}/{}/status'.format(host, version, job_id)
    return requests.get(url)


def submit(host, workflow, inputs, dependencies, options, labels, language, language_version, version='v1'):
    """Submit a workflow for execution (Cromwell)"""
    data = {}

    if validators.url(workflow):
        data['workflowUrl'] = workflow
    else:
        data['workflowSource'] = open(workflow, 'rb')

    data['workflowInputs'] = open(inputs, 'rb')

    if dependencies is not None:
        data['workflowDependencies'] = open(dependencies, 'rb')

    if options is not None:
        data['workflowOptions'] = open(options, 'rb')

    if labels is not None:
        data['labels'] = open(labels, 'rb')

    if language is not None:
        data['workflowType'] = language

    if language_version is not None:
        if language == 'CWL':
            language_version = "v" + language_version
        data['workflowTypeVersion'] = language_version

    url = host + '/api/workflows/' + version
    return requests.post(url, files=data)


def outputs(host, job_id, version):
    """Get the outputs for a workflow"""
    url = host + '/api/workflows/' + version + '/' + job_id + '/outputs'
    return requests.get(url)
