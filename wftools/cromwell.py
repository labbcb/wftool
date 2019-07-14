from . import is_url
from .client import Client


class CromwellClient(Client):
    """
    Cromwell API client.
    Provides all methods available of this API
    """

    def __init__(self, host, api_version='v1'):
        """
        Initializes CromwellClient
        :param host: Cromwell server URL
        :param api_version: Cromwell API version
        """
        super().__init__(host)
        self.api_version = api_version

    def abort(self, workflow_id):
        """
        Abort a running workflow
        :param workflow_id: Workflow ID
        :return: dict containing workflow ID and updated status
        """
        path = '/api/workflows/{version}/{id}/abort'.format(id=workflow_id, version=self.api_version)
        response = super().post(path)
        if response.get('status') in ('fail', 'error'):
            raise Exception(response.get('message'))
        return response.get('status')

    def describe(self, workflow, inputs=None, language=None, language_version=None):
        """
        Machine-readable description of a workflow, including inputs and outputs
        :param workflow: Workflow source file path (or URL)
        :param inputs: JSON or YAML file path containing the inputs
        :param language: Workflow language (WDL or CWL)
        :param language_version: Workflow language version (draft-2, 1.0 for WDL or v1.0 for CWL)
        :return:
        """
        data = dict(workflowType=language, workflowTypeVersion=language_version)
        if is_url(workflow):
            data['workflowUrl'] = workflow
        else:
            data['workflowSource'] = open(workflow, 'rb')

        if inputs is not None:
            data['workflowInputs'] = open(inputs, 'rb')

        path = '/api/womtool/{version}/describe'.format(version=self.api_version)
        response = super().post(path, data)
        if response.get('status') in ('fail', 'error'):
            raise Exception(response.get('message'))
        return response

    def diff(self, workflow_id_a, workflow_id_b, call_a, call_b, index_a, index_b):
        """
        Explain hashing differences for 2 calls
        :param workflow_id_a: Workflow ID of the first workflow
        :param workflow_id_b: Workflow ID of the second workflow
        :param call_a: Fully qualified name, including workflow name, of the first call
        :param call_b: Fully qualified name, including workflow name, of the second call
        :param index_a: Shard index for the first call for cases where the requested call was part of a scatter
        :param index_b: Shard index for the second call for cases where the requested call was part of a scatter
        :return:
        """
        data = dict(callA=call_a, callB=call_b, indexA=index_a, indexB=index_b, workflowA=workflow_id_a,
                    workflowB=workflow_id_b)

        path = '/api/workflows/{version}/callcaching/diff'.format(version=self.api_version)
        response = super().get(path, data)
        if response.get('status') in ('fail', 'error'):
            raise Exception(response.get('message'))
        return response

    def health_status(self):
        """
        Return the current health status of any monitored subsystems
        :return:
        """
        path = '/engine/{version}/status'.format(version=self.api_version)
        response = super().get(path)
        if response.get('status') in ('fail', 'error'):
            raise Exception(response.get('message'))
        return response

    def info(self):
        """
        List the supported backends by Cromwell Server
        :return: dic containing default and supported backends
        """
        path = '/api/workflows/{version}/backends'.format(version=self.api_version)
        response = super().get(path)
        if response.get('status') in ('fail', 'error'):
            raise Exception(response.get('message'))
        return response

    def labels(self, workflow_id):
        """
        Retrieves the current labels for a workflow
        :return:
        """
        path = '/api/workflows/{version}/{id}/labels'.format(id=workflow_id, version=self.api_version)
        response = super().get(path)
        if response.get('status') in ('fail', 'error'):
            raise Exception(response.get('message'))
        return response

    def list(self, workflow_ids=None, names=None, status=None):
        """
        Get workflows matching some criteria
        :param workflow_ids: Returns only workflows with the specified workflow IDs
        :param names: Returns only workflows with the specified name
        :param status: Returns only workflows with the specified status
        :return:
        """
        path = '/api/workflows/{version}/query'.format(version=self.api_version)
        data = dict(id=workflow_ids, name=names, status=status)
        response = super().get(path, data)
        if response.get('status') in ('fail', 'error'):
            raise Exception(response.get('message'))
        return response.get('results')

    def logs(self, workflow_id):
        """
        Get the logs for a workflow
        :return:
        """
        path = '/api/workflows/{version}/{id}/logs'.format(id=workflow_id, version=self.api_version)
        response = super().get(path)
        if response.get('status') in ('fail', 'error'):
            raise Exception(response.get('message'))
        return response.get('calls')

    def metadata(self, workflow_id, exclude_key, expand_sub_workflows, include_key):
        """
        Get workflow and call-level metadata for a specified workflow
        :return:
        """
        path = '/api/workflows/{version}/{id}/metadata'.format(id=workflow_id, version=self.api_version)
        data = dict(excludeKey=exclude_key, expandSubWorkflows=expand_sub_workflows, includeKey=include_key)
        response = super().get(path, data)
        if response.get('status') in ('fail', 'error'):
            raise Exception(response.get('message'))
        return response

    def release(self, workflow_id):
        """
        Switch a workflow from 'On Hold' to 'Submitted' status
        :return:
        """
        path = '/api/workflows/{version}/{id}/releaseHold'.format(id=workflow_id, version=self.api_version)
        response = super().post(path)
        if response.get('status') in ('fail', 'error'):
            raise Exception(response.get('message'))
        return response.get('status')

    def status(self, workflow_id):
        """
        Retrieves the current state for a workflow
        :param workflow_id:
        :return:
        """
        path = '/api/workflows/{version}/{id}/status'.format(id=workflow_id, version=self.api_version)
        response = super().get(path)
        if response.get('status') in ('fail', 'error'):
            raise Exception(response.get('message'))
        return response.get('status')

    def submit(self, workflow, inputs=None, options=None, dependencies=None, labels=None, language=None,
               language_version=None, root=None, hold=None):
        """
        Submit a workflow for execution
        :param workflow: Workflow source file path (or URL)
        :param inputs: JSON or YAML file path containing the inputs
        :param options: JSON file path containing configuration options for the execution of this workflow
        :param dependencies: ZIP file containing workflow source files that are used to resolve local imports
        :param labels: JSON file containing labels to apply to this workflow
        :param language: Workflow language (WDL or CWL)
        :param language_version: Workflow language version (draft-2, 1.0 for WDL or v1.0 for CWL)
        :param root: The root object to be run. Only necessary for CWL submissions containing multiple objects
        :param hold: Put workflow on hold upon submission. By default, it is taken as false
        :return:
        """
        data = dict(workflowRoot=root, workflowOnHold=hold, workflowType=language, workflowTypeVersion=language_version)
        if is_url(workflow):
            data['workflowUrl'] = workflow
        else:
            data['workflowSource'] = open(workflow, 'rb')

        if inputs is not None:
            data['workflowInputs'] = open(inputs, 'rb')

        if dependencies is not None:
            data['workflowDependencies'] = open(dependencies, 'rb')
        if options is not None:
            data['workflowOptions'] = open(options, 'rb')
        if labels is not None:
            data['labels'] = open(labels, 'rb')

        path = '/api/workflows/{version}'.format(version=self.api_version)
        response = super().post(path, data)
        if response.get('status') in ('fail', 'error'):
            raise Exception(response.get('message'))
        return response.get('id')

    def submit_batch(self, workflow, inputs, options=None, dependencies=None, labels=None, language=None,
                     language_version=None, hold=None):
        """
        Submit a batch of workflows for execution
        :param workflow: Workflow source file path (or URL)
        :param inputs: List of JSON or YAML file paths containing the inputs
        :param options: JSON file path containing configuration options for the execution of this workflow
        :param dependencies: ZIP file containing workflow source files that are used to resolve local imports
        :param labels: JSON file containing labels to apply to this workflow
        :param language: Workflow language (WDL or CWL)
        :param language_version: Workflow language version (draft-2, 1.0 for WDL or v1.0 for CWL)
        :param hold: Put workflow on hold upon submission. By default, it is taken as false
        :return:
        """
        data = dict(workflowOnHold=hold, workflowType=language, workflowTypeVersion=language_version)
        if is_url(workflow):
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

        path = '/api/workflows/{version}/batch'.format(version=self.api_version)
        response = super().post(path, data)
        if isinstance(response, dict) and response.get('status') == 'fail':
            raise Exception(response.get('message'))
        return [workflow.get('id') for workflow in response]

    def timing(self, workflow_id, html=False):
        """
        Get a visual diagram of a running workflow
        :param workflow_id: Workflow ID
        :param html: return fetched HTML data instead of URL (default is False)
        :return: URL to web page or HTML data
        """
        path = '/api/workflows/{version}/{id}/timing'.format(id=workflow_id, version=self.api_version)
        return super().get(path, raw_response_content=True).decode() if html else super().url(path)

    def update_labels(self, workflow_id, labels):
        """
        Update labels for a workflow
        :return:
        """
        path = '/api/workflows/{version}/{id}/labels'.format(id=id, version=self.api_version)
        data = dict(id=workflow_id, labels=labels)
        response = super().patch(path, data)
        if response.get('fail', None) == 'fail':
            raise Exception(response['message'])
        return response

    def outputs(self, workflow_id):
        """
        Get the outputs for a workflow
        :return:
        """
        path = '/api/workflows/{version}/{id}/outputs'.format(id=workflow_id, version=self.api_version)
        response = super().get(path)
        if response.get('status') in ('fail', 'error'):
            raise Exception(response.get('message'))
        return response.get('outputs')

    def version(self):
        """
        Return the version of this Cromwell server
        :return: version of the Cromwell in str
        """
        path = '/engine/{version}/version'.format(version=self.api_version)
        response = super().get(path)
        if response.get('status') in ('fail', 'error'):
            raise Exception(response.get('message'))
        return response.get('cromwell')
