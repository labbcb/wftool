from .client import Client


class WesClient(Client):
    """
    WES API client
    """

    def __init__(self, host, api_version='v1'):
        self.base_path = '/ga4gh/wes/' + api_version
        super().__init__(host)

    def cancel_run(self, run_id):
        """
        Cancel a running workflow
        :param run_id:
        :return:
        """
        path = self._get_path('{run_id}/cancel'.format(run_id=run_id))
        return super().post(path)

    def get_service_info(self):
        """
        Get information about Workflow Execution Service
        :return:
        """
        path = self._get_path('service-info')
        return super().get(path)

    def list_runs(self, page_size=None, page_token=None):
        """
        List the workflow runs
        :param page_size:
        :param page_token:
        :return:
        """
        data = dict(page_size=page_size, page_token=page_token)
        path = self._get_path('runs')
        return super().get(path, data)

    def run_workflow(self, workflow_url, workflow_params, workflow_type, workflow_type_version, workflow_attachment,
                     workflow_engine_parameters=None, tags=None):
        """
        Run a workflow
        :param workflow_url: URL or relative path (attachment) to primary workflow
        :param workflow_params: path to workflow params JSON file
        :param workflow_type: workflow language (CWL, WDL)
        :param workflow_type_version: version of the workflow language
        :param tags: list of tags to label workflow submission
        :param workflow_engine_parameters: path to engine-specific params JSON fie
        :param workflow_attachment: dict of {filename: file_path} of workflow files
        :return:
        """
        data = dict(workflow_url=workflow_url, workflow_type=workflow_type, workflow_type_version=workflow_type_version,
                    tags=tags)

        data['workflow_attachment'] = dict()
        for filename, file_path in workflow_attachment.items():
            data['workflow_attachment'][filename] = open(file_path)

        data['workflow_params'] = open(workflow_params)

        if workflow_engine_parameters:
            data['workflow_engine_parameters'] = open(workflow_engine_parameters)

        path = self._get_path('runs')
        return super().post(path, data)

    def get_run_log(self, run_id):
        """
        Get detailed info about a workflow run
        :param run_id: Workflow run ID
        :return:
        """
        path = self._get_path('run/{id}'.format(id=run_id))
        return super().get(path)

    def get_run_status(self, run_id):
        """
        Get quick status info about a workflow run
        :param run_id:
        :return:
        """
        path = self._get_path('run/{id}/status'.format(id=run_id))
        return super().get(path)

    def _get_path(self, part):
        return '{base_path}/{part}'.format(base_path=self.base_path, part=part)
