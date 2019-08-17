from unittest import TestCase

from wftools.cromwell import CromwellClient
from time import sleep

client = CromwellClient('http://localhost:8000')
workflow = 'hello.wdl'
inputs = 'hello.inputs.json'
sleep_time = 10


class TestCromwellClient(TestCase):

    def test_abort(self):
        workflow_id = client.submit(workflow)
        sleep(sleep_time)
        status = client.abort(workflow_id)
        self.assertEqual(status, "Aborting")

    def test_describe(self):
        response = client.describe(workflow)
        self.assertIs(type(response), dict)

    def test_status(self):
        workflow_id = client.submit(workflow)
        sleep(sleep_time)
        status = client.status(workflow_id)
        self.assertEqual(status, "Submitted")

    def test_info(self):
        response = client.info()
        self.assertIs(type(response), dict)

    def test_list(self):
        response = client.list()
        self.assertIs(type(response), list)

    def test_logs(self):
        workflow_id = client.submit(workflow)
        sleep(sleep_time)

        status = client.status(workflow_id)
        while status in ('Submitted', 'Running'):
            status = client.status(workflow_id)

        response = client.logs(workflow_id)
        self.assertIs(type(response), dict)

    def test_release(self):
        workflow_id = client.submit(workflow, hold=True)
        sleep(sleep_time)

        status = client.status(workflow_id)
        self.assertEqual(status, 'On Hold')

        status = client.release(workflow_id)
        self.assertEqual(status, 'Submitted')

    def test_submit(self):
        workflow_id = client.submit(workflow, inputs)
        sleep(sleep_time)

        status = client.status(workflow_id)
        while status in ('Submitted', 'Running'):
            status = client.status(workflow_id)

        response = client.outputs(workflow_id)
        self.assertEqual(response.get('SayHello.msg'), 'Hello wftools!')
