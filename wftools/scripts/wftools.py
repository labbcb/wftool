import itertools
import os
import shutil
from json import dumps

import click

from wftools.scripts import write_as_csv, write_as_json
from ..tes import TesClient
from ..cromwell import CromwellClient


def call_client_method(method, *args):
    """
    Given a API client method and its arguments try to call or exit program
    :param method: API client method
    :param args: Arguments for method
    :return: object from method call
    """
    try:
        return method(*args)
    except Exception as e:
        click.echo(str(e), err=True)
        exit(1)


@click.group()
def cli():
    """Workflow and task management for genomics research"""
    pass


@cli.command()
@click.option('--host', help='Server address', default='http://localhost:8000', show_default=True)
@click.option('--api', default='cromwell', type=click.Choice(['cromwell']), help='API', show_default=True)
@click.option('-f', '--format', 'output_format', default='console', type=click.Choice(['console', 'csv', 'json']),
              help='Format of output', show_default=True)
@click.argument('id')
def abort(host, api, id, output_format):
    """Abort a running workflow or task"""
    client = CromwellClient(host)
    data = call_client_method(client.abort, id)

    if output_format != 'console':
        click.echo('This method only supports console format as output.', err=True)

    click.echo(data)


@cli.command()
@click.option('--host', help='Server address', default='http://localhost:8000', show_default=True)
@click.option('--api', default='cromwell', type=click.Choice(['cromwell']), help='API', show_default=True)
@click.option('--no-task-dir', is_flag=True, default=False, help='Do not create subdirectories for tasks')
@click.option('--copy/--move', 'copy', default=True, help='Copy or move output files? Copy by default.')
@click.option('--overwrite', is_flag=True, default=False, help='Overwrite existing files.')
@click.argument('id')
@click.argument('destination', type=click.Path())
def collect(host, api, id, no_task_dir, copy, overwrite, destination):
    """Copy or move output files of workflow to directory (Cromwell)"""
    client = CromwellClient(host)
    data = call_client_method(client.outputs, id)

    if not os.path.exists(destination):
        os.mkdir(destination)

    for task in data:

        if no_task_dir:
            task_dir = destination
        else:
            task_dir = os.path.join(destination, task)
            if not os.path.exists(task_dir):
                os.mkdir(task_dir)

        if isinstance(data[task], str):
            files = [data[task]]
        elif any(isinstance(i, list) for i in data[task]):
            files = itertools.chain.from_iterable(data[task])
        else:
            files = data[task]

        for file in files:
            if os.path.exists(file):
                dest_file = os.path.join(task_dir, os.path.basename(file))
                if os.path.exists(dest_file) and not overwrite:
                    click.echo('File already exists: ' + dest_file, err=True)
                    exit(1)
                if copy:
                    shutil.copyfile(file, dest_file)
                else:
                    shutil.move(file, dest_file)
            else:
                click.echo('File not found: ' + file, err=True)


@cli.command()
@click.option('--host', help='Server address', default='http://localhost:8000', show_default=True)
@click.option('--api', default='cromwell', type=click.Choice(['cromwell']), help='API', show_default=True)
@click.option('--inputs', help='Path to inputs file')
@click.option('--language', type=click.Choice(['WDL', 'CWL']), help='Workflow file format')
@click.option('--language-version', type=click.Choice(['draft-2', '1.0']), help='Language version')
@click.option('-f', '--format', 'output_format', default='json', type=click.Choice(['console', 'csv', 'json']),
              help='Format of output', show_default=True)
@click.argument('workflow')
def describe(host, api, workflow, inputs, language, language_version, output_format):
    """Describe a workflow (Cromwell)"""
    client = CromwellClient(host)
    data = call_client_method(client.describe, workflow, inputs, language, language_version)

    if output_format != 'json':
        click.echo('This method only supports JSON format as output.', err=True)

    click.echo(dumps(data))


@cli.command()
@click.option('--host', help='Server address', default='http://localhost:8000', show_default=True)
@click.option('--api', default='cromwell', type=click.Choice(['cromwell']), help='API', show_default=True)
@click.option('-f', '--format', 'output_format', default='console', type=click.Choice(['console', 'csv', 'json']),
              help='Format of output', show_default=True)
def info(host, api, output_format):
    """Ger server info"""
    client = CromwellClient(host)
    data = call_client_method(client.backends)

    if output_format == 'csv':
        click.echo("This method doesn't support CSV format as output.", err=True)

    if output_format == 'json':
        click.echo(dumps(data))
    else:
        click.echo('Default backend: {}'.format(data.get('defaultBackend')))
        click.echo('Supported backends: {}'.format(','.join(data.get('supportedBackends'))))


@cli.command()
@click.option('--host', help='Server address', default='http://localhost:8000', show_default=True)
@click.option('--api', default='cromwell', type=click.Choice(['cromwell']), help='API', show_default=True)
@click.option('-f', '--format', 'output_format', default='console', type=click.Choice(['console', 'csv', 'json']),
              help='Format of output', show_default=True)
@click.argument('id')
def logs(host, api, id, output_format):
    """Get the logs for a workflow (Cromwell)"""
    client = CromwellClient(host)
    data = call_client_method(client.logs, id)

    if output_format == 'json':
        click.echo(dumps(data))
    elif output_format == 'csv':
        fixed_data = []
        for task_name, logs in data.items():
            for log in logs:
                log['task'] = task_name
                fixed_data.append(log)
        write_as_csv(fixed_data)
    else:
        for task in data:
            click.echo('Task {}'.format(task))
            for idx in range(len(data[task])):
                click.echo('Shard {} stdout: {}'.format(idx, data[task][idx]['stdout']))
                click.echo('Shard {} stderr: {}'.format(idx, data[task][idx]['stderr']))


def cromwell_query(host, id, name, workflow_status, output_format):
    if host is None:
        host = 'http://localhost:8000'
    client = CromwellClient(host)
    data = call_client_method(client.query, id, name, workflow_status)

    if output_format == 'json':
        click.echo(dumps(data))
    elif output_format == 'csv':
        write_as_csv(data)
    else:
        click.echo('{:36}  {:9}  {:24}  {:24}  {:24}  {}'.format('ID', 'Status', 'Start', 'End', 'Submitted', 'Name'))
        for workflow in data:
            click.echo('{:36}  {:9}  {:24}  {:24}  {:24}  {}'.format(workflow.get('id', '-'),
                                                                     workflow.get('status', '-'),
                                                                     workflow.get('start', '-'),
                                                                     workflow.get('end', '-'),
                                                                     workflow.get('submission', '-'),
                                                                     workflow.get('name', '-')))


def tes_query(host, ids, names, task_states, output_format):
    if host is None:
        host = 'http://localhost:8080'
    client = TesClient(host)
    data = client.list_tasks('FULL')

    if ids:
        data['tasks'] = [t for t in data.get('tasks') if t.get('id') in ids]
    if names:
        data['tasks'] = [t for t in data.get('tasks') if t.get('name') in names]
    if task_states:
        task_states = [t.upper() for t in task_states]
        data['tasks'] = [t for t in data.get('tasks') if t.get('state') in task_states]

    if output_format == 'json':
        write_as_json(data)
    elif output_format == 'csv':
        write_as_csv(data)
    else:
        click.echo('{:24}  {:8}  {:28}  {:3}  {:6}  {:6}'.format('ID', 'State', 'Created', 'CPU', 'RAM', 'DISK'))
        for task in data.get('tasks'):
            resources = task.get('resources')
            click.echo('{:24}  {:8}  {:28}  {:3}  {:6.2f}  {:6.2f}'.format(task.get('id'),
                                                                           task.get('state'),
                                                                           task.get('creation_time'),
                                                                           resources.get('cpu_cores', 0),
                                                                           resources.get('ram_gb', 0),
                                                                           resources.get('disk_gb', 0)))


@cli.command('list')
@click.option('--host', help='Server address', show_default=True)
@click.option('--api', default='cromwell', type=click.Choice(['cromwell', 'tes']), help='API', show_default=True)
@click.option('--id', multiple=True, help='Filter by one or more workflow ID')
@click.option('--name', multiple=True, help='Filter by one or more workflow name')
@click.option('--status', multiple=True, help='Filter by one or more workflow status')
@click.option('-f', '--format', 'output_format', default='console', type=click.Choice(['console', 'csv', 'json']),
              help='Format of output', show_default=True)
def query(host, api, id, name, status, output_format):
    """List tasks or workflows (Cromwell)"""
    if api == 'cromwell':
        cromwell_query(host, id, name, status, output_format)
    elif api == 'tes':
        tes_query(host, id, name, status, output_format)


@cli.command()
@click.option('--host', help='Server address', default='http://localhost:8000', show_default=True)
@click.option('--api', default='cromwell', type=click.Choice(['cromwell']), help='API', show_default=True)
@click.option('-f', '--format', 'output_format', default='console', type=click.Choice(['console', 'csv', 'json']),
              help='Format of output', show_default=True)
@click.argument('id')
def release(host, api, id, output_format):
    """Switch a workflow from 'On Hold' to 'Submitted' status"""
    client = CromwellClient(host)
    data = call_client_method(client.release, id)

    if output_format != 'console':
        click.echo('This method only supports console format as output.', err=True)

    click.echo(data)


@cli.command()
@click.option('--host', help='Server address', default='http://localhost:8000', show_default=True)
@click.option('--api', default='cromwell', type=click.Choice(['cromwell']), help='API', show_default=True)
@click.option('-f', '--format', 'output_format', default='console', type=click.Choice(['console', 'csv', 'json']),
              help='Format of output', show_default=True)
@click.argument('id')
def status(host, api, id, output_format):
    """Retrieves the current state for a workflow (Cromwell)"""
    client = CromwellClient(host)
    data = call_client_method(client.status, id)

    if output_format != 'console':
        click.echo('This method only supports console format as output.', err=True)

    click.echo(data)


@cli.command()
@click.option('--host', help='Server address', default='http://localhost:8000', show_default=True)
@click.option('--api', default='cromwell', type=click.Choice(['cromwell']), help='API', show_default=True)
@click.option('--inputs', help='Path to inputs file')
@click.option('--dependencies', help='ZIP file containing workflow source files that are used to resolve local imports')
@click.option('--options', help='Path to options file')
@click.option('--labels', help='Labels file to apply to this workflow')
@click.option('--language', type=click.Choice(['WDL', 'CWL']), help='Workflow file format')
@click.option('--language-version', type=click.Choice(['draft-2', '1.0']), help='Language version')
@click.option('--hold', is_flag=True, default=False, help='Put workflow on hold upon submission')
@click.option('--root', help='The root object to be run (CWL)')
@click.option('-f', '--format', 'output_format', default='console', type=click.Choice(['console', 'csv', 'json']),
              help='Format of output', show_default=True)
@click.argument('workflow')
def submit(host, api, workflow, inputs, dependencies, options, labels, language, language_version, root, hold,
           output_format):
    """Submit a workflow for execution (Cromwell)"""
    client = CromwellClient(host)
    data = call_client_method(client.submit, workflow, inputs, dependencies, options, labels, language,
                              language_version, root, hold)

    if output_format != 'console':
        click.echo('This method only supports console format as output.', err=True)

    click.echo(data)


def cromwell_outputs(host, id, output_format):
    if not host:
        host = 'http://localhost:8000'
    client = CromwellClient(host)
    data = call_client_method(client.outputs, id)

    if output_format == 'json':
        click.echo(dumps(data))
    elif output_format == 'csv':
        fixed_data = []
        for task_name, outputs in data.items():
            if isinstance(outputs, str):
                fixed_data.append(dict(task=task_name, shardIndex=0, file=outputs))
                continue
            for i, output in enumerate(outputs):
                fixed_data.append(dict(task=task_name, shardIndex=i, file=output))
        write_as_csv(fixed_data)
    else:
        for task in data:
            click.echo(task)
            if type(data[task]) is str:
                click.echo(data[task])
                continue
            if any(isinstance(i, list) for i in data[task]):
                files = itertools.chain.from_iterable(data[task])
            else:
                files = data[task]
            for file in files:
                click.echo(file)


def tes_outputs(host, task_id, output_format):
    if not host:
        host = 'http://localhost:8080'
    client = TesClient(host)
    data = client.get_task(task_id, view='BASIC')

    if output_format == 'json':
        write_as_json(data.outputs)
    elif output_format == 'csv':
        write_as_csv(data.outputs)
    else:
        click.echo(data.outputs)


@cli.command()
@click.option('--host', help='Server address')
@click.option('--api', default='cromwell', type=click.Choice(['cromwell', 'tes']), help='API', show_default=True)
@click.option('-f', '--format', 'output_format', default='console', type=click.Choice(['console', 'csv', 'json']),
              help='Format of output', show_default=True)
@click.argument('id')
def outputs(host, api, id, output_format):
    """Get the outputs for a workflow"""
    if api == 'cromwell':
        cromwell_outputs(host, id, output_format)
    else:
        tes_outputs(host, id, output_format)


@cli.command()
@click.option('--host', help='Server address', default='http://localhost:8000', show_default=True)
@click.option('--api', default='cromwell', type=click.Choice(['cromwell']), help='API', show_default=True)
@click.option('--language', type=click.Choice(['WDL', 'CWL']), help='Workflow file format')
@click.option('--language-version', type=click.Choice(['draft-2', '1.0']), help='Language version')
@click.option('-f', '--format', 'output_format', default='console', type=click.Choice(['console', 'csv', 'json']),
              help='Format of output', show_default=True)
@click.argument('workflow')
def validate(host, api, workflow, language, language_version, output_format):
    """Describe a workflow (Cromwell)"""
    client = CromwellClient(host)
    data = call_client_method(client.describe, workflow, language, language_version)

    if output_format != 'console':
        click.echo('This method only supports console format as output.', err=True)

    click.echo('Valid' if data.get('valid') else 'Invalid')


@cli.command()
@click.option('--host', help='Server address', default='http://localhost:8000', show_default=True)
@click.option('--api', default='cromwell', type=click.Choice(['cromwell']), help='API', show_default=True)
@click.option('-f', '--format', 'output_format', default='console', type=click.Choice(['console', 'csv', 'json']),
              help='Format of output', show_default=True)
def version(host, api, output_format):
    """Return the version of this Cromwell server"""
    client = CromwellClient(host)
    data = call_client_method(client.version)

    if output_format != 'console':
        click.echo('This method only supports console format as output.', err=True)

    click.echo(data)
