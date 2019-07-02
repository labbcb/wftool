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
@click.option('--host', help='Server address', required=True)
@click.argument('workflow_id')
def abort(host, workflow_id):
    """Abort a running workflow or task"""
    client = CromwellClient(host)
    data = call_client_method(client.abort, workflow_id)
    click.echo(data)


@cli.command()
@click.option('--host', help='Server address', required=True)
@click.option('--no-task-dir', is_flag=True, default=False, help='Do not create subdirectories for tasks')
@click.option('--copy/--move', 'copy', default=True, help='Copy or move output files? Copy by default.')
@click.option('--overwrite', is_flag=True, default=False, help='Overwrite existing files.')
@click.argument('workflow_id')
@click.argument('destination', type=click.Path())
def collect(host, workflow_id, no_task_dir, copy, overwrite, destination):
    """Copy or move output files of workflow to directory"""
    client = CromwellClient(host)
    data = call_client_method(client.outputs, workflow_id)

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
@click.option('--host', help='Server address', required=True)
@click.option('--inputs', help='Path to inputs file')
@click.option('--language', type=click.Choice(['WDL', 'CWL']), help='Workflow file format')
@click.option('--language-version', type=click.Choice(['draft-2', '1.0']), help='Language version')
@click.argument('workflow')
def describe(host, workflow, inputs, language, language_version):
    """Describe a workflow"""
    client = CromwellClient(host)
    data = call_client_method(client.describe, workflow, inputs, language, language_version)
    click.echo(dumps(data))


@cli.command()
@click.option('--host', help='Server address', required=True)
@click.option('-f', '--format', 'output_format', default='console', type=click.Choice(['console', 'json']),
              help='Format of output', show_default=True)
def info(host, output_format):
    """Ger server info"""
    client = CromwellClient(host)
    data = call_client_method(client.backends)

    if output_format == 'json':
        write_as_json(data)
    else:
        click.echo('Default backend: {}'.format(data.get('defaultBackend')))
        click.echo('Supported backends: {}'.format(','.join(data.get('supportedBackends'))))


@cli.command()
@click.option('--host', help='Server address', required=True)
@click.option('-f', '--format', 'output_format', default='console', type=click.Choice(['console', 'csv', 'json']),
              help='Format of output', show_default=True)
@click.argument('workflow_id')
def logs(host, workflow_id, output_format):
    """Get the logs for a workflow"""
    client = CromwellClient(host)
    data = call_client_method(client.logs, workflow_id)

    if output_format == 'json':
        click.echo(dumps(data))
    elif output_format == 'csv':
        fixed_data = []
        for task_name, task_logs in data.items():
            for log in task_logs:
                log['task'] = task_name
                fixed_data.append(log)
        write_as_csv(fixed_data)
    else:
        for task in data:
            click.echo('Task {}'.format(task))
            for idx in range(len(data[task])):
                click.echo('Shard {} stdout: {}'.format(idx, data[task][idx]['stdout']))
                click.echo('Shard {} stderr: {}'.format(idx, data[task][idx]['stderr']))


@cli.command()
@click.option('--host', help='Server address', required=True)
@click.argument('workflow_id')
def release(host, workflow_id):
    """Switch a workflow from 'On Hold' to 'Submitted' status"""
    client = CromwellClient(host)
    data = call_client_method(client.release, workflow_id)
    click.echo(data)


@cli.command()
@click.option('--host', help='Server address', required=True)
@click.argument('workflow_id')
def status(host, workflow_id):
    """Retrieves the current state for a workflow"""
    client = CromwellClient(host)
    data = call_client_method(client.status, workflow_id)
    click.echo(data)


@cli.command()
@click.option('--host', help='Server address', required=True)
@click.option('--id', 'ids', multiple=True, help='Filter by one or more task ID')
@click.option('--name', 'names', multiple=True, help='Filter by one or more task name')
@click.option('--status', 'states', multiple=True, help='Filter by one or more task states')
@click.option('-f', '--format', 'output_format', default='console', type=click.Choice(['console', 'csv', 'json']),
              help='Format of output', show_default=True)
def tasks(host, ids, names, states, output_format):
    """List tasks"""
    client = TesClient(host)
    data = client.list_tasks('FULL')

    if ids:
        data['tasks'] = [t for t in data.get('tasks') if t.get('id') in ids]
    if names:
        data['tasks'] = [t for t in data.get('tasks') if t.get('name') in names]
    if states:
        states = [t.upper() for t in states]
        data['tasks'] = [t for t in data.get('tasks') if t.get('state') in states]

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


@cli.command()
@click.option('--host', help='Server address', required=True)
@click.option('--inputs', help='Path to inputs file')
@click.option('--dependencies', help='ZIP file containing workflow source files that are used to resolve local imports')
@click.option('--options', help='Path to options file')
@click.option('--labels', help='Labels file to apply to this workflow')
@click.option('--language', type=click.Choice(['WDL', 'CWL']), help='Workflow file format')
@click.option('--language-version', type=click.Choice(['draft-2', '1.0']), help='Language version')
@click.option('--hold', is_flag=True, default=False, help='Put workflow on hold upon submission')
@click.option('--root', help='The root object to be run (CWL)')
@click.argument('workflow')
def submit(host, workflow, inputs, dependencies, options, labels, language, language_version, root, hold):
    """Submit a workflow for execution"""
    client = CromwellClient(host)
    data = call_client_method(client.submit, workflow, inputs, dependencies, options, labels, language,
                              language_version, root, hold)
    click.echo(data)


@cli.command()
@click.option('--host', help='Server address', required=True)
@click.option('-f', '--format', 'output_format', default='console', type=click.Choice(['console', 'csv', 'json']),
              help='Format of output', show_default=True)
@click.argument('workflow_id')
def outputs(host, workflow_id, output_format):
    """Get the outputs for a workflow"""
    client = CromwellClient(host)
    data = call_client_method(client.outputs, workflow_id)

    if output_format == 'json':
        click.echo(dumps(data))
    elif output_format == 'csv':
        fixed_data = []
        for task_name, task_outputs in data.items():
            if isinstance(task_outputs, str):
                fixed_data.append(dict(task=task_name, shardIndex=0, file=task_outputs))
                continue
            for i, output in enumerate(task_outputs):
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


@cli.command()
@click.option('--host', help='Server address', required=True)
@click.option('--inputs', help='Path to inputs file')
@click.option('--language', type=click.Choice(['WDL', 'CWL']), help='Workflow file format')
@click.option('--language-version', type=click.Choice(['draft-2', '1.0']), help='Language version')
@click.argument('workflow')
def validate(host, workflow, inputs, language, language_version):
    """Validate a workflow and (optionally) its inputs"""
    client = CromwellClient(host)
    data = call_client_method(client.describe, workflow, inputs, language, language_version)
    if data.get('valid'):
        click.echo('Valid')
    else:
        click.echo('Invalid')
        for error in data.get('errors'):
            click.echo(error, err=True)


@cli.command()
@click.option('--host', help='Server address', required=True)
def version(host):
    """Return the version of this Cromwell server"""
    client = CromwellClient(host)
    data = call_client_method(client.version)
    click.echo(data)


@cli.command()
@click.option('--host', help='Server address', required=True)
@click.option('--id', 'ids', multiple=True, help='Filter by one or more workflow IDs')
@click.option('--name', 'names', multiple=True, help='Filter by one or more workflow names')
@click.option('--status', 'statuses', multiple=True, help='Filter by one or more workflow status')
@click.option('-f', '--format', 'output_format', default='console', type=click.Choice(['console', 'csv', 'json']),
              help='Format of output', show_default=True)
def workflows(host, ids, names, statuses, output_format):
    """List workflows"""
    client = CromwellClient(host)
    data = call_client_method(client.query, ids, names, statuses)

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
