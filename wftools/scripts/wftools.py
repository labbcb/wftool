import itertools
import os
import shutil
from json import dumps

import click

from . import write_as_csv, write_as_json
from ..cromwell import CromwellClient
from ..tes import TesClient
from ..wes import WesClient


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


@cli.group()
def cromwell():
    """Cromwell API commands"""


@cromwell.command('abort')
@click.option('-h', '--host', help='Server address', required=True, envvar='CROMWELL_SERVER')
@click.argument('workflow_id')
def cromwell_abort(host, workflow_id):
    """Abort a running workflow"""
    client = CromwellClient(host)
    data = call_client_method(client.abort, workflow_id)
    click.echo(data)


@cromwell.command('collect')
@click.option('-h', '--host', help='Server address', required=True, envvar='CROMWELL_SERVER')
@click.option('--no-task-dir', is_flag=True, default=False, help='Do not create subdirectories for tasks')
@click.option('--copy/--move', 'copy', default=True, help='Copy or move output files? Copy by default.')
@click.option('--overwrite', is_flag=True, default=False, help='Overwrite existing files.')
@click.argument('workflow_id')
@click.argument('destination', type=click.Path())
def cromwell_collect(host, workflow_id, no_task_dir, copy, overwrite, destination):
    """Copy or move output files to directory"""
    client = CromwellClient(host)
    data = call_client_method(client.outputs, workflow_id)

    if not os.path.exists(destination):
        os.mkdir(destination)

    for task_name, task_outputs in data.items():
        if no_task_dir:
            task_dir = destination
        else:
            task_dir = os.path.join(destination, task_name)
            if not os.path.exists(task_dir):
                os.mkdir(task_dir)

        if isinstance(task_outputs, str):
            files = [task_outputs]
        elif any(isinstance(i, list) for i in task_outputs):
            files = itertools.chain.from_iterable(task_outputs)
        else:
            files = task_outputs

        for src_file in files:
            if os.path.exists(src_file):
                dst_file = os.path.join(task_dir, os.path.basename(src_file))
                if os.path.exists(dst_file) and not overwrite:
                    click.echo('File already exists: ' + dst_file, err=True)
                    exit(1)
                if copy:
                    shutil.copyfile(src_file, dst_file)
                else:
                    shutil.move(src_file, dst_file)
            else:
                click.echo('File not found: ' + src_file, err=True)


@cromwell.command('describe')
@click.option('-h', '--host', help='Server address', required=True, envvar='CROMWELL_SERVER')
@click.option('-i', '--inputs', help='Path to inputs file')
@click.option('-l', '--language', type=click.Choice(['WDL', 'CWL']), help='Workflow file format')
@click.option('-v', '--version', 'language_version', type=click.Choice(['draft-2', '1.0']), help='Language version')
@click.argument('workflow')
def cromwell_describe(host, workflow, inputs, language, language_version):
    """Describe a workflow"""
    client = CromwellClient(host)
    data = call_client_method(client.describe, workflow, inputs, language, language_version)
    click.echo(dumps(data))


@cromwell.command('info')
@click.option('-h', '--host', help='Server address', required=True, envvar='CROMWELL_SERVER')
@click.option('-f', '--format', 'output_format', default='console', type=click.Choice(['console', 'json']),
              help='Format of output')
def cromwell_info(host, output_format):
    """Ger server info"""
    client = CromwellClient(host)
    data = call_client_method(client.info)

    if output_format == 'json':
        write_as_json(data)
    else:
        click.echo('Default backend: {}'.format(data.get('defaultBackend')))
        click.echo('Supported backends: {}'.format(','.join(data.get('supportedBackends'))))


@cromwell.command('list')
@click.option('-h', '--host', help='Server address', required=True, envvar='CROMWELL_SERVER')
@click.option('-i', '--id', 'ids', multiple=True, help='Filter by one or more workflow IDs')
@click.option('-n', '--name', 'names', multiple=True, help='Filter by one or more workflow names')
@click.option('-s', '--status', 'statuses', multiple=True, help='Filter by one or more workflow status')
@click.option('-f', '--format', 'output_format', default='console', type=click.Choice(['console', 'csv', 'json']),
              help='Format of output')
def cromwell_list(host, ids, names, statuses, output_format):
    """List workflows"""
    client = CromwellClient(host)
    data = call_client_method(client.list, ids, names, statuses)

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


@cromwell.command('logs')
@click.option('-h', '--host', help='Server address', required=True, envvar='CROMWELL_SERVER')
@click.option('-f', '--format', 'output_format', default='console', type=click.Choice(['console', 'csv', 'json']),
              help='Format of output')
@click.argument('workflow_id')
def cromwell_logs(host, workflow_id, output_format):
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


@cromwell.command('release')
@click.option('-h', '--host', help='Server address', required=True, envvar='CROMWELL_SERVER')
@click.argument('workflow_id')
def cromwell_release(host, workflow_id):
    """Switch from 'On Hold' to 'Submitted' status"""
    client = CromwellClient(host)
    data = call_client_method(client.release, workflow_id)
    click.echo(data)


@cromwell.command('status')
@click.option('-h', '--host', help='Server address', required=True, envvar='CROMWELL_SERVER')
@click.argument('workflow_id')
def cromwell_status(host, workflow_id):
    """Retrieves the current state for a workflow"""
    client = CromwellClient(host)
    data = call_client_method(client.status, workflow_id)
    click.echo(data)


@cromwell.command('submit')
@click.option('-h', '--host', help='Server address', required=True, envvar='CROMWELL_SERVER')
@click.option('-i', '--inputs', help='Path to inputs file')
@click.option('-d', '--dependencies',
              help='ZIP file containing workflow source files that are used to resolve local imports')
@click.option('-o', '--options', help='Path to options file')
@click.option('-t', '--labels', help='Labels file to apply to this workflow')
@click.option('-l', '--language', type=click.Choice(['WDL', 'CWL']), help='Workflow file format')
@click.option('-v', '--version', 'language_version', type=click.Choice(['draft-2', '1.0']), help='Language version')
@click.option('--hold', is_flag=True, default=False, help='Put workflow on hold upon submission')
@click.option('--root', help='The root object to be run (CWL)')
@click.argument('workflow')
def cromwell_submit(host, workflow, inputs, dependencies, options, labels, language, language_version, root, hold):
    """Submit a workflow for execution"""
    client = CromwellClient(host)
    data = call_client_method(client.submit, workflow, inputs, dependencies, options, labels, language,
                              language_version, root, hold)
    click.echo(data)


@cromwell.command('outputs')
@click.option('-h', '--host', help='Server address', required=True, envvar='CROMWELL_SERVER')
@click.option('-f', '--format', 'output_format', default='console', type=click.Choice(['console', 'csv', 'json']),
              help='Format of output')
@click.argument('workflow_id')
def cromwell_outputs(host, workflow_id, output_format):
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


@cromwell.command('validate')
@click.option('-h', '--host', help='Server address', required=True, envvar='CROMWELL_SERVER')
@click.option('-i', '--inputs', help='Path to inputs file')
@click.option('-l', '--language', type=click.Choice(['WDL', 'CWL']), help='Workflow file format')
@click.option('-v', '--version', 'language_version', type=click.Choice(['draft-2', '1.0']), help='Language version')
@click.argument('workflow')
def cromwell_validate(host, workflow, inputs, language, language_version):
    """Validate a workflow and its inputs"""
    client = CromwellClient(host)
    data = call_client_method(client.describe, workflow, inputs, language, language_version)
    if data.get('valid'):
        click.echo('Valid')
    else:
        click.echo('Invalid')
        for error in data.get('errors'):
            click.echo(error, err=True)


@cromwell.command('version')
@click.option('-h', '--host', help='Server address', required=True, envvar='CROMWELL_SERVER')
def cromwell_version(host):
    """Return the version of this Cromwell server"""
    client = CromwellClient(host)
    data = call_client_method(client.version)
    click.echo(data)


@cli.group()
def tes():
    """GA4GH TES API commands"""


@tes.command('abort')
@click.option('-h', '--host', help='Server address', required=True, envvar='TES_SERVER')
@click.argument('task_id')
def tes_abort(host, task_id):
    """Abort a running task"""
    client = TesClient(host)
    data = call_client_method(client.abort, task_id)
    click.echo(data)


@tes.command('info')
@click.option('-h', '--host', help='Server address', required=True, envvar='TES_SERVER')
def tes_info(host):
    """Information about the service"""
    client = TesClient(host)
    data = call_client_method(client.info)
    write_as_json(data)


@tes.command('list')
@click.option('-h', '--host', help='Server address', required=True, envvar='TES_SERVER')
@click.option('-i', '--id', 'ids', multiple=True, help='Filter by one or more task ID')
@click.option('-n', '--name', 'names', multiple=True, help='Filter by one or more task name')
@click.option('-s', '--status', 'states', multiple=True, help='Filter by one or more task states')
@click.option('-f', '--format', 'output_format', default='console', type=click.Choice(['console', 'csv', 'json']),
              help='Format of output')
def tes_list(host, ids, names, states, output_format):
    """List tasks"""
    client = TesClient(host)
    data = call_client_method(client.list, 'FULL')

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
            resources = task.get('resources') if task.get('resources') else dict()
            click.echo('{:24}  {:8}  {:28}  {:3}  {:6.2f}  {:6.2f}'.format(task.get('id'),
                                                                           task.get('state'),
                                                                           task.get('creation_time', '-'),
                                                                           resources.get('cpu_cores', 0),
                                                                           resources.get('ram_gb', 0),
                                                                           resources.get('disk_gb', 0)))


@tes.command('status')
@click.option('-h', '--host', help='Server address', required=True, envvar='TES_SERVER')
@click.argument('task_id')
def tes_status(host, task_id):
    """Retrieves the current state of a task"""
    client = TesClient(host)
    data = call_client_method(client.status, task_id)
    click.echo(data.get('state'))


@cli.group()
def wes():
    """GA4GH WES API commands"""


@wes.command('abort')
@click.option('-h', '--host', help='Server address', required=True, envvar='WES_SERVER')
@click.argument('task_id')
def wes_abort(host, task_id):
    """Cancel a running workflow"""
    client = WesClient(host)
    data = call_client_method(client.abort, task_id)
    click.echo(data)


@wes.command('info')
@click.option('-h', '--host', help='Server address', required=True, envvar='WES_SERVER')
def wes_info(host):
    """Get information about service"""
    client = WesClient(host)
    data = call_client_method(client.info)
    write_as_json(data)


@wes.command('list')
@click.option('-h', '--host', help='Server address', required=True, envvar='WES_SERVER')
def wes_list(host):
    """List the workflow runs"""
    client = WesClient(host)
    data = call_client_method(client.list)
    write_as_json(data)


@wes.command('logs')
@click.option('-h', '--host', help='Server address', required=True, envvar='WES_SERVER')
@click.argument('run_id')
def wes_logs(host, run_id):
    """Get detailed info about a workflow run"""
    client = WesClient(host)
    data = call_client_method(client.logs, run_id)
    click.echo(data)


@wes.command('status')
@click.option('-h', '--host', help='Server address', required=True, envvar='WES_SERVER')
@click.argument('run_id')
def wes_status(host, run_id):
    """Get quick status info about a workflow run"""
    client = WesClient(host)
    data = call_client_method(client.status, run_id)
    click.echo(data)


@wes.command('submit')
@click.option('-h', '--host', help='Server address', required=True, envvar='WES_SERVER')
@click.option('-i', '--inputs', help='Path to inputs file')
@click.option('-d', '--dependencies',
              help='ZIP file containing workflow source files that are used to resolve local imports')
@click.option('-o', '--options', help='Path to options file')
@click.option('-t', '--tags', help='Labels file to apply to this workflow')
@click.option('-l', '--language', type=click.Choice(['WDL', 'CWL']), help='Workflow file format')
@click.option('-v', '--version', 'language_version', type=click.Choice(['draft-2', '1.0', 'v1.0']),
              help='Language version')
@click.argument('workflow')
def wes_submit(host, workflow, inputs, dependencies, options, tags, language, language_version):
    """Run a workflow"""
    client = WesClient(host)
    data = call_client_method(client.submit, workflow, inputs, language, language_version, dependencies, options, tags)
    click.echo(data)
