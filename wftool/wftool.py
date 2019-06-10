import os
import shutil
import sys
import json
import itertools

import click

from wftool import client


@click.group()
def cli():
    """Task and workflow management for genomics research"""
    pass


@cli.command()
@click.option('--host', required=True, help='Server address')
@click.option('--workflow', required=True, help='Path to workflow file')
@click.option('--inputs', help='Path to inputs file')
@click.option('--version', "v1", help='Cromwell API version')
@click.option('--language', type=click.Choice(['WDL', 'CWL']), help='Workflow file format')
@click.option('--language-version', type=click.Choice(['draft-2', '1.0']), help='Language version')
@click.option('--format', 'output_format', default='console', type=click.Choice(['console', 'csv', 'json']),
              help='Format of output', show_default=True)
@click.argument('output', type=click.File('w'), default='-', required=False)
def describe(host, workflow, inputs, language, language_version, version, output_format, output):
    """Describe a workflow (Cromwell)"""
    r = client.describe(host, workflow, inputs, language, language_version, version)
    if output_format == 'json':
        click.echo(r.json(), file=output)


@cli.command()
@click.option('--host', required=True, help='Server address')
@click.option('--workflow', required=True, help='Path to workflow file')
@click.option('--inputs', required=True, help='Path to inputs file')
@click.option('--dependencies', help='ZIP file containing workflow source files that are used to resolve local imports')
@click.option('--options', help='Path to options file')
@click.option('--labels', help='Labels file to apply to this workflow')
@click.option('--language', type=click.Choice(['WDL', 'CWL']), help='Workflow file format')
@click.option('--language-version', type=click.Choice(['draft-2', '1.0']), help='Language version')
@click.option('--version', default='v1', help='Cromwell API version')
@click.option('--api', default='cromwell', type=click.Choice(['cromwell', 'wes']), help='API', show_default=True)
@click.argument('output', type=click.File('w'), default='-', required=False)
def submit(host, workflow, inputs, dependencies, options, labels, language, language_version, version, api, output):
    """Submit a workflow for execution (Cromwell)"""
    if api != 'cromwell':
        click.echo('API not supported: ' + api, err=True)
        exit(1)

    r = client.submit(host, workflow, inputs, options, dependencies, labels, language, language_version, version).json()
    if r['status'] == 'Submitted':
        click.echo(r['id'], file=output)
    else:
        click.echo(r['status'], err=True)
        exit(1)


@cli.command('list')
@click.option('--host', required=True, help='Server address')
@click.option('--api', default='cromwell', type=click.Choice(['cromwell', 'wes', 'tes']), help='API',
              show_default=True)
@click.option('--version', default='v1', help='Cromwell API version')
@click.option('--format', 'output_format', default='console', type=click.Choice(['console', 'csv', 'json']),
              help='Format of output', show_default=True)
@click.argument('output', type=click.File('w'), default='-', required=False)
def list_jobs(host, api, version, output_format, output):
    """List tasks or workflows (Cromwell)"""
    if api != 'cromwell':
        click.echo('API not supported: ' + api, err=True)
        exit(1)

    data = client.query(host, version).json()
    if output_format == 'console':
        click.echo('{:36}  {:9}  {:24}  {:24}  {:24}  {}'.format('ID', 'Status', 'Start', 'End', 'Submitted', 'Name'),
                   file=output)
        for workflow in data['results']:
            click.echo('{:36}  {:9}  {:24}  {:24}  {:24}  {}'.format(workflow.get('id', '-'),
                                                                     workflow.get('status', '-'),
                                                                     workflow.get('start', '-'),
                                                                     workflow.get('end', '-'),
                                                                     workflow.get('submission', '-'),
                                                                     workflow.get('name', '-')),
                       file=output)
    elif output_format == 'csv':
        click.echo('ID,Status,Start,End,Submitted,Name', file=output)
        for workflow in data['results']:
            click.echo('{},{},{},{},{},{}'.format(workflow.get('id', ''),
                                                  workflow.get('status', ''),
                                                  workflow.get('start', ''),
                                                  workflow.get('end', ''),
                                                  workflow.get('submission', ''),
                                                  workflow.get('name', '')),
                       file=output)
    else:
        click.echo(json.dumps(data), file=output)


@cli.command()
@click.option('--host', required=True, help='Server address')
@click.option('--id', 'job_id', required=True, help='Workflow ID')
@click.option('--version', default='v1', help='Cromwell API version')
@click.option('--format', 'output_format', default='console', type=click.Choice(['console', 'csv', 'json']),
              help='Format of output', show_default=True)
@click.argument('output', type=click.File('w'), default='-', required=False)
def logs(host, job_id, output_format, version, output):
    """Get the logs for a workflow (Cromwell)"""
    data = client.logs(host, job_id, version).json()

    if data.get('status', None) == 'fail':
        click.echo(data['message'], err=True)
        exit(1)

    if output_format == 'console':
        for task in data['calls']:
            click.echo('Task {}'.format(task), file=output)
            for idx in range(len(data['calls'][task])):
                click.echo('Shard {} stdout: {}'.format(idx, data['calls'][task][idx]['stdout']), file=output)
                click.echo('Shard {} stderr: {}'.format(idx, data['calls'][task][idx]['stderr']), file=output)
    elif output_format == 'csv':
        click.echo('Task,Shard,Descriptor,File', file=output)
        for task in data['calls']:
            for idx in range(len(data['calls'][task])):
                click.echo('{},{},{},{}'.format(task, idx, 'stdout', data['calls'][task][idx]['stdout']), file=output)
                click.echo('{},{},{},{}'.format(task, idx, 'stderr', data['calls'][task][idx]['stderr']), file=output)
    else:
        click.echo(json.dumps(data), file=output)


@cli.command()
@click.option('--host', required=True, help='Server address')
@click.option('--id', 'job_id', required=True, help='Workflow ID')
@click.option('--version', default='v1', help='Cromwell API version')
@click.option('--format', 'output_format', default='console', type=click.Choice(['console', 'json']),
              help='Format of output', show_default=True)
@click.argument('output', type=click.File('w'), default='-', required=False)
def status(host, job_id, version, output_format, output):
    """Retrieves the current state for a workflow (Cromwell)"""
    data = client.status(host, job_id, version).json()

    if data.get('status', None) == 'fail':
        click.echo(data['message'], err=True)
        exit(1)

    if output_format == 'console':
        click.echo(data['status'], file=output)
    else:
        click.echo(json.dumps(data), file=output)


@cli.command()
@click.option('--host', required=True, help='Server address')
@click.option('--id', 'job_id', required=True, help='Workflow ID')
@click.option('--version', default='v1', help='Cromwell API version')
@click.option('--format', 'output_format', default='console', type=click.Choice(['console', 'csv', 'json']),
              help='Format of output', show_default=True)
@click.argument('output', type=click.File('w'), default='-', required=False)
def outputs(host, job_id, version, output_format, output):
    """Get the outputs for a workflow"""
    data = client.outputs(host, job_id, version).json()

    if data.get('status', None) == 'fail':
        click.echo(data['message'], err=True)
        exit(1)

    if output_format == 'console':
        for task in data['outputs']:
            click.echo('Task {}'.format(task), file=output)
            if type(data['outputs'][task]) is str:
                click.echo(data['outputs'][task], file=output)
                continue
            if any(isinstance(i, list) for i in data['outputs'][task]):
                files = itertools.chain.from_iterable(data['outputs'][task])
            else:
                files = data['outputs'][task]
            for file in files:
                click.echo(file, file=output)
    elif output_format == 'csv':
        click.echo('Task,File', file=output)
        for task in data['outputs']:
            if type(data['outputs'][task]) is str:
                click.echo('{},{}'.format(task, data['outputs'][task]), file=output)
                continue
            if any(isinstance(i, list) for i in data['outputs'][task]):
                files = itertools.chain.from_iterable(data['outputs'][task])
            else:
                files = data['outputs'][task]
            for file in files:
                click.echo('{},{}'.format(task, file), file=output)
    else:
        click.echo(json.dumps(data), file=output)


@cli.command()
@click.option('--host', required=True, help='Server address')
@click.option('--id', 'job_id', required=True, help='Workflow ID')
@click.option('--version', default='v1', help='Cromwell API version')
@click.option('--no-task-dir', is_flag=True, default=False, help='Do not create subdirectories for tasks')
@click.option('--copy/--move', 'copy', default=True, help='Copy or move output files? Copy by default.')
@click.argument('destination', default='.', type=click.Path())
def collect(host, job_id, no_task_dir, copy, version, destination):
    """Copy or move output files of workflow to directory (Cromwell)"""
    data = client.outputs(host, job_id, version).json()

    if data.get('status', None) == 'fail':
        click.echo(data['message'], err=True)
        exit(1)

    for task in data['outputs']:

        if no_task_dir:
            task_dir = destination
        else:
            task_dir = os.path.join(destination, task)
            if not os.path.exists(task_dir):
                os.mkdir(task_dir)

        if type(data['outputs'][task]) is str:
            files = [data['outputs'][task]]
        elif any(isinstance(i, list) for i in data['outputs'][task]):
            files = itertools.chain.from_iterable(data['outputs'][task])
        else:
            files = data['outputs'][task]

        for file in files:
            if os.path.exists(file):
                dest_file = os.path.join(task_dir, os.path.basename(file))
                if os.path.exists(dest_file):
                    click.echo('File already exists: ' + dest_file, err=True)
                    sys.exit(1)
                if copy:
                    shutil.copyfile(file, dest_file)
                else:
                    shutil.move(file, dest_file)
            else:
                click.echo('File not found: ' + file, err=True)
                sys.exit(1)
