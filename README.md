# Workflow and task management for genomics research

__wftools__ is a command line tool and package for managing genomics data processing workflows.
It communicate with workflows and tasks execution server through the following APIs:

- [Cromwell](https://cromwell.readthedocs.io/en/develop/api/RESTAPI/)
- [Workflow Execution Service (WES)](https://github.com/ga4gh/workflow-execution-service-schemas)
- [Task Execution Service (TES)](https://github.com/ga4gh/task-execution-schemas)

```bash
wftools
```

    Usage: wftools [OPTIONS] COMMAND [ARGS]...
    
      Workflow and task management for genomics research
    
    Options:
      --help  Show this message and exit.
    
    Commands:
      cromwell  Cromwell
      tes       Task Execution Schema
      wes       Workflow Execution Schema

## Cromwell commands

- abort     Abort a running workflow
- collect   Copy or move output files to directory
- describe  Describe a workflow
- info      Ger server info
- list      List workflows
- logs      Get the logs for a workflow
- outputs   Get the outputs for a workflow
- release   Switch from 'On Hold' to 'Submitted' status
- status    Retrieves the current state for a workflow
- submit    Submit a workflow for execution
- validate  Validate a workflow and its inputs
- version   Return the version of this Cromwell server

Set `CROMWELL_SERVER` environment variable to omit `--host` argument.

```bash
export CROMWELL_SERVER=http://localhost:8000
wftools cromwell info
```

## TES commands

- abort   Abort a running task
- info    Information about the service
- list    List tasks
- status  Retrieves the current state of a task

Set `TES_SERVER` environment variable to omit `--host` argument.

```bash
export TES_SERVER=http://localhost:8000
wftools tes info
```

## WES commands

- abort   Cancel a running workflow
- info    Get information about service
- list    List the workflow runs
- logs    Get detailed info about a workflow run
- status  Get quick status info about a workflow run
- submit  Run a workflow

Set `WES_SERVER` environment variable to omit `--host` argument.

```bash
export WES_SERVER=http://localhost:8000
wftools wes info
```