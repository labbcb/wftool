# Workflow and task management for genomics research

__wftools__ is command line tool and package for managing genomics data processing workflows.
It communicate with workflows and tasks execution server through APIs:

- [Cromwell](https://cromwell.readthedocs.io/en/develop/api/RESTAPI/)
- [Workflow Execution Service (WES)](https://github.com/ga4gh/workflow-execution-service-schemas)
- [Task Execution Service (TES)](https://github.com/ga4gh/task-execution-schemas)

Commands

    abort             Abort a running workflow (Cromwell)
    cancel            Abort a running task (TES)
    cancel-run        Cancel a running workflow (WES)
    collect           Copy or move output files to directory (Cromwell)
    describe          Describe a workflow (Cromwell)
    info              Ger server info (Cromwell)
    logs              Get the logs for a workflow (Cromwell)
    outputs           Get the outputs for a workflow (Cromwell)
    release           Switch from 'On Hold' to 'Submitted' status (Cromwell)
    run-log           Get detailed info about a workflow run (WES)
    run-status        Get quick status info about a workflow run (WES)
    run-workflow      Run a workflow (WES)
    runs              List the workflow runs (WES)
    service-tes-info  Information about the service (TES)
    service-wes-info  Get information about Workflow Execution Service (WES)
    state             Retrieves the current state of a task (TES)
    status            Retrieves the current state for a workflow (Cromwell)
    submit            Submit a workflow for execution (Cromwell)
    tasks             List tasks (TES)
    validate          Validate a workflow and its inputs (Cromwell)
    version           Return the version of this Cromwell server
    workflows         List workflows (Cromwell)
