# Workflow and task management for genomics research

__wftools__ is command line tool and package for managing genomics data processing workflows.
It communicate with workflows and tasks execution server through APIs:

- Cromwell
- Workflow Execution Service
- Task Execution Services

To use __wftools__ you should have access to one more API endpoints.
If you just want to test this tool see [Deploying servers for testing](#Deploying servers for testing)

Workflow specific commands

- submit - Submit a _workflow_ for execution
- abort - Abort a running workflow
- collect - Copy or move output files of workflow to directory
- logs - Get the logs for a workflow
- describe - Describe a workflow
- status - Retrieves the current state for a workflow
- workflows - List workflows

Task specific commands

- cancel - Abort a running _task_
- state - Retrieves the current state of a task
- tasks - List tasks

Cromwell specific commands

- outputs - Get the outputs for a workflow
- info - Ger server info
- validate - Validate a workflow and (optionally) its inputs
- release - Switch a workflow from 'On Hold' to 'Submitted' status
- version - Return the version of this Cromwell server

## Deploying servers for testing
