
select event_time
, event_date
, date_format(event_date, 'yyyy-MM') AS year_month
, workspace_id
, service_name
, action_name
, request_id
, account_id
, request_params.runCreatorUserName
, request_params.warehouseId
, request_params.alertId
, request_params.jobId
, request_params.runId
, request_params.runTaskType
, request_params.idInJob
, request_params.jobTerminalState
, request_params.taskKey
, request_params.jobTriggerType
from system.access.audit  
where action_name in ('runFailed', 'runSucceeded');