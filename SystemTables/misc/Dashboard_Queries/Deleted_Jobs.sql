select workspace_id
, request_params.job_id
, event_time
, event_date 
, user_identity.email as user_email
, action_name 
, event_id

from system.access.audit 

where action_name = 'delete' 
and service_name = 'jobs'
and response.status_code = 200


