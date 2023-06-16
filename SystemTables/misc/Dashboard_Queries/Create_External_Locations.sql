
select event_time
, event_date
, date_format(event_date, 'yyyy-MM') AS year_month
, user_identity.email as user_email
, service_name
, action_name
, request_id
, request_params.name as external_location_name
, request_params.skip_validation
, request_params.credential_name
, request_params.url
, request_params.workspace_id
, request_params.metastore_id
, response.status_code


from system.access.audit 


where action_name = 'createExternalLocation' 
