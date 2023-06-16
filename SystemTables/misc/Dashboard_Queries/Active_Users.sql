-- Daily and Monthly Active Users 
select distinct
event_date as `Date`
, date_format(event_date, 'yyyy-MM') AS year_month
, workspace_id
, user_identity.email as user_email

from system.access.audit 


