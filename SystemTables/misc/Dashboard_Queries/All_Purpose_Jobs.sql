with created_jobs as (
  select 
    workspace_id
    , event_time as created_time
    , user_identity.email as creator
    , request_id
    , event_id
    , get_json_object(response.result, '$.job_id') as job_id
    , request_params.name as job_name
    , request_params.job_type 
    , request_params.schedule

    from system.access.audit 

    where action_name = 'create' 
    and service_name = 'jobs'
    and response.status_code = 200




), 
deleted_jobs as (

  select request_params.job_id, workspace_id
  from system.access.audit 

  where action_name = 'delete' 
  and service_name = 'jobs'
  and response.status_code = 200


)



select a.workspace_id
, a.sku_name
, a.cloud 
, a.usage_date
, date_format(usage_date, 'yyyy-MM') as YearMonth
, a.usage_unit
, d.list_price
, sum(a.usage_quantity) total_dbus
, sum(a.usage_quantity)*d.list_price as list_cost
, a.system_metadata.cluster_id
, a.system_metadata.job_id
, a.system_metadata.sql_endpoint_id 
, a.system_metadata.instance_pool_id
, a.system_metadata.node_type
, case when b.job_id is not null then TRUE else FALSE end as job_created_flag
, case when c.job_id is not null then TRUE else FALSE end as job_deleted_flag

from system.billing.usage a
  left join created_jobs b on a.workspace_id = b.workspace_id and a.system_metadata.job_id = b.job_id
  left join deleted_jobs c on a.workspace_id = c.workspace_id and a.system_metadata.job_id = c.job_id
  left join main.default.sku_cost_lookup d on sku_name = sku


where system_metadata.job_id is not Null 
and contains(sku_name, 'ALL_PURPOSE')

group by all