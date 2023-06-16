select account_id
, workspace_id
, usage_record_id
, sku_name
, cloud 
, usage_start_time 
, usage_end_time
, usage_date
, date_format(usage_date, 'yyyy-MM') as YearMonth
, usage_unit
, usage_quantity
, list_price
, list_price*usage_quantity as list_cost
, custom_tags.Team -- parse out custom tags if available 
, system_metadata
, system_metadata.cluster_id
, system_metadata.job_id
, system_metadata.sql_endpoint_id 
, system_metadata.instance_pool_id
, system_metadata.node_type

from system.billing.usage
left join main.default.sku_cost_lookup on sku_name = sku

where contains(sku_name, 'INFERENCE')