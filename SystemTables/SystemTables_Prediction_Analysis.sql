-- Databricks notebook source
--- Evaluations on Metrics 


select *
  , case when y >= yhat_lower AND y <= yhat_upper THEN true ELSE false END AS on_trend
from ryan_chynoweth_catalog.ryan_chynoweth_schema.dbu_forecasts_28evals 
order by ds desc

-- COMMAND ----------

--- Evaluations on Metrics 


select *
  , case when y >= yhat_lower AND y <= yhat_upper THEN true ELSE false END AS on_trend
from ryan_chynoweth_catalog.ryan_chynoweth_schema.dbu_forecasts_7evals 
where max_forecast_date = '2023-05-10'
order by ds desc

-- COMMAND ----------

--- Evaluations on Metrics 


select a.training_date
, a.workspace_id
, a.sku
, a.max_forecast_date
, a.mae
, a.mse
, a.rmse
, sum(b.y) as TotalDBUs 
, avg(b.y) as AvgDBUs
, try_divide(a.mae, avg(b.y) ) as mae_avg_ratio
, try_divide(a.rmse, avg(b.y)) as rmse_avg_ratio


from ryan_chynoweth_catalog.ryan_chynoweth_schema.dbu_forecasts_28eval_metrics a
inner join ryan_chynoweth_catalog.ryan_chynoweth_schema.dbu_forecasts_28evals b on a.workspace_id = b.workspace_id and a.sku = b.sku and a.max_forecast_date = b.max_forecast_date




group by 1,2,3,4,5,6,7

order by TotalDBUs desc



-- COMMAND ----------

--- Evaluations on Metrics 

select a.training_date
, a.workspace_id
, a.sku
, a.max_forecast_date
, a.mae
, a.mse
, a.rmse
, sum(b.y) as TotalDBUs 
, avg(b.y) as AvgDBUs
, try_divide(a.mae, avg(b.y) ) as mae_avg_ratio
, try_divide(a.rmse, avg(b.y)) as rmse_avg_ratio


from ryan_chynoweth_catalog.ryan_chynoweth_schema.dbu_forecasts_7eval_metrics a
inner join ryan_chynoweth_catalog.ryan_chynoweth_schema.dbu_forecasts_7evals b on a.workspace_id = b.workspace_id and a.sku = b.sku and a.max_forecast_date = b.max_forecast_date




group by 1,2,3,4,5,6,7

order by TotalDBUs desc



-- COMMAND ----------


