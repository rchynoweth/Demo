# System Table Data Analysis

This directory is dedicated to system table data analysis. The primary purpose at this time is to support DBU forecasts.  

The following is a description of the current collateral:  
1. `libs.ddl_helper.py`: helper functions for ddl creation for visualization purposes. 
1. `libs.dbu_prophet_forecast.py`: class supporting forecast creation  
1. `run_dbu_forecasts.py`: primary notebook for granular DBU and $DBU forecasts. ETL and Modeling Only. 
1. `run_dbu_forecasts_conslidated_sku.py`: primary notebook for consolidated DBU and $DBU forecasts. ETL and Modeling Only. All SKUs are rolled up to a generic SKU i.e. `ALL_PURPOSE`, `SQL` etc. 
1. `View_DBU_Forecasts.py`: notebook to visualize forecast results.  
1. `run_dbu_forecasts_consolidated_analysis.py`: evaluation notebook to understand model performance. Does not use MLflow yet.  
1. `misc` directory: please disregard. This is used for exploratory analysis. 