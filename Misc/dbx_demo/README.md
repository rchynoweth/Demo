# Using DBX for Development 

https://github.com/databrickslabs/dbx/blob/main/README.rst


## Set Up and Basics


1. Set up a connection. This uses the Databricks CLI.  
    ```
    dbx configure
    ```

1. Create a `conf/deployment.json` file that looks like the following. This would be used to execute code on a jobs cluster.    
    ```json
    {
        "default": {
            "jobs": [
                {
                    "name": "your-job-name",
                    "new_cluster": {
                        "spark_version": "7.3.x-cpu-ml-scala2.12",
                        "node_type_id": "Standard_F4s",
                        "num_workers": 2
                    },
                    "libraries": [],
                    "max_retries": 0,
                    "spark_python_task": {
                        "python_file": "path/to/entrypoint.py"
                    }
                }
            ]
        }
    }
    ```

1. Author code and create a job for interactive development. The run the following to deploy your project to databricks as a job: `dbx deploy --deployment-file conf/deployment.json --job dbx_job --no-rebuild`.   
    - Note the job name corresponds to the job within the `deployment.json` file.  


1. Execute the job with the below command. Note this does not execute the job created in the previous step, it interactively executes the job.     
    ```
    dbx execute --job dbx_job --cluster-name my_cluster --no-rebuild --no-package --deployment-file conf/deployment.json
    ```