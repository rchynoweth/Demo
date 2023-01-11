# Executing Jobs via CLI/REST API 


Shared Auto Scaling America: 0812-164905-tear862 


## CLI   

CLI Python Task

Please reference this simple Python script. 
```
# remove file
databricks fs rm dbfs:/users/ryan.chynoweth@databricks.com/demo_env/simply_python.py

# copy file 
databricks fs cp simply_python.py dbfs:/users/ryan.chynoweth@databricks.com/demo_env/simply_python.py

# create job 
databricks jobs create --json-file ./simply_python.json

>> returns job id = 54369997116783 

# run the job 
databricks jobs run-now --job-id 277863935010607

```


CLI Notebook Task

Please reference this simple Python script. 
```
# create job 
databricks jobs create --json-file ./notebook_job.json

>> returns job id = 19712148800419 

# run the job 
databricks jobs run-now --job-id 19712148800419

```


AGENDA - 

1.      From the command line, create a scheduled job (using APIs) that references a python file located in S3 

a.       The job creation process should also specify parameters to pass to this python file (e.g., organization id) 

2.      Also: From the command line, create a scheduled job (using APIs) that references a notebook 

a.       The job creation process should also allow specifying parameters as inputs to the notebook (e.g., organization id) 

3.      The script or notebook should show how we would query the model registry and download the latest model for a particular organization 

4.      The script should demonstrate how we can be used to keep track of job statistics (e.g., number of items determined to be of one class or another or how well the model training performs on the training/validation sets) 

5.      Demonstration of how we would edit the job either via the Databricks platform or via APIs. 


 


Bonus:
How would we use the feature registry to keep track of feature transform models (and is that the right use case for it)? 