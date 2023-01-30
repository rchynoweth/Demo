# OSS PySpark and Delta Lake in ADLS Gen2

This directory demonstrates how to connect to and read Delta Lake tables stored in Azure Data Lake Storage Gen2. Please reference [Delta Lake Documentation](https://docs.delta.io/latest/delta-storage.html#azure-data-lake-storage-gen2) additional details.  

This shows how users are able to access their data without using Databricks compute.  

## OSS Spark Instructions 

1. Create a Python 3.9 Virtual Environment and install delta lake and pyspark
    ```
    conda create -n pyspark_env python=3.9

    conda activate pyspark_end

    pip install pyspark 
    pip install delta-spark 
    ```

1. Add secrets to config.conf (see example below). 
    ```
    storage_account_name=mystorageaccountname
    container_name=mycontainername
    client_id=myclientid
    client_secret=myclientsecret
    tenant_id=azuretenantid
    ```

1. Execute `run.py`. Please note that you will need to provide full path values for the CSV and Delta table. 
    - Line 48: `data_path = "abfss://{}@{}.dfs.core.windows.net/".format(container_name, storage_account_name)` 
    - Line 54: `delta_path = "abfss://{}@{}.dfs.core.windows.net/".format(container_name, storage_account_name)` 


## Delta Sharing 

Create a Delta Sharing Server using ADLS Gen2 and access that data using open source Apache Spark. 



Instructions 
1. Download the package from [GitHub](https://github.com/delta-io/delta-sharing/releases)
1. Unpack zip file and edit the `conf/delta-sharing-server.yaml` file. 
1. Provide the required details and add the following to the yaml file. 
    ```
    authorization:
    bearerToken: <token>
    ```
1. Add a `core-site.xml` file to add the account key required for delta sharing authentication with cloud storage.  
    ```
    <?xml version="1.0"?>
    <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
    <configuration>
    <property>
        <name>fs.azure.account.auth.type.YOUR-ACCOUNT-NAME.dfs.core.windows.net</name>
        <value>SharedKey</value>
        <description>
        </description>
    </property>
    <property>
        <name>fs.azure.account.key.YOUR-ACCOUNT-NAME.dfs.core.windows.net</name>
        <value>YOUR-ACCOUNT-KEY</value>
        <description>
        The secret password. Never share these.
        </description>
    </property>
    </configuration>
    ```
1. Start the server `bin/delta-sharing-server -- --config <the-server-config-yaml-file>`  
1. Run [`run_delta_share.py`](delta_demos/LocalSpark/run_delta_share.py).

Resources
- [Create a Delta Sharing Server](https://github.com/delta-io/delta-sharing#delta-sharing-reference-server)  
- [GitHub](https://github.com/delta-io/delta-sharing)
- [Web Page](https://delta.io/sharing)
- Pre-built Docker (not tested)
    ```
    docker run -p <host-port>:<container-port> \
    --mount type=bind,source=<the-server-config-yaml-file>,target=/config/delta-sharing-server-config.yaml \
    deltaio/delta-sharing-server:0.5.0 -- --config /config/delta-sharing-server-config.yaml
    ```