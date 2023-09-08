# Dataflow Sample

Dataflow Templates are great. But they are point A to point B "streaming" solutions with a high priority on getting data into BigQuery for more data processing and analytics. Dataflow should be considered a data integration tool that ingests data into a batch system (BQ) with little support for ML/AI. In this example, we go beyond the templates and develop a custom job that read/writes data to an from Delta Lake. 



## Setup

To set up a development environment, we will create a local Python virtual environment running the following commands. 

1. Create Environment 
    ```
    conda create -n dataflow python=3.9

    conda activate dataflow
    ```

1. [Install the GCP SDK](https://cloud.google.com/sdk/docs/install)
    ```
    # install 
    ./google-cloud-sdk/install.sh

    # auth
    ./google-cloud-sdk/bin/gcloud init
    ```

1. [Install](https://cloud.google.com/sdk/docs/install) and Configure the gcloud CLI then run `gcloud init`. 

1. Might need to run `gcloud auth application-default login` for interactive login. 

1. Run `pip install 'apache-beam[gcp]'`.

1. Run the sample locally with the following from the repository root to test:
    ```
    python -m apache_beam.examples.wordcount --output ./GCP/DF_Example/outputs
    ```

1. Run the sample code remotely with the following from the repository root. 
    ```
    python -m apache_beam.examples.wordcount \
        --region us-west1 \
        --input gs://dataflow-samples/shakespeare/kinglear.txt \
        --output gs://racgcs/results/outputs \
        --runner DataflowRunner \
        --project fe-dev-sandbox \
        --temp_location gs://racgcs/tmp/ 
    ```



1. Great! It works with an out of the box sample, but what if I have custom code on my desktop. You can run the following to execute the code locally.  
    ```
    python -m GCP.DF_Example.run_delta 
    ```

1. To run the service on Dataflow we will need to package the deltaio file as a tarball. To do so run the following command from the `DF_Example` directory. 
    ```
    python setup.py sdist
    ```

1. Run the local code on the Dataflow Service using the following command in the repository root. 
    ```
    python -m GCP.DF_Example.run_delta \
        --region us-west1 \
        --runner DataflowRunner \
        --project fe-dev-sandbox \
        --temp_location gs://racgcs/tmp/ \
        --requirements_file ./GCP/DF_Example/requirements.txt \
        --extra_package ./GCP/DF_Example/dist/deltaio-0.1.tar.gz
    ```


Please see below for the GCP Dataflow Job that Reads and Writes a Delta Lake Table
![](./imgs/DeltaLakeJobRun.png)






### Misc Resources 
- [Python Demo](https://medium.com/google-cloud/understanding-the-dataflow-quickstart-for-python-tutorial-e134f39564c7)
- [Python Quickstart](https://cloud.google.com/dataflow/docs/quickstarts/create-pipeline-python)  
- [SDK](https://cloud.google.com/sdk/docs/install)  
- [YouTube Video](https://www.youtube.com/watch?v=J-b2Eo5Qvp8)
- [Beam Datalake](https://github.com/nanhu-lab/beam-datalake)
- [Delta Lake Documentation](https://delta-io.github.io/delta-rs/python/usage.html#querying-delta-tables)
- [ParquetIO](https://beam.apache.org/releases/pydoc/2.45.0/_modules/apache_beam/io/parquetio.html)
- [BigQueryIO](https://beam.apache.org/releases/pydoc/2.45.0/_modules/apache_beam/io/gcp/bigtableio.html)
- [Creating Jobs from Notebook](https://cloud.google.com/dataflow/docs/guides/interactive-pipeline-development#launch-jobs-from-pipeline)
- [Jobs with Local Dependencies](https://stackoverflow.com/questions/46604870/apache-beam-local-python-dependencies)
- [Beam Local Dependencies](https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/)
- [Change Data Feed](https://docs.delta.io/2.0.0/delta-change-data-feed.html)
- [Delta Streaming](https://delta-io.github.io/delta-rs/python/api_reference.html?highlight=open_output_stream#module-deltalake.fs)
- [Table Performance Discussion](https://github.com/delta-io/delta-rs/issues/1569)

