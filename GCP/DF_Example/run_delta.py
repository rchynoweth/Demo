import argparse 
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.parquetio import WriteToParquet

import pyarrow as pa
from deltalake import DeltaTable 

from GCP.DF_Example.deltaio import WriteToDeltaTable, ArrowTableToRowDictionaries





def run(argv=None, save_main_session=True):
    output_dir = "GCP"

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://racgcs/ext_products_non_uniform',
        help='Input table to read.')
    
    parser.add_argument(
        '--output',
        dest='output',
        default='gs://racgcs/deltaoutput',
        help='Output file to write results to.')

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    # read delta table 
    dt = DeltaTable(known_args.input)
    arrow_dt = dt.to_pyarrow_table()#.flatten()


    # data_schema = pa.schema([
    #             ('product_id', pa.string()),  # Define the schema of the Parquet file
    #             ('product_category', pa.string()),
    #             ('product_name', pa.string()),
    #             ('sales_price', pa.string()),
    #             ('EAN13', pa.string()),
    #             ('EAN5', pa.string()),
    #             ('product_unit', pa.string())
    #         ])

    delta_schema = dt.schema() # delta schema
    arrow_schema = arrow_dt.schema # arrow schema


    # Create an Apache Beam pipeline
    with beam.Pipeline() as pipeline:
        # Convert the PyArrow table to a PCollection using the custom transform
        data = (
            pipeline
            | "Convert to PCollection" >> beam.Create([arrow_dt])
            | "Convert to Row Dictionaries" >> beam.ParDo(ArrowTableToRowDictionaries())
        )


        data | WriteToParquet(
            file_path_prefix=output_dir,
            schema=arrow_schema,
            file_name_suffix=".parquet", 
            num_shards=1  
        )


        data | WriteToDeltaTable(
            table_path='./test.delta',
            write_mode='append',
            schema=arrow_schema
            )

        
    

if __name__ == '__main__':
    run()