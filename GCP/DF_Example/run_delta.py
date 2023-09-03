import argparse 
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.parquetio import WriteToParquet

from GCP.DF_Example.deltaio import WriteToDeltaTable, ArrowTableToRowDictionaries, DeltaTableReader



def run(argv=None, save_main_session=True):
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


    # Create an Apache Beam pipeline
    with beam.Pipeline() as pipeline:
        # read delta table 
        delta_reader = DeltaTableReader()
        arrow_dt, arrow_schema = delta_reader.read_table(known_args.input)
        
        # Convert the PyArrow table to a PCollection using the custom transform
        data = (
            pipeline
            | "Convert to PCollection" >> beam.Create([arrow_dt])
            | "Convert to Row Dictionaries" >> beam.ParDo(ArrowTableToRowDictionaries())
        )


        # data | WriteToParquet(
        #     file_path_prefix=output_dir,
        #     schema=arrow_schema,
        #     file_name_suffix=".parquet", 
        #     num_shards=1  
        # )


        data | WriteToDeltaTable(
            # table_path='./test.delta',
            table_path=known_args.output,
            write_mode='append',
            schema=arrow_schema
            )



if __name__ == '__main__':
    run()