from google.cloud import bigquery


class BQConnect():

  def __init__(self, project):
    self.project = project 
    self.client = bigquery.Client( project=self.project)


  def execute_query(self, sql):
    query_job = self.client.query( sql )
    results = query_job.result() 
    return results
  
  def get_latest_iceberg_metadata(self, table_location):
    return [f.path for f in dbutils.fs.ls(table_location+'/metadata') if '.json' in f.path][-1]


  def format_table_update_query(self, dataset, table, metadata_path):
    sql = f"""
          CREATE OR REPLACE EXTERNAL TABLE rac_dataset.ext_products2
          OPTIONS (
              format = 'ICEBERG',
              uris = ["{metadata_path}"]
          );
          """
    return sql