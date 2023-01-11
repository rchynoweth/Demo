from flask import Flask, render_template, request
from databricks import sql 
import os
from config_parser import ConfigParser

app = Flask(__name__)


cfg = ConfigParser()
server_hostname = cfg.server_hostname
http_path = cfg.http_path
access_token = cfg.access_token

@app.route('/')
def index():
    schemas = get_schemas()
    query_text = request.form.get('query_text', "")
    query_results = query_db_sql(query_text) if query_text != "" else []

    return render_template('index.html', schemas=schemas, query_headers=['No Results'], query_results=query_results)


@app.route("/", methods=['POST'])
def execute_query():
    schemas = get_schemas()
    
    query_text = request.form.get('query_text', "")
    query_results = query_db_sql(query_text) if query_text != "" else []
    
    query_headers = list(query_results[0].asDict().keys())
    
    return render_template('index.html', schemas=schemas, query_results=query_results, query_headers=query_headers)



def query_db_sql(query):
    with sql.connect(server_hostname=server_hostname, 
                    http_path=http_path, 
                    access_token=access_token) as connection:
        
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()

    return result


def get_schemas():
    return query_db_sql("SHOW SCHEMAS")


if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)
