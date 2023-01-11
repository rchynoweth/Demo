# Unity Catalog Examples 



## Information Schema 

Please reference `uc_information_schema.py` for code examples and the [associated medium blog](https://medium.com/@24chynoweth/unity-catalog-the-information-schema-1d162ede3737). 

The INFORMATION_SCHEMA is a SQL Standard based, system provided schema present in every catalog other than the HIVE_METASTORE catalog.

Within the information schema, you can find a set of views describing the objects known to the schema’s catalog that you are privileged the see. The information schema of the SYSTEM catalog returns information about objects across all catalogs within the metastore.

The purpose of the information schema is to provide a SQL based, self describing API to the metadata.


Below is a sample query than can be used to generate a network graph of the table relationships: 
```sql
%sql
CREATE OR REPLACE VIEW rac_demo_catalog.rac_demo_db.table_relationships_view
AS 
SELECT DISTINCT ptc.table_catalog as p_table_catalog -- table 1
  , ptc.table_schema as p_table_schema
  , ptc.table_name as p_table_name
  , stc.table_catalog as s_table_catalog -- table 2
  , stc.table_schema as s_table_schema
  , stc.table_name as s_table_name
  , rc.constraint_name -- foreign key
  , pccu.column_name as constraint_column_name
  , rc.unique_constraint_name -- primary key
  , sccu.column_name as unique_constraint_column_name

FROM referential_constraints rc 
INNER JOIN table_constraints ptc on ptc.constraint_name =  rc.constraint_name 
INNER JOIN table_constraints stc on stc.constraint_name =  rc.unique_constraint_name 
INNER JOIN constraint_column_usage pccu on pccu.constraint_name = rc.constraint_name 
INNER JOIN constraint_column_usage sccu on sccu.constraint_name = rc.unique_constraint_name 

-- WHERE rc.constraint_catalog = 'rac_demo_catalog'
;

SELECT * FROM rac_demo_catalog.rac_demo_db.table_relationships_view;
```
<br></br>
Visual:
<br></br>
<img src="https://racadlsgen2.blob.core.windows.net/public/TableNetwork.png" width=500 />


