from deltalake import DeltaTable 

path = 'test.delta'

dt = DeltaTable(path)

dt.to_pandas().head()