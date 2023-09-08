from deltalake import DeltaTable , write_deltalake
from deltalake.fs import DeltaStorageHandler
from deltalake import write_deltalake

input_path = "gs://racgcs/ext_products_non_uniform"
output_path = 'gs://racgcs/deltaoutput'


output_df = DeltaTable(output_path).to_pandas()
output_base_len = len(output_df)
output_base_len

input_pdf = DeltaTable(input_path).to_pandas()
input_len = len(input_pdf)
input_len

### Run pipeline 
## check rows were appended appropriately. 

new_output_table = DeltaTable(output_path).to_pandas()
assert len(new_output_table) == (output_base_len+input_len)



### Streaming 
# https://delta-io.github.io/delta-rs/python/api_reference.html?highlight=open_output_stream#deltalake.fs.DeltaStorageHandler
dsh = DeltaStorageHandler("gs://racgcs/ext_products_non_uniform")
## I think I need to provide authentication for this - https://arrow.apache.org/docs/python/generated/pyarrow.fs.FileSystemHandler.html#pyarrow.fs.FileSystemHandler.open_input_stream
# it is not inherited from my cli

dsh = DeltaStorageHandler("./")
dsh.open_input_stream('test.delta/')


write_deltalake('test.delta', input_pdf)