import pyarrow as pa
from deltalake import DeltaTable 
from deltalake.writer import write_deltalake

from apache_beam.transforms import DoFn, PTransform, window, ParDo




# Stolen from parquet reader
class ArrowTableToRowDictionaries(DoFn):
    """ A DoFn that consumes an Arrow table and yields a python dictionary for
    each row in the table."""
    def process(self, table, with_filename=False):
        if with_filename:
            file_name = table[0]
            table = table[1]
        num_rows = table.num_rows
        data_items = table.to_pydict().items()
        for n in range(num_rows):
            row = {}
            for column, values in data_items:
                row[column] = values[n]
            if with_filename:
                yield (file_name, row)
            else:
                yield row





class ReadDeltaTableAsArrow():

    def read_table(self, path):
        # read delta table 
        dt = DeltaTable(path)
        arrow_dt = dt.to_pyarrow_table()
    
        return arrow_dt
    



class RowDictionariesToArrowTable(DoFn):
    """ A DoFn that consumes python dictionarys and yields a pyarrow table."""
    def __init__(self, schema, row_group_buffer_size=64 * 1024 * 1024, record_batch_size=1000):
        self._schema = schema
        self._row_group_buffer_size = row_group_buffer_size
        self._buffer = [[] for _ in range(len(schema.names))]
        self._buffer_size = record_batch_size
        self._record_batches = []
        self._record_batches_byte_size = 0

    def process(self, row):
        if len(self._buffer[0]) >= self._buffer_size:
            self._flush_buffer()

        if self._record_batches_byte_size >= self._row_group_buffer_size:
            table = self._create_table()
            yield table

        # reorder the data in columnar format.
        for i, n in enumerate(self._schema.names):
            self._buffer[i].append(row[n])

    def finish_bundle(self):
        if len(self._buffer[0]) > 0:
            self._flush_buffer()
        if self._record_batches_byte_size > 0:
            table = self._create_table()
            yield window.GlobalWindows.windowed_value_at_end_of_window(table)


    def _create_table(self):
        table = pa.Table.from_batches(self._record_batches, schema=self._schema)
        self._record_batches = []
        self._record_batches_byte_size = 0
        return table

    def _flush_buffer(self):
        arrays = [[] for _ in range(len(self._schema.names))]
        for x, y in enumerate(self._buffer):
            arrays[x] = pa.array(y, type=self._schema.types[x])
            self._buffer[x] = []
        rb = pa.RecordBatch.from_arrays(arrays, schema=self._schema)
        self._record_batches.append(rb)
        size = 0
        for x in arrays:
            for b in x.buffers():
                if b is not None:
                    size = size + b.size
        self._record_batches_byte_size = self._record_batches_byte_size + size




class DeltaWriteFn(DoFn):

    def __init__(self, table_path, write_mode, schema):
        super().__init__()
        self.beam_options = {'table_path': table_path, 'write_mode': write_mode, 'schema':schema}

    def __getstate__(self):
        return self.beam_options

    def __setstate__(self, options):
        self.beam_options = options


    def process(self, data):


        write_deltalake(
            self.beam_options.get('table_path'), 
            data, 
            mode=self.beam_options.get('write_mode'),
            schema=self.beam_options.get('schema')
            )



class WriteToDeltaTable(PTransform):
    def __init__(self, table_path, write_mode, schema):
        super().__init__()
        self.beam_options = {'table_path': table_path, 'write_mode': write_mode, 'schema':schema}

    def expand(self, pvalue):
        return (
            pvalue
            | ParDo(
                RowDictionariesToArrowTable(schema=self.beam_options.get('schema'))
            )
            | ParDo(
                DeltaWriteFn(
                    self.beam_options.get('table_path'),
                    self.beam_options.get('write_mode'),
                    self.beam_options.get('schema')
                    )
                )
            )
