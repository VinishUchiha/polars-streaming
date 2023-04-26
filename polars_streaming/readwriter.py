import polars as pl
from glob import glob
import os

FILE_SOURCES = ['csv','parquet','json','avro']

READERS = {'csv': pl.read_csv, 'parquet': pl.read_parquet, 'json': pl.read_json, 'avro': pl.read_avro}

class DataStreamReader():

    def __init__(self):
        self._options = {}

    def format(self, source):
        self.source = source
        return self

    def option(self, key: str, value):
        self._options[key] = value
        return self

    def options(self, options):
        for key, value in options.items():
            self._options[key] = value
        return self

    def schema(self, schema):
        self.schema = schema

    def load(self, path: str = None, preFetchFirstBatch: str = False):
        self.file_source_path = path
        if preFetchFirstBatch:
            if self.source in FILE_SOURCES:
                if self.file_source_path is not None:
                    files = glob(f"{self.file_source_path.rstrip('/')}/*.{self.source}")
                    files.sort(key=os.path.getmtime)
                    self.df = READERS[self.source](files[0])
        return self
        
class DataStreamWriter():
    def __init__(self):
        self._options = {}

    def format(self, source):
        self.source = source
        return self

    def option(self, key, value):
        self._options[key] = value
        return self
    
    def options(self, options):
        for key, value in options.items():
            self._options[key] = value
        return self
    
    def outputMode(self, mode):
        self.output_mode = mode
        return self

    def trigger(self, ProcessingTime="1 second"):
        self.processing_time = ProcessingTime
        return self
