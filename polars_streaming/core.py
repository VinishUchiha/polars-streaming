import polars as pl
from exceptions import FileSourcePathMissingException
from watcher import Handler
from watchdog.observers import Observer
import time
from readwriter import DataStreamReader, DataStreamWriter

file_sources = ['csv','parquet','json','avro']

class StreamProcessor():

    @property
    def readStream(self) -> DataStreamReader:
        self.reader = DataStreamReader()
        return self.reader

    @property
    def writeStream(self) -> DataStreamWriter:
        self.writer = DataStreamWriter()
        return self.writer

    def add_transform(self, transforms):
        self.transform = transforms

    def start(self):
        if self.reader.source in file_sources:
            event_handler = Handler(self.reader.source, self.transform, self.writer.output_mode)
            observer = Observer()
            if self.reader.file_source_path:
                path = self.reader.file_source_path
            elif 'path' in self.options:
                path = self.options['path']
            else:
                raise FileSourcePathMissingException("file source path not mentioned")
            observer.schedule(event_handler,path, recursive=True)
            observer.start()
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                observer.stop()
                observer.join()
