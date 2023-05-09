import polars as pl
import socket
import schedule
from .utils import _writer, SUPPORTED_FILE_WRITERS, trigger_time_extracter
from ..sinks import sink_utils

class SocketProcessor():
    def __init__(self, reader, transform, writer):
        self.reader = reader
        self.writer = writer
        self.transform = transform

    def apply_transform(self, transformation):
        df = pl.from_records(self.mini_batch)
        df = transformation(df.lazy())
        if self.writer.source=='console':
            print(f"Batch: {self.batch_count}")
            print(df.collect())
        elif self.writer.source in SUPPORTED_FILE_WRITERS:
            path = self.writer._options['path'].rstrip('/')
            filename = f"batch_{self.batch_count}"
            fullpath = f"{path}/{filename}.{self.writer.source}"
            _writer(df.collect(), fullpath, self.writer.source)
        elif self.writer.source in ['mongo','elasticsearch']:
            self.sink.write_dataframe(df.collect())
        self.batch_count+=1
        self.mini_batch.clear()

    def start(self):
        host = self.reader._options['host']
        port = self.reader._options['port']
        if 'bufferSize' in self.reader._options:
            buffer = self.reader._options['bufferSize']
        else:
            buffer = 1024
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, port))
        if self.writer.source in ['mongo','elasticsearch']:
            self.sink = sink_utils.sinker(self.writer)
        self.mini_batch = []
        ptime = trigger_time_extracter(self.writer.processing_time)
        schedule.every(ptime).seconds.do(self.apply_transform, transformation = self.transform)
        self.batch_count = 0
        try:
            while True:
                msg = s.recv(buffer)
                msg = msg.decode("utf-8")
                self.mini_batch.append(msg)
                schedule.run_pending()
        except KeyboardInterrupt:
            print('Canceled by user.')
        finally:
            s.close()