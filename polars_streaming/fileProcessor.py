from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer
from exceptions import FileSourcePathMissingException, FileExtensionNotSupportedException
import polars as pl
import time
from pathlib import Path
from utils import _writer, READERS, SUPPORTED_FILE_WRITERS

class FileHandler(PatternMatchingEventHandler):
    def __init__(self, reader, transform, writer) -> None:
        self.reader = reader
        self.transform = transform
        self.writer = writer
        PatternMatchingEventHandler.__init__(self, patterns=[f'*.{self.reader.source}'], ignore_directories=True, case_sensitive=False)

    def on_created(self, event):
        #print(f"New {self.source} file created", event.src_path)
        try:
            df = READERS[self.reader.source](event.src_path)
            df = self.transform(df.lazy())
            if self.writer.source=='console':
                print(df.collect())
            elif self.writer.source in SUPPORTED_FILE_WRITERS:
                path = self.writer._options['path'].rstrip('/')
                filename = Path(event.src_path).stem
                fullpath = f"{path}/{filename}.{self.writer.source}"
                _writer(df.collect(), fullpath, self.writer.source)
        except Exception as e:
            print(e)

class FileProcessor():
    def __init__(self, reader, transform, writer) -> None:
        self.reader = reader
        self.transform = transform
        self.writer = writer

    def start(self):
        event_handler = FileHandler(self.reader, self.transform, self.writer)
        observer = Observer()
        if self.reader.file_source_path:
            path = self.reader.file_source_path
        elif 'path' in self.reader._options:
            path = self.reader._options['path']
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