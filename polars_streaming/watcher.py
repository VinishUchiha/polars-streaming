from watchdog.events import PatternMatchingEventHandler
import polars as pl

READERS = {'csv': pl.read_csv, 'parquet': pl.read_parquet, 'json': pl.read_json, 'avro': pl.read_avro}

class Handler(PatternMatchingEventHandler):
    def __init__(self, source, transform, output_mode) -> None:
        self.source = source
        self.transform = transform
        self.output_mode = output_mode
        PatternMatchingEventHandler.__init__(self, patterns=[f'*.{source}'], ignore_directories=True, case_sensitive=False)

    def on_created(self, event):
        print(f"New {self.source} file created", event.src_path)
        try:
            df = READERS[self.source](event.src_path)
            df = self.transform(df.lazy())
            if self.output_mode=='console':
                print(df)
        except Exception as e:
            print(e)