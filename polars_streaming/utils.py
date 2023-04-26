from exceptions import FileExtensionNotSupportedException
import polars as pl

READERS = {'csv': pl.read_csv, 'parquet': pl.read_parquet, 'json': pl.read_json, 'avro': pl.read_avro}
SUPPORTED_FILE_WRITERS = ['csv','parquet','json','avro']

def _writer(df, path, extension):
    if extension == 'csv':
        df.write_csv(path)
    elif extension == 'parquet':
        df.write_parquet(path)
    elif extension == 'json':
        df.write_json(path)
    elif extension == 'avro':
        df.write_avro(path)
    else:
        raise FileExtensionNotSupportedException(f'UnSupported File Extension: {extension}')
    
def trigger_time_extracter(time_str):
    t, unit = time_str.split()
    if unit in ['seconds', 'second', 'sec', 'secs']:
        return int(t)
    elif unit in ['minutes', 'minute', 'min', 'mins']:
        return int(t)*60
    elif unit in ['hours', 'hour', 'hr', 'hrs']:
        return int(t)*60*60
