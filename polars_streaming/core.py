from fileProcessor import FileProcessor
from readwriter import DataStreamReader, DataStreamWriter
from kafkaProcessor import KafkaProcessor
from exceptions import NotImplementedError

FILE_SOURCES = ['csv','parquet','json','avro']

class StreamProcessor():

    @property
    def readStream(self) -> DataStreamReader:
        self.reader = DataStreamReader()
        return self.reader

    @property
    def writeStream(self) -> DataStreamWriter:
        self.writer = DataStreamWriter()
        return self.writer
    
    def preFetchedDF(self):
        return self.reader.df

    def add_transform(self, transforms):
        self.transform = transforms

    def start(self):
        if self.reader.source in FILE_SOURCES:
            FileProcessor(self.reader, self.transform, self.writer).start()
        elif self.reader.source == 'kafka':
            KafkaProcessor(self.reader, self.transform, self.writer).start()
        else:
            raise NotImplementedError(f'Not Implemented ReadStream Source: {self.reader.source}')
