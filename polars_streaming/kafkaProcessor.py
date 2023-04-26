import polars as pl
import schedule
from utils import _writer, SUPPORTED_FILE_WRITERS, trigger_time_extracter
from exceptions import ModuleNotFoundException

options_mapper = {'kafka.bootstrap.servers':'bootstrap.servers',
                #'subscribe': 'subscribe',
                #'includeHeaders': 'includeHeaders',
                'startingOffsets': 'auto.offset.reset',
                #'endingOffsets': 'endingOffsets',
                #'subscribePattern': 'subscribePattern',
                'kafka.group.id': 'group.id'}

class KafkaProcessor():
    def __init__(self, reader, transform, writer):
        try:
            import confluent_kafka
        except ImportError:
            raise ModuleNotFoundException("confluent_kafka not installed. try `pip install confluent-kafka`.")
        self.reader = reader
        self.writer = writer
        self.transform = transform

    def _kafka_option_mapper(self):
        options = self.reader._options
        kafka_config = {}
        for key, value in options.items():
            if key in options_mapper:
                kafka_config[options_mapper[key]] = value
        self.kafka_config = kafka_config

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
        self.batch_count+=1
        self.mini_batch.clear()

    def start(self):
        from confluent_kafka import Consumer, KafkaException
        self._kafka_option_mapper()
        consumer = Consumer(self.kafka_config)
        topics = self.reader._options['subscribe'].split(',')
        consumer.subscribe(topics)
        self.mini_batch = []
        ptime = trigger_time_extracter(self.writer.processing_time)
        schedule.every(ptime).seconds.do(self.apply_transform, transformation = self.transform)
        self.batch_count = 0
        try:
            while True:
                event = consumer.poll(1.0)
                if event is None:
                    continue
                if event.error():
                    raise KafkaException(event.error())
                else:
                    val = event.value().decode('utf8')
                    consumer.commit(event)
                self.mini_batch.append(val)
                schedule.run_pending()
        except KeyboardInterrupt:
            print('Canceled by user.')
        finally:
            consumer.close()