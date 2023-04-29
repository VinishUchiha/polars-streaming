from .exceptions import ModuleNotFoundException

class ElasticSearch():
    def __init__(self, host: str, index: str, username: str =None, password: str =None, api_key: str = None) -> None:
        try:
            from elasticsearch import Elasticsearch
        except ImportError:
            raise ModuleNotFoundException("elasticsearch not installed. try `pip install elasticsearch`.")
        if username is not None and password is not None:
            self.es = ElasticSearch([host], basic_auth=(username, password))
        elif api_key is not None:
            self.es = ElasticSearch([host], api_key=api_key)
        else:
            self.es = ElasticSearch([host])
        self.index = index

    def write_dataframe(self, df, index):
        from elasticsearch.helpers import bulk
        records = df.to_dict()
        actions = [
            {
                "_index": self.index,
                "_source": doc
            }
            for doc in records
        ]
        bulk(self.es, actions)