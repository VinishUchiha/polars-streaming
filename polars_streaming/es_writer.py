from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

class ElasticSearch():
    def __init__(self, host: str, username: str =None, password: str =None, api_key: str = None) -> None:
        if username is not None and password is not None:
            self.es = ElasticSearch([host], basic_auth=(username, password))
        elif api_key is not None:
            self.es = ElasticSearch([host], api_key=api_key)
        else:
            self.es = ElasticSearch([host])

    def write_dataframe(self, df, index):
        records = df.to_dict()
        actions = [
            {
                "_index": index,
                "_source": doc
            }
            for doc in records
        ]
        bulk(self.es, actions)