from ..exceptions import ModuleNotFoundException

class ESWriter():
    def __init__(self, host: str, index: str, username: str =None, password: str =None, api_key: str = None) -> None:
        try:
            from elasticsearch import Elasticsearch
        except ImportError:
            raise ModuleNotFoundException("elasticsearch not installed. try `pip install elasticsearch`.")
        if username is not None and password is not None:
            self.es = Elasticsearch([host], basic_auth=(username, password))
        elif api_key is not None:
            self.es = Elasticsearch([host], api_key=api_key)
        else:
            self.es = Elasticsearch([host])
        self.index = index

    def write_dataframe(self, df):
        from elasticsearch.helpers import bulk
        records = df.to_dicts()
        actions = [
            {
                "_index": self.index,
                "_source": doc
            }
            for doc in records
        ]
        bulk(self.es, actions)