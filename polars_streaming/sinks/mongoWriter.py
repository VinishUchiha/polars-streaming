from ..exceptions import ModuleNotFoundException

class MongoWriter():
    def __init__(self, conn_str: str, database: str, collection: str) -> None:
        try:
            from pymongo import MongoClient
        except ImportError:
            raise ModuleNotFoundException("pymongo not installed. try `pip install pymongo`.")
        self.mdb = MongoClient(conn_str)
        self.collection = collection
        self.database = database

    def write_dataframe(self, df):
        records = df.to_dict()
        self.mdb[self.database][self.collection].insert_many(records)
        