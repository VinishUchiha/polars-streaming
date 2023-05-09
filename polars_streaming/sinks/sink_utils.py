from .esWriter import ESWriter
from .mongoWriter import MongoWriter

def sinker(writer):
    if writer.source == 'mongo':
        mongoUrl = writer._options['uri']
        database = writer._options['database']
        collection = writer._options['collection']
        return MongoWriter(mongoUrl, database, collection)
    elif writer.source == 'elasticsearch':
        es_host = writer._options['host']
        es_index = writer._options['index']
        es_username = writer._options['username'] if 'username' in writer._options else None
        es_password = writer._options['password'] if 'password' in writer._options else None
        es_api_key = writer._options['api_key'] if 'api_key' in writer._options else None
        return ESWriter(host=es_host,index=es_index,username=es_username,password=es_password,api_key=es_api_key)