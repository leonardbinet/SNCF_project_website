from datetime import datetime, timedelta
from pymongo import MongoClient
import pymongo
from navitia_client import Client

try:
    # Python 3.x
    from urllib.parse import quote_plus
except ImportError:
    # Python 2.x
    from urllib import quote_plus


def connect_mongoclient(host, user=None, password=None, port=None, database=None, max_delay=15000):
    # Build URI
    uri = "mongodb://"
    if user and password:
        uri += "%s:%s@" % (quote_plus(user), quote_plus(password))
    uri += host
    if port:
        uri += str(port)
    if database:
        uri += "/%s" % quote_plus(database)
    client = MongoClient(uri, serverSelectionTimeoutMS=max_delay)
    return client


def check_mongo_connection(host, user=None, password=None, port=None, database=None, max_delay=500):

    status = False
    client = connect_mongoclient(host, user=user, password=password,
                                 port=port, database=database, max_delay=max_delay)
    try:
        server_info = client.server_info()
        database_names = client.database_names()
        # print(server_info)
        status = True
        add_info = {
            "server_info": server_info,
            "database_names": database_names,
            "databases_stats": get_databases_stats(client)
        }
        print("MongoDB connection OK")
    except pymongo.errors.ServerSelectionTimeoutError as err:
        # Status stays False
        add_info = None
        print(err)
    return status, add_info


def get_databases_stats(client):
    result = []
    database_names = client.database_names()
    for database_name in database_names:
        database_result = []
        collection_names = client[database_name].collection_names()
        for collection_name in collection_names:
            database_result.append(
                {
                    "collection": collection_name,
                    "count": client[database_name][collection_name].count()
                })
        added_object = {
            "database": database_name,
            "collections": database_result
        }
        result.append(added_object)

    return result
