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


def check_mongo_connection(host, user=None, password=None, port=None, database=None, max_delay=500):

    status = False

    # Build URI
    uri = "mongodb://"
    if user and password:
        uri += "%s:%s@" % (quote_plus(user), quote_plus(password))
    uri += host
    if port:
        uri += str(port)
    if database:
        uri += "/%s" % quote_plus(database)

    client = MongoClient(uri)
    try:
        client = pymongo.MongoClient(uri,
                                     serverSelectionTimeoutMS=max_delay)
        server_info = client.server_info()
        database_names = client.database_names()
        print(server_info)
        status = True
        add_info = {
            "server_info": server_info,
            "database_names": database_names
        }
        print("MongoDB connection OK")
    except pymongo.errors.ServerSelectionTimeoutError as err:
        # Status stays False
        add_info = None
        print(err)
    return status, add_info
