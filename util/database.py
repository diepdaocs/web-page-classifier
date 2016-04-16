from pymongo import MongoClient


def get_mg_client():
    return MongoClient(host='159.203.170.25')
