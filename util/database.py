from pymongo import MongoClient
import redis


dev_server = 'localhost'
prod_server = '159.203.170.25'


def get_mg_client():
    return MongoClient(host=prod_server)


def get_redis_conn():
    return redis.Redis(host=prod_server)
