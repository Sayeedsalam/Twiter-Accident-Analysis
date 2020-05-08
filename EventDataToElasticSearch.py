from pymongo import MongoClient
from elasticsearch import Elasticsearch
es = Elasticsearch(hosts=["http://149.165.157.107/"])

def __get_mongo_connection():
    # For local debugging
    MONGO_SERVER_IP = "172.29.100.22"
    MONGO_PORT = "3154"
    MONGO_USER = "event_reader"
    MONGO_PSWD = "dml2016"
    NUM_ARTICLES = 1000

    #password = urllib.quote_plus(MONGO_PSWD)
    return MongoClient('mongodb://' + MONGO_USER + ':' + MONGO_PSWD + '@' + MONGO_SERVER_IP + ":" + MONGO_PORT)
    #return MongoClient(host="127.0.0.1")


def send_to_es(data, id):
    es.index(index="phoenix_event", id=id, doc_type="Message", body=data,
             request_timeout=30)




db = __get_mongo_connection().event_scrape
count = 0
cursor = db.phoenix_events.find({"date8":{"$gt": "20191220"}}, no_cursor_timeout=True)
for entry in cursor:
    id = str(entry.pop("_id"))
    print(entry)
    send_to_es(entry, id)
    count += 1


    print(count)

cursor.close()

