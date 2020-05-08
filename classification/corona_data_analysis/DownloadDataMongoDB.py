import operator
from operator import itemgetter

from pymongo import MongoClient

from elasticsearch import Elasticsearch
import json


db = MongoClient().tweets

file = open("covid_tweets_es.json", "w+")

annotation_entry = []



#entry_es = entry_es.sort(key=operator.itemgetter('cluster_id'))
count = 300

for ent in db.covid_ext_es.find({}):
    anno_entry = {}
    hit = ent
    if hit["created_at"] < "2020-04-03":
        continue

    anno_entry["tweet_id"] = hit["id_str"]
    #anno_entry["county"] = hit["county"]
    anno_entry["created_at"] = hit["created_at"]
    anno_entry["text"] = hit["text"]
    anno_entry["user_location"] = hit["user"]["location"]
    #anno_entry["cluster_id"] = hit["cluster_id"]

    anno_entry["annotaion"]= {}

    anno_entry["annotaion"]["informative"] = "True|False"
    anno_entry["annotaion"]["type"] = "status|prevention"
    anno_entry["annotaion"]["key_info"] = []


    annotation_entry.append(anno_entry)
    if count == 0:
        break
    count -= 1


json.dump(annotation_entry, file)

file.close()





