import operator
from operator import itemgetter

from pymongo import MongoClient

from elasticsearch import Elasticsearch
import json

es = Elasticsearch(hosts=["149.165.157.107"])

doc = {
        'size' : 10000,
        'query': {
            'match_all' : {}
       }
   }

res = es.search(index='covid_tweets', body=doc)

file = open("covid_tweets_en.json", "w+")

annotation_entry = []

entry_es = res['hits']['hits']


#entry_es = entry_es.sort(key=operator.itemgetter('cluster_id'))

for ent in entry_es:
    anno_entry = {}
    hit = ent["_source"]

    anno_entry["tweet_id"] = hit["id_str"]
    anno_entry["text"] = hit["text"]
    anno_entry["user_location"] = hit["user"]["location"]
    anno_entry["cluster_id"] = hit["cluster_id"]

    anno_entry["annotaion"]= {}

    anno_entry["annotaion"]["is_informative"] = "True|False"
    anno_entry["annotaion"]["info_type"] = "Status|Prevention"
    anno_entry["annotaion"]["key_info"] = []


    annotation_entry.append(anno_entry)



json.dump(annotation_entry, file)

file.close()





