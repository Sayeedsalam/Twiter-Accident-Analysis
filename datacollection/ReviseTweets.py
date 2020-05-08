import time

import schedule
import tweepy
from elasticsearch import Elasticsearch
from pymongo import MongoClient

accessToken = "3939721633-EJIfdOWAcuWVjwFpsQp7Qs3P5NNyh3q7ihpC9ld"
accessToken_Secret = "wyQ9uA8vZ02Ni2fimTJegOSdIJTO95ttIJWWkDZUxl5cZ"
apiKey = "8VQzfyd9REr8gO2nesBRkJTru"
apiKey_Secret = "2CS4iYIdBq6SNSmTJCWUFJuhOjAkN4b68bXkOIS7nYouuh7r8n"

db = MongoClient().tweets

def authenticate():  # authenticates with tweepy
    authenticator = tweepy.OAuthHandler(apiKey, apiKey_Secret)
    API = tweepy.API(authenticator)
    return API

cursor = db.covid.find({}, no_cursor_timeout=True)

log_file = open("log_id.txt", "w+")

for entry in cursor:

    id = entry["id_str"]
    text = entry["text"]
    try:
        a = authenticate().get_status(int(id), tweet_mode="extended")

        if 'retweeted_status' in a._json:
            text = a._json['retweeted_status']['full_text']
        else:
            text = a.full_text
    except:
        log_file.write(id+"\n")


    entry["text"] = text
    print(text)
    db.covid_ext.insert(entry)
    time.sleep(1)

log_file.close()