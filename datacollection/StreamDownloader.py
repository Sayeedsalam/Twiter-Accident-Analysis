import time

import tweepy
from pymongo import MongoClient

class CovidStreamListner(tweepy.StreamListener):

    def __init__(self):
        self.mongo_db = MongoClient().tweets

    def on_status(self, status):
        print("Hello")
        self.mongo_db.covid_stream.insert(status._json)


    def __reformat(self, status):
        pass

apiKey = "8VQzfyd9REr8gO2nesBRkJTru"
apiKey_Secret = "2CS4iYIdBq6SNSmTJCWUFJuhOjAkN4b68bXkOIS7nYouuh7r8n"

def authenticate():  # authenticates with tweepy
    authenticator = tweepy.OAuthHandler(apiKey, apiKey_Secret)
    API = tweepy.API(authenticator)
    return API


listener = CovidStreamListner()

stream = tweepy.Stream(auth=authenticate().auth, listener=listener)



try:
    stream.filter(track=["covid"], languages=["en"], is_async=True)

    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Stopped")
finally:
    print('Done.')
    stream.disconnect()





