from pprint import pprint

import json
#from pymongo import MongoClient
import schedule
import time
import tweepy
import csv
import datetime
#from bson import ObjectId, json_util
from elasticsearch import Elasticsearch
from geopy.geocoders import Nominatim
#from pymongo import MongoClient

from kafka import KafkaProducer





es = Elasticsearch(hosts=["149.165.157.107"])
# auth for twitter
accessToken = "3939721633-EJIfdOWAcuWVjwFpsQp7Qs3P5NNyh3q7ihpC9ld"
accessToken_Secret = "wyQ9uA8vZ02Ni2fimTJegOSdIJTO95ttIJWWkDZUxl5cZ"
apiKey = "8VQzfyd9REr8gO2nesBRkJTru"
apiKey_Secret = "2CS4iYIdBq6SNSmTJCWUFJuhOjAkN4b68bXkOIS7nYouuh7r8n"
radius = 11  # average radius of the cities
numInc = 0
location = ""
interval = 2 #minutes

largest_id = "1"
prev_largest = "1"


kafka_producer = KafkaProducer(bootstrap_servers=['localhost:9092'])


def send_tweet_to_pipeline(tweet):
    print("Sending Data to Kafka")
    tweet_str = json.dumps(tweet)
    kafka_producer.send("tweets", bytes(tweet_str, encoding="utf-8"))




def authenticate():  # authenticates with tweepy
    authenticator = tweepy.OAuthHandler(apiKey, apiKey_Secret)
    API = tweepy.API(authenticator)
    return API


def compareResults(results_without_location, results_with_location):  # removes dups
    global radius, largest_id

    tweets_set1 = []
    tweets_set2 = []
    store_this_set = []

    # getting the raw tweets attributes
    for tweet in results_with_location:
        tweets_set1.append(get_tweet_attributes(tweet))
    for tweet in results_without_location:
        tweets_set2.append(get_tweet_attributes(tweet))

    for x in tweets_set1:  # tweets searched with search_query
        if x in tweets_set2:  # tweets searched with lati,long
            tweets_set1.remove(x)  # removes duplicates
    # print(len(tweets_set1))
    # print(len(tweets_set2))
    # print(len(store_this_set))

    store_this_set = tweets_set1 + tweets_set2
    for tweet in store_this_set:
        if tweet["id_str"] > largest_id:
            largest_id = tweet["id_str"]
    print (largest_id)
    store_results_in_file(store_this_set)
    for tweet in store_this_set:
        send_tweet_to_pipeline(tweet)
    # mongoDB(store_this_set)


def get_tweet_attributes(tweets_raw):  # gets attributes from tweets and its user\
    # pprint(tweets_raw)
    tweets_raw_data = {'id_str': tweets_raw.id_str,
                       'created_at': str(tweets_raw.created_at),
                       'text': tweets_raw.full_text,
                       'tweet url': "https://twitter.com/" + str(tweets_raw.user.screen_name) + "/status/" + str(tweets_raw.id),
                       'source': tweets_raw.source,
                       'coordinates': tweets_raw.coordinates,
                       'favorite_count': tweets_raw.favorite_count,
                       'entities': tweets_raw.entities,
                       'lang': tweets_raw.lang,
                       'place': str(tweets_raw.place),
                       'user': {
                            'id': tweets_raw.user.id,
                            'name': tweets_raw.user.name,
                            'screen_name': tweets_raw.user.screen_name,
                            'location': tweets_raw.user.location,
                            'url': tweets_raw.user.url,
                            'description': tweets_raw.user.description,
                            'verified': tweets_raw.user.verified,
                            'followers_count': tweets_raw.user.followers_count,
                            'friends_count': tweets_raw.user.friends_count,
                            'listed_count': tweets_raw.user.listed_count,
                            'favourites_count': tweets_raw.user.favourites_count,
                            'statuses_count': tweets_raw.user.statuses_count,
                            'profile_image_url_https': tweets_raw.user.profile_image_url_https,
                            'default_profile': tweets_raw.user.default_profile,
                            'default_profile_image': tweets_raw.user.default_profile_image}}
    return tweets_raw_data


# def mongoDB(results):  # Stores in mongodb
#     old_data = []
#     client = pymongo.MongoClient('mongodb://localhost:27017/')
#     data_base = client['TwitterSearcher']
#     collection = data_base["Tweets Data"]
#     for i in collection.find():
#         old_data.append(i)
#     for i in old_data:
#         try:
#             del i["_id"]
#         except KeyError:
#             print("Key '_id' not found")
#     for i in results:
#         if i not in old_data:
#             collection.insert_one(i)
#     # get_new_data()

#
# def __get_mongo_connection():
#     # For local debugging
#     MONGO_SERVER_IP = "172.29.100.22"
#     MONGO_PORT = "3154"
#     MONGO_USER = "event_reader"
#     MONGO_PSWD = "dml2016"
#     NUM_ARTICLES = 1000
#     password = MONGO_PSWD
#     # return MongoClient('mongodb://' + MONGO_USER + ':' + password + '@' + MONGO_SERVER_IP + ":" + MONGO_PORT)
#     return MongoClient()


# def get_new_data():  # kibana
#     try:
#         last_id = es.search(index="accident", body={
#             'sort': {
#                 '_id': {
#                     'order': 'desc'
#                 }
#             },
#             'size': 1
#         })
#         last_id = last_id['sort'][0]
#
#     except Exception:
#         last_id = None
#
#     if last_id is None:
#         last_id = bytes(12)
#     obj_id = ObjectId(last_id)
#
#     conn = __get_mongo_connection()
#     db = conn.TwitterSearcher
#
#     # cursor = db.tweets_accident.find({"_id":{"$gt": obj_id}})
#     cursor = db["tweets_view"].find({})
#     for entry in cursor:
#         id = str(entry["_id"])
#         print(entry.pop("_id"))
#
#         es.index(index="accident", id=id, doc_type="Message", body=entry)
#

def store_results_in_file(results):  # Stores into a json and csv
    new_data = []
    # json
    with open('tweets.json') as File:
        for tweet in File:
            new_data.append(json.loads(tweet))
    for x in results:
        if x not in new_data:
            new_data.append(x)
    with open('tweets.json', 'w') as File:
        for x in new_data:
            json.dump(x, File)
            File.write('\n')
    # csv
    with open('tweets.csv', 'w', newline='', encoding="utf-8") as File:
        write = csv.writer(File, quoting=csv.QUOTE_ALL)
        write.writerow(new_data)


def search_without_location(searchQuery):  # searches with out a location
    results = authenticate().search(q=searchQuery, since_id=prev_largest, lang="en", count=100, tweet_mode="extended")
    return results


def search_with_location(latitude, longitude):  # searches with coordinates and has a radius that is changeable
    tweet_search_location = str(latitude) + "," + str(longitude) + "," + str(radius) + "mi"
    results = authenticate().search(q="Car Accident -filter:retweets", since_id=prev_largest, geocode=tweet_search_location, lang="en",
                                    count=100, tweet_mode="extended")
    return results


def geoLocator(latitude, longitude):  # reverse geolocator for the coords
    geolocator = Nominatim(user_agent="Twitter Searcher")
    coordinates = (str(latitude), str(longitude))
    city = geolocator.reverse(coordinates)
    location_precise = [x.strip() for x in city.address.split(',')]
    print(location_precise)
    print("    0		1		2	3	 4 	     5")
    precision = int(input("Precision: "))
    return location_precise[precision]


def twitterSearcher(cities): # main function that controls both types of searches
    global numInc, location, prev_largest, largest_id
    for i in cities:
        split_city = [x.strip() for x in i.split(',')]
        search_query = split_city[0] + " Accident -filter:retweets"
        location = split_city[0]
        pprint(split_city)
        compareResults(search_without_location(search_query), search_with_location(split_city[1], split_city[2]))
    numInc += 1
    prev_largest = largest_id

    print("Largest Tweet ID", prev_largest)


def scheduleIteration(iterations, cities):  # scheduler
    global numInc
    schedule.every(2).minutes.do(twitterSearcher, cities)

    if iterations == -1:
        schedule.run_pending()
        time.sleep(1)
    else:
        while numInc < iterations:
            schedule.run_pending()
            time.sleep(1)


if __name__ == '__main__':
    #cities = ["New York,40.7128,-74.0060", "Lagos,6.5244,3.3792", "Dallas,32.7767,-96.7970"]
    cities = ["Lagos,6.5244,3.3792"]
    twitterSearcher(cities)
    scheduleIteration(1000, cities)
