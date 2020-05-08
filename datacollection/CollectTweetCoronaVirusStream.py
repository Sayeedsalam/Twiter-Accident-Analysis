import json
import time
from pprint import pprint

import preprocessor as p
import gensim
import schedule
import tweepy
from elasticsearch import Elasticsearch
from pymongo import MongoClient


db = MongoClient().tweets

from clustering.TweetCluster import TweetCluster

keywords = ["corona", "covid"]

lanugae = {"spanish": "es", "english": ""}

states = {
            'AL': 'Alabama',
            'AK': 'Alaska',
            'AZ': 'Arizona',
            'AR': 'Arkansas',
            'CA': 'California',
            'CO': 'Colorado',
            'CT': 'Connecticut',
            'DE': 'Delaware',
            'DC': 'District of Columbia',
            'FL': 'Florida',
            'GA': 'Georgia',
            'HI': 'Hawaii',
            'ID': 'Idaho',
            'IL': 'Illinois',
            'IN': 'Indiana',
            'IA': 'Iowa',
            'KS': 'Kansas',
            'KY': 'Kentucky',
            'LA': 'Louisiana',
            'ME': 'Maine',
            'MD': 'Maryland',
            'MA': 'Massachusetts',
            'MI': 'Michigan',
            'MN': 'Minnesota',
            'MS': 'Mississippi',
            'MO': 'Missouri',
            'MT': 'Montana',
            'NE': 'Nebraska',
            'NV': 'Nevada',
            'NH': 'New Hampshire',
            'NJ': 'New Jersey',
            'NM': 'New Mexico',
            'NY': 'New York',
            'NC': 'North Carolina',
            'ND': 'North Dakota',
            'OH': 'Ohio',
            'OK': 'Oklahoma',
            'OR': 'Oregon',
            'PA': 'Pennsylvania',
            'RI': 'Rhode Island',
            'SC': 'South Carolina',
            'SD': 'South Dakota',
            'TN': 'Tennessee',
            'TX': 'Texas',
            'UT': 'Utah',
            'VT': 'Vermont',
            'VA': 'Virginia',
            'WA': 'Washington',
            'WV': 'West Virginia',
            'WI': 'Wisconsin',
            'WY': 'Wyoming'
         }

def extract_state(status):

    inside_us = True
    place_name = ""
    if type(status) is tweepy.models.Status:
        status = status._json
    #Try to get the place from the place data inside the status dict
    if status["place"] is not None:
        try:
            place = status["place"]
            if place['country'] != 'United States':
                place_name = place['country'].title()
            elif place['place_type'] == 'admin':
                place_name = place['name'].title()
            elif place['place_type'] == 'city':
                place_name = states.get(place['full_name'].split(', ')[-1]).title()
        except AttributeError:
            place_name = ""
    #If the status dict has no place info, get the place from the user data
    else:
        place = status["user"]["location"]
        try:
            place = place.split(', ')[-1].upper().title()
        except AttributeError:
            place_name = None
        if place in states:
            place_name  = states[place]
        else:
            place_name = place.title()

    return ( place_name in states.values(), place_name)

def get_tweet_attributes(tweets_raw):  # gets attributes from tweets and its user\
    # pprint(tweets_raw)
    inside_us, state_name = extract_state(tweets_raw)
    text = ""

    if 'retweeted_status' in tweets_raw._json:
        text = tweets_raw._json['retweeted_status']['full_text']
    else:
        text = tweets_raw.full_text

    tweets_raw_data = {'id_str': tweets_raw.id_str,
                       'created_at': str(tweets_raw.created_at),
                       'text': text,
                       'tweet url': "https://twitter.com/" + str(tweets_raw.user.screen_name) + "/status/" + str(tweets_raw.id),
                       'source': tweets_raw.source,
                       'coordinates': tweets_raw.coordinates,
                       'favorite_count': tweets_raw.favorite_count,
                       'entities': tweets_raw.entities,
                       'lang': tweets_raw.lang,
                       'place': str(tweets_raw.place),
                       'within_USA': inside_us,
                       'state': state_name,

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

accessToken = "3939721633-EJIfdOWAcuWVjwFpsQp7Qs3P5NNyh3q7ihpC9ld"
accessToken_Secret = "wyQ9uA8vZ02Ni2fimTJegOSdIJTO95ttIJWWkDZUxl5cZ"
apiKey = "8VQzfyd9REr8gO2nesBRkJTru"
apiKey_Secret = "2CS4iYIdBq6SNSmTJCWUFJuhOjAkN4b68bXkOIS7nYouuh7r8n"

es = Elasticsearch(hosts=["149.165.157.107"])

def authenticate():  # authenticates with tweepy
    authenticator = tweepy.OAuthHandler(apiKey, apiKey_Secret)
    API = tweepy.API(authenticator)
    return API



prev_largest = ["", ""]
# w2v_model = gensim.models.KeyedVectors.load_word2vec_format("/Volumes/Untitled 2/Users/sayeed/GoogleNews-vectors-negative300.bin", binary=True)
# print(type(w2v_model))
#w2v_model = None
clusters = []
for k in keywords:
    clusters.append([])
id = 0
current_index = 0

log_file = open("download_log.log", "w+")
def cluster_tweets(new_messages):
    global id
    log_file.write("new message count, "+ str(len(new_messages))+"\n")
    log_file.flush()
    for message in new_messages:
        tweet = json.loads(message)

        # # for tweet in tweets:
        # max_cluster = None
        # max_sim = 0.8
        # upload_cluster = None
        #
        # for cluster in clusters[current_index]:
        #     sim = cluster.similarity(tweet)
        #     print(sim)
        #
        #     if sim > max_sim:
        #         max_cluster = cluster
        #         max_sim = sim
        # if max_cluster == None:
        #     print("Appending New Cluster")
        #     new_cluster = TweetCluster(w2v_model, id)
        #     id += 1
        #     new_cluster.addTweet(tweet)
        #     clusters[current_index].append(new_cluster)
        #     print("== Added new Cluster")
        #     new_cluster.print_cluster()
        #     tweet["cluster_id"] = new_cluster.get_cluster_id()
        #     upload_cluster = new_cluster
        # else:
        #     max_cluster.addTweet(tweet)
        #     print("== Added to existing cluster")
        #     max_cluster.print_cluster()
        #     tweet["cluster_id"] = max_cluster.get_cluster_id()
        #     upload_cluster = max_cluster
        #
        # #es.index(index=keywords[current_index]+"_tweets_statewise", id=id, doc_type="Message", body=tweet)
        # id += 1
        tweet["cluster_id"]= -1
        db[keywords[current_index]+"_ext_es"].insert(tweet)


# a = authenticate().get_status(1241924291651911680, tweet_mode="extended")
#
# if 'retweeted_status' in a._json:
#     print(a._json['retweeted_status']['full_text'])
# else:
#     print("Full Text :", a.full_text)

def collect_tweets():
    global prev_largest
    results = authenticate().search(q=keywords[current_index],
                                    since_id=prev_largest[current_index],
                                    lang="es",
                                    count=500,
                                    tweet_mode="extended")
    return results


def process_tweet():
    global prev_largest, current_index
    tweets = collect_tweets()
    print(len(tweets), " collected")
    tweets_attribute = [get_tweet_attributes(tweet) for tweet in tweets]

    for tweet in tweets_attribute:
        if prev_largest[current_index] < tweet["id_str"]:
            prev_largest[current_index] = tweet["id_str"]
        cleaned_text = p.clean(tweet["text"])
        tweet["text_cleaned"] = cleaned_text

    tweets_json = [json.dumps(tweet) for tweet in tweets_attribute]

    cluster_tweets(tweets_json)
    print("Keyword in this step was", keywords[current_index])
    print("clustering done")

    current_index += 1
    if current_index == len(keywords):
        current_index = 0


def scheduleIteration():  # scheduler
    global numInc
    schedule.every(30).seconds.do(process_tweet)

    while True:
        schedule.run_pending()
        time.sleep(1)


process_tweet()
scheduleIteration()