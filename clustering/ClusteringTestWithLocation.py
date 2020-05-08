import json

from elasticsearch import Elasticsearch

#from Tweet import Tweet
from clustering.TweetCluster import TweetCluster
import gensim
from multiprocessing import Pool
from kafka import KafkaProducer, KafkaConsumer
import schedule
from summarization.temp import summerize, remove_punctuation, remove_stop_words
import pandas as pd

import spacy
from spacy import displacy
from collections import Counter
import en_core_web_sm
nlp = en_core_web_sm.load()

def tweet_entities(tweet_text):

    processed = nlp(tweet_text)
    return [(X.text, X.label_) for X in processed.ents]

tweets = []

w2v_model = gensim.models.KeyedVectors.load_word2vec_format("/Volumes/Untitled 2/Users/sayeed/GoogleNews-vectors-negative300.bin", binary=True)
print(type(w2v_model))
print("Model Load Complete")

clusters = []

es = Elasticsearch(hosts=["149.165.157.107"])

id = 0

def update_counters():
    global clusters
    removable_cluster = []
    for cluster in clusters:
        val = cluster.decrease_counter()
        if val == 0:
            removable_cluster.append(cluster)
    for c in removable_cluster:
        clusters.remove(c)
def calculate_relevancy(text, target_text):
    return 1.0

def summarization(texts):
    new_tweets = remove_punctuation(texts)
    new_tweets = remove_stop_words(new_tweets)
    print(new_tweets)

    df = pd.DataFrame(new_tweets, columns=['tweet_texts'])
    return summerize(df)


# schedule.every(1).minute.do(update_counters)
# schedule.run_pending()

kafka_consumer = KafkaConsumer("accident_tweet", bootstrap_servers="localhost:9092")

for message in kafka_consumer:

    tweet = json.loads(message.value.decode("utf-8"))
    tweet["entities"] = tweet_entities(tweet["text"])
#for tweet in tweets:
    max_cluster = None
    max_sim = 0.8
    upload_cluster = None

    for cluster in clusters:
        sim = cluster.similarity(tweet)
        print(sim)

        if sim > max_sim:
            max_cluster = cluster
            max_sim = sim
    if max_cluster == None:
        print("Appending New Cluster")
        new_cluster = TweetCluster(w2v_model, id)
        id += 1
        new_cluster.addTweet(tweet)
        clusters.append(new_cluster)
        print("== Added new Cluster")
        new_cluster.print_cluster()
        tweet["cluster_id"] = new_cluster.get_cluster_id()
        upload_cluster = new_cluster
    else:
        max_cluster.addTweet(tweet)
        print("== Added to existing cluster")
        max_cluster.print_cluster()
        tweet["cluster_id"] = max_cluster.get_cluster_id()
        upload_cluster = max_cluster


    es.index(index="location_tweets", id=id, doc_type="Message", body=tweet)



    upload_cluster.print_cluster()
    tweets_text = []
    for tweet in upload_cluster.get_tweets():
        tweets_text.append(tweet["text"])


    if len(tweets_text) > 1:
        try:
            summary_text = summarization(tweets_text)
        except IndexError:
            summary_text = "Not Available"
    else:
        summary_text=tweets_text[0]


    upload_data = {}
    upload_data["_id"] = str(upload_cluster.get_cluster_id())
    upload_data["summary"] = summary_text
    upload_data["type_of_injury"] = "N/A"
    upload_data["num_people_affected"] = -1
    upload_data["reported_locations"]= []
    upload_data["occur_time"]= upload_cluster.get_created_at()
    upload_data["related_tweets"] = []
    upload_data["geolocation"]= {"longitude": 0.00, "latitude": 0.00}
    upload_data["key_terms"] = []

    i = 0
    # for tweet in upload_cluster.get_tweets():
    #     tweet_info = {}
    #
    #     #tweet_info["username"] = tweet['username']
    #     tweet_info["message"] = tweet['text']
    #     tweet_info["username"] = tweet['user']['screen_name']
    #     tweet_info["name"] = tweet['user']['name']
    #     tweet_info["profile_image"] = tweet['user']["profile_image_url_https"]
    #     tweet_info["tweet_url"] = tweet["tweet url"]
    #     tweet_info["media_url"] = ""
    #     tweet_info["relevancy_score"] = calculate_relevancy(tweet['text'], summary_text)
    #
    #     #tweet_info["key_terms"] = []
    #     upload_data["related_tweets"].append(tweet_info)
    #
    #
    # upload_data["instagram_posts"] = []
    #
    # import requests
    # import json
    #
    #
    #
    # # data= {"data": json.dumps(data)}

    # resp = requests.post("http://149.165.157.107:5001/upload", json=upload_data)
    #
    # print(resp.status_code)













