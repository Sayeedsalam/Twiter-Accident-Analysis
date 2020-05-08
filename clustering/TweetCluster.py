import numpy as np
import gensim
from scipy import spatial
from datetime import datetime, timezone

class TweetCluster:

    cluster_id = 1

    def __init__(self, w2v_model, id=0):
        self.counter = 30
        self.center = np.zeros(300)
        self.tweets = []
        self.w2v_model = w2v_model
        self.id = TweetCluster.cluster_id
        self.created_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %I:%M:%S")
        TweetCluster.cluster_id += 1

    def get_cluster_id(self):
        return self.id

    def get_created_at(self):
        return self.created_at

    def get_tweets(self):
        return self.tweets

    def __vectorize(self, text):
        words = text.split(" ")
        summation_vector = np.zeros(shape=[len(self.center)])
        num_ignored = 0
        for word in words:
            try:
                summation_vector = np.add(summation_vector, self.w2v_model.wv[word.lower()])
                # print (w2v_model.wv[word.lower()])
            except KeyError:
                num_ignored += 1

        return np.true_divide(summation_vector, len(words) - num_ignored)  # normalization

    def similarity(self, newTweet):

        if len(self.tweets) == 0:
            return 0
        else:
            if "incident_id" in self.tweets[0] and "incident_id" in newTweet:
                if self.tweets[0]["incident_id"] != newTweet["incident_id"]:
                    return 0
            return self.__similarity_vector(self.center, self.__vectorize(newTweet["text_cleaned"]))

    def __similarity_vector(self, vec1, vec2):
        return 1 - spatial.distance.cosine(vec1, vec2)

    def decrease_counter(self):
        self.counter -= 1
        return self.counter == 0

    def increase_counter(self):
        self.counter += 1

    def addTweet(self, newTweet):

        self.increase_counter()
        self.center = np.add(self.center*len(self.tweets)
                             , self.__vectorize(newTweet["text_cleaned"]))
        self.tweets.append(newTweet)
        self.center = np.true_divide(self.center, len(self.tweets))

    def merge_cluster(self, another_cluster):

        self.center =  np.add(self.center*len(self.tweets)
                             , another_cluster.center*len(another_cluster.tweets))
        self.increase_counter()
        self.tweets.append(another_cluster.tweets)
        self.center = np.true_divide(self.center, len(self.tweets))


    def cluster_similarity(self, another_cluster):
        return self.__similarity_vector(self.center, another_cluster.center)


    def print_cluster(self):
        print ("Number of tweets ", len(self.tweets))
        for tweet in self.tweets:
            print(tweet["text_cleaned"])



