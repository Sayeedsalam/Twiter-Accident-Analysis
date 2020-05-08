import numpy as np
import pandas as pd

from twython import Twython
import tweepy

from tqdm import *

# auth for twitter
accessToken = "3939721633-EJIfdOWAcuWVjwFpsQp7Qs3P5NNyh3q7ihpC9ld"
accessToken_Secret = "wyQ9uA8vZ02Ni2fimTJegOSdIJTO95ttIJWWkDZUxl5cZ"
apiKey = "8VQzfyd9REr8gO2nesBRkJTru"
apiKey_Secret = "2CS4iYIdBq6SNSmTJCWUFJuhOjAkN4b68bXkOIS7nYouuh7r8n"



tweet_ids = pd.read_csv('tweet_ids/150425104337_nepal_earthquake_20150425_vol-1.json.csv')
print(tweet_ids.head())
print("Total tweets: ",len(tweet_ids))

twitter = Twython(apiKey, apiKey_Secret, accessToken, accessToken_Secret)

test_id = tweet_ids.tweet_id[1][1:-1]
print("tweet id = ",test_id)

# quit()
# try:
#     tweet = twitter.show_status(id=test_id)
#     print(tweet)
#     print(tweet['text'])
# except:
#     print("oh rats")

for i in tqdm(range(2400, len(tweet_ids))):
    individual_id = tweet_ids.tweet_id.iloc[i][1:-1]
    try:
        tweet = twitter.show_status(id=individual_id)['text']
    except:
        tweet = None
    tweet_ids.set_value(i, 'tweet_texts', tweet)

tweet_ids.to_csv('string_filled_tweets.csv', encoding = 'utf-8')

stripped_tweets = tweet_ids[pd.notnull(tweet_ids.tweet_texts)]
stripped_tweets.to_csv('stripped_filled_tweets.csv', encoding = 'utf-8')
read_tweets = pd.read_csv('stripped_filled_tweets.csv', encoding = 'ISO-8859-1')
print(len(read_tweets))
