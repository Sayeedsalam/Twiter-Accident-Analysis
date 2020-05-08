import GetOldTweets3 as got

tweetCriteria = got.manager.TweetCriteria().setQuerySearch("Social Distance Collin county")\
                                           .setMaxTweets(4000)
tweets = got.manager.TweetManager.getTweets(tweetCriteria)

for tweet in tweets:
    print(tweet.text)

print(len(tweets))