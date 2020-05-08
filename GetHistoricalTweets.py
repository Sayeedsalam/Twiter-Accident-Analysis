import GetOldTweets3 as got



tweetCriteria = got.manager.TweetCriteria().setQuerySearch('accident')\
                                           .setNear("Lagos, Nigeria")\
                                           .setWithin("100mi")\
                                           .setSince("2019-01-01")\
                                           .setUntil("2019-01-31")\
                                           .setMaxTweets(500)



tweets = got.manager.TweetManager.getTweets(tweetCriteria)
print(len(tweets))

