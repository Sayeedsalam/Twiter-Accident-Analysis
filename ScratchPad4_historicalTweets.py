import GetOldTweets3 as got
from datetime import datetime, timedelta
import json
from pprint import pprint


def get_tweets(longitude, latitude, date_time):
    date = str(date_time)[0:10]
    time = str(date_time)[11:19]
    tweets_data = []

    if time < "00:05:00":  # edge case where tweet time is near midnight
        _date = datetime.strptime(date, '%Y-%m-%d') - timedelta(days=1)  # subtracts a day from date
        # tweetCriteria = got.manager.TweetCriteria().setSince(str(_date)).setUntil(date).setQuerySearch("Accident") \
        #     .setNear(str(longitude) + ", " + str(latitude)).setWithin("5mi").setMaxTweets(100)  # searches
        tweetCriteria = got.manager.TweetCriteria().setSince(str(_date)).setUntil(date).setQuerySearch("Accident") \
            .setMaxTweets(100)  # temporarily searching with out coords
        tweets = got.manager.TweetManager.getTweets(tweetCriteria)
        for tweet in tweets:
            if is_time_between(time, str(tweet.time)[11:19], midnight=True):
                tweets_data.append(get_tweet_data(tweet))  # stores in a list of dicts
    else:
        # tweetCriteria = got.manager.TweetCriteria().setUntil(date).setQuerySearch("Accident") \
        #     .setNear(str(longitude) + ", " + str(latitude)).setWithin("5mi").setMaxTweets(100)
        tweetCriteria = got.manager.TweetCriteria().setUntil(date).setQuerySearch("Accident").setMaxTweets(100)
        tweets = got.manager.TweetManager.getTweets(tweetCriteria)
        for tweet in tweets:
            if is_time_between(time, str(tweet.date)[11:19], midnight=False):
                tweets_data.append(get_tweet_data(tweet))
    return tweets_data


def is_time_between(check_time, tweet_time, midnight):  # compares tweet time if it is in range
    if midnight:  # edge case
        begin_time = "23:55:00"
        end_time = str(datetime.strptime(check_time, '%H:%M:%S') + timedelta(minutes=20))
        return tweet_time >= begin_time or tweet_time <= end_time
    else:
        begin_time = str(datetime.strptime(check_time, '%H:%M:%S') - timedelta(minutes=5))[11:19]
        end_time = str(datetime.strptime(check_time, '%H:%M:%S') + timedelta(minutes=20))[11:19]
        return begin_time <= tweet_time <= end_time


def get_tweet_data(tweet):  # returns dict of the tweet's data
    tweet_data = {
        'id': tweet.id,
        'link': tweet.permalink,
        'username': tweet.username,
        'to': tweet.to,
        'text': tweet.text,
        'date': tweet.date,
        'retweets': tweet.retweets,
        'favorites': tweet.favorites,
        'mentions': tweet.mentions,
        'hashtags': tweet.hashtags,
        'geo': tweet.geo
    }
    return tweet_data


def get_useable_datetime(accidentDate, accidentTime):
    accidentDate = datetime.strptime(accidentDate, '%m/%d/%y').strftime('%Y-%m-%d')
    accidentTime = datetime.strptime(accidentTime, '%I:%M %p').strftime('%H:%M')
    return datetime.strptime(accidentDate + " " + accidentTime + ":00", '%Y-%m-%d %H:%M:%S')


if __name__ == '__main__':
    with open("sample.json", "r") as read:
        data = json.load(read)

    for x in range(len(data)):
        try: # some data does not have location/time/id/or date
            location = data[x]['maps_output'][0]['geometry']['location']
            time = data[x]['time']
            incident_id = data[x]['id']
            date = data[x]['date']
            if len(get_tweets(location['lat'], location['lng'], get_useable_datetime(date, time))) != 0:
                print(x, incident_id, get_tweets(location['lng'], location['lat'], get_useable_datetime(date, time)))
        except:
            try:
                incident_id = data[x]['id']
                print("Error in location, time, date, or incident_id for #id: " + incident_id)
            except:
                print("Error in location, time, date, or incident_id for iteration: ", x)