from pprint import pprint

import tweepy
from tweepy import api

accessToken = "3939721633-EJIfdOWAcuWVjwFpsQp7Qs3P5NNyh3q7ihpC9ld"
accessToken_Secret = "wyQ9uA8vZ02Ni2fimTJegOSdIJTO95ttIJWWkDZUxl5cZ"
apiKey = "8VQzfyd9REr8gO2nesBRkJTru"
apiKey_Secret = "2CS4iYIdBq6SNSmTJCWUFJuhOjAkN4b68bXkOIS7nYouuh7r8n"


def authenticate():  # authenticates with tweepy
    auth = tweepy.OAuthHandler(apiKey, apiKey_Secret)
    auth.set_access_token(accessToken, accessToken_Secret)
    API = tweepy.API(auth)
    return API


place_name = "United States"

api = authenticate()
result = api.geo_search(query="Collin County, Texas, United States")

pprint(result)