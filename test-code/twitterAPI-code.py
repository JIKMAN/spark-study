import tweepy

twitter_api_key = "YkZvad3vXqnArsEHtfbcaedo5"
twitter_api_secret_key = "SmHZGrW31XUHPl0B0McogZ7N7dJuPbtlxIXaTcr13XMxIyh8vt"
twitter_access_token = "1419577272164442116-RKTNXp7ftBoOK6aGutZhEJqzLorjj5"
twitter_access_token_secret = "oPj5SFs7SkuCU5bB5VwirKoRssPnjMTuiAVIibQcc3EbA"

class SimpleStreamListener(tweepy.StreamListener):
   def on_status(self, status):
       print(status)

stream_listener = SimpleStreamListener()

auth = tweepy.OAuthHandler(twitter_api_key, twitter_api_secret_key)
auth.set_access_token(twitter_access_token, twitter_access_token_secret)

twitterStream = tweepy.Stream(auth, stream_listener)
twitterStream.filter(track=['tesla'])