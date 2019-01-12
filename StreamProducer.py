from kafka import KafkaProducer
import kafka
import json
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

consumer_key = "0u7RtnOnFbFuec0E1pvoNtOdU"
consumer_secret = "UdDBvzE3lojSmPl4s0889Hl4VO7aRKp6mcPmMRtfPGeVNwV7dE "
access_token = "901210657-9MTFPTjSUmL70GGJysL1ZlnBuiP5RIZgnKPoGuT5"
access_secret = "wTNzP5Q8hGJLTASEQoGWNyuKFqPZLLtWRfVuu4RVLe9Ws"

# TWITTER API AUTH
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)


# Twitter Stream Listener
class KafkaPushListener(StreamListener):
    def __init__(self):
        # localhost:9092 = Default Zookeeper Producer Host and Port Adresses
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])



    def on_data(self, data):
        # Producer produces data for consumer

        self.producer.send("twitter", data.encode('utf-8'))
        print(data)
        return True

    def on_error(self, status):
        print(status)
        return True


# Twitter Stream Config
twitter_stream = Stream(auth, KafkaPushListener())

# Produce Data that has Game of Thrones hashtag (Tweets)
twitter_stream.filter(track=['#trump'])