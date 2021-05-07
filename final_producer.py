from kafka import KafkaConsumer , KafkaProducer
from datetime import datetime
import tweepy as tw
import time

access_token = 'yours'
access_token_secret =  'yours'
consumer_key =  'yours'
consumer_secret =  'yours'

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth)

def normalize_timestamp(time):
    mytime = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
    return (mytime.strftime("%Y-%m-%d %H:%M:%S"))
    
producer = KafkaProducer(bootstrap_servers='172.18.0.2:6667')
topic_name = 'final_topic'




def get_twitter_data():
    res = api.search("#Egypt")
    for i in res:
        record =''
        record +=str(i.user.id_str)
        record += ';'
        record += str(normalize_timestamp(str(i.created_at)))
        record += ';'
        record +=str(i.text)
        record += ';'
        record +=str(i.id)
        record += ';'
        record +=str(i.user.screen_name)
        record += ';'
        record +=str(normalize_timestamp(str(i.user.created_at)))
        record += ';'
        record +=str(i.user.name)
        record += ';'
        record +=str(i.user.description)
        record += ';'
        record +=str(i.user.location)
        record += ';'
        record +=str(i.user.friends_count)
        record += ';'
        record +=str(i.user.followers_count)
        record += ';'
        record +=str(i.user.statuses_count)
        record += ';'
        record +=str(i.retweeted)
        record += ';'
        record +=str(i.retweet_count)
        record += ';'
        record +=str(i.favorited)
        record += ';'
        print("================")
        producer.send(topic_name, str.encode(record))
        
        
get_twitter_data()
def periodic_work(interval):
    while True:
        get_twitter_data()
        time.sleep(interval)
        
periodic_work(60* 0.1)
