from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as f
from textblob import TextBlob
from datetime import datetime
import tweepy as tw
import time

def reply_to_tweet(tweet):
    user = tweet.user_name
    if tweet.sentiment=="positive":
      msg = "@%s Thank you for the positive attitude!" %user
    else:
      msg = "@%s We are sorry for this feeling!" %user
    msg_sent = api.update_status(msg, tweet.tweet_id)
    print("=================reply sent ==========================")
    
def preprocessing(lines):
    words = lines.select(explode(split(lines.value, "t_end")).alias("word"))
    
    words= words.withColumn('user_id',f.split('word',';').getItem(0))\
    .withColumn('tweet_creation_date', f.split('word', ';').getItem(1))\
    .withColumn('text', f.split('word', ';').getItem(2))\
    .withColumn('tweet_id', f.split('word', ';').getItem(3))\
    .withColumn('user_name', f.split('word', ';').getItem(4))\
    .withColumn('account_creation_date', f.split('word', ';').getItem(5))\
    .withColumn('name', f.split('word', ';').getItem(6))\
    .withColumn('description', f.split('word', ';').getItem(7))\
    .withColumn('location', f.split('word', ';').getItem(8))\
    .withColumn('friends_count', f.split('word', ';').getItem(9))\
    .withColumn('followers_count', f.split('word', ';').getItem(10))\
    .withColumn('status_count', f.split('word', ';').getItem(11))\
    .withColumn('retweeted', f.split('word', ';').getItem(12))\
    .withColumn('retweet_count', f.split('word', ';').getItem(13))\
    .withColumn('favorited', f.split('word', ';').getItem(14))

    return words

# text classification
def polarity_detection(text):
    return TextBlob(text).sentiment.polarity
def subjectivity_detection(text):
    return TextBlob(text).sentiment.subjectivity
def text_classification(words):
    # polarity detection
    polarity_detection_udf = udf(polarity_detection, StringType())
    words = words.withColumn("polarity", polarity_detection_udf("word"))
    # subjectivity detection
    subjectivity_detection_udf = udf(subjectivity_detection, StringType())
    words = words.withColumn("subjectivity", subjectivity_detection_udf("word"))
    return words
    
def sentiment_analysis(tweet_text):
        
    analysis = TextBlob(tweet_text)
    if analysis.sentiment.polarity >= 0:
      return 'positive'
    else:
      return 'negative'
      
if __name__ == "__main__":
    #preparing for reply
    access_token = 'yours'
    access_token_secret =  'yours'
    consumer_key =  'yours'
    consumer_secret =  'yours'
    
    auth = tw.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tw.API(auth)
    # create Spark session
    spark = SparkSession.builder.appName("Big_Data_Case_Study").getOrCreate()
    spark.conf.set("spark.sql.parquet.compression.codec", "gzip")

    
    #### Subscribe to 1 topic
    lines = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "172.18.0.2:6667").option("subscribe", "final_topic").load()
    lines.printSchema()
    # Preprocess the data
    words = preprocessing(lines)
    # text classification to define polarity and subjectivity
    words = text_classification(words)
    udf_sentiment = udf(sentiment_analysis, StringType())
    words = words.withColumn('sentiment',udf_sentiment('text'))
    words = words.repartition(1)   
    reply_query = words.writeStream.foreach(reply_to_tweet).start()
    
   
    words.printSchema()
    
    query = words \
        .writeStream \
        .format("parquet") \
        .option("path", "hdfs://sandbox-hdp.hortonworks.com:8020/user/root/case_study/parquet_files") \
        .option("checkpointLocation", "hdfs://sandbox-hdp.hortonworks.com:8020/user/root/case_study/checkpoint") \
        .start()
        


    query.awaitTermination()
    reply_query.awaitTermination()
