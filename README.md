# twitter_sentiment_analysis_Kafka_Spark
The project aims at building a data platform for real time moderation and analytics of twitter data.

I used tweepy Python library for accessing the Twitter API.

I use TextBlob Python library for sentiment analysis.

I worked with Spark Structured Streaming

#Commands:

#go to kafka broker path and make kafka topic

cd /usr/hdp/current/kafka-broker

bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic final_topic

#configure date

ntpdate -u time.google.com

#configure python virtual environment

python3.6 -m venv ./iti41

source iti41/bin/activate

#run the producer script

python Kafka_producer.py

#to test the messages in topic 

bin/kafka-console-consumer.sh --bootstrap-server 172.18.0.2:6667 --topic final_topic

#run the spark consumer script

spark-submit --packages  org.apache.spark:spark-streaming-kafka-0-10_2.12:2.4.3,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 --master local[*] Spark_consumer.py

#make hive table 

beeline -u 'jdbc:hive2://sandbox-hdp.hortonworks.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2'

CREATE EXTERNAL TABLE twitter_analysis(
user_ID string,tweet_creation_date string, text string, tweet_id string, user_name string,
account_creation_date string, name string, description string, location string,friends_count string,followers_count string,status_count string,retweet_count string,polarity string,subjectivity string,sentiment string                  
)
STORED AS PARQUET
LOCATION "/user/root/case_study/parquet_files";

#connect hive with ODBC Driver to be able to connect on power BI and make live reporting 
