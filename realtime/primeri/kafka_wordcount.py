r"""
 Run the example
    `$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 $SPARK_HOME/primeri/kafka_wordcount.py zoo1:2181 subreddit-politics subreddit-worldnews`
"""
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import *

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: kafka_wordcount.py <zk> <topic1> <topic2>", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="SparkStreamingKafkaWordCount")
    quiet_logs(sc)
    ssc = StreamingContext(sc, 3)

    ssc.checkpoint("stateful_checkpoint_direcory")

    zooKeeper, topic1, topic2 = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zooKeeper, "spark-streaming-consumer", {topic1: 1, topic2: 1})
    
    lines = kvs.map(lambda x: x[1])

    stopWordList = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself",\
                                                            "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself",\
                                                            "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these",\
                                                            "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does",\
                                                            "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by",\
                                                            "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below",\
                                                            "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here",\
                                                            "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such",\
                                                            "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don",\
                                                            "should", "now"]

    def updateFunc(new_values, last_sum):
        if last_sum is None:
            last_sum = 0
        return sum(new_values, last_sum)
    
    words = lines.flatMap(lambda line: line.split(" ")) \
        .filter(lambda word: word.strip())\
        .map(lambda word: (word, 1))\
        .updateStateByKey(updateFunc)
        
     # Convert RDDs of the words DStream to DataFrame and run SQL query
    def process(time, rdd):
        print("========= %s =========" % str(time))


        schemaString = "word value"
        fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
        schema = StructType(fields)

        try:
            # Get the singleton instance of SparkSession
            spark = getSparkSessionInstance(rdd.context.getConf())

            # Convert RDD[String] to RDD[Row] to DataFrame
            rowRdd = rdd.map(lambda w: Row(word=w[0], value=w[1]))

            wordsDataFrame = spark.createDataFrame(rowRdd, schema=schema)

            # Creates a temporary view using the DataFrame.
            wordsDataFrame.createOrReplaceTempView("words")

            # Do word count on table using SQL and print it
            wordCountsDataFrame = \
                spark.sql("select word, value from words")

            wordCountsDataFrame.show()

        except Exception as e:
            print(e)
            pass

    

    words.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()