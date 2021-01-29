r"""
 Run the example
    `$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 $SPARK_HOME/realtime/kafka_uv_index.py zoo1:2181 uv_index`
"""
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.functions import *
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
    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic1> ", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="SparkStreamingKafkaUvIndex")
    quiet_logs(sc)
    ssc = StreamingContext(sc, 31)

    zooKeeper, topic1= sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zooKeeper, "spark-streaming-consumer", {topic1: 1})
    kvs.pprint()
    lines = kvs.map(lambda x: "{0},{1},{2}".format(x[0], x[1].split()[2], x[1].split()[6]))
  
    
    lines.pprint()
 
        
     # Convert RDDs of the words DStream to DataFrame and run SQL query
    def process(time, rdd):
        print("========= %s =========" % str(time))


        schemaString = "datetime uv_index uv_index_max"
        fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
        schema = StructType(fields)

        try:
            # Get the singleton instance of SparkSession
            spark = getSparkSessionInstance(rdd.context.getConf())

            # Convert RDD[String] to RDD[Row] to DataFrame
            rowRdd = rdd.map(lambda w: Row(datetime=w.split(",")[0], uv_index=w.split(",")[1], uv_index_max=w.split(",")[3]))
            #print(rowRdd.take(1))
           
            dataFrame = spark.createDataFrame(rowRdd, schema=schema)
            dataFrame.show()

            # # Creates a temporary view using the DataFrame.
            dataFrame.createOrReplaceTempView("average")
            
            dataFrameAverage = spark.sql("select avg(uv_index) as avg from average")
            dataFrameAverage = dataFrameAverage.withColumn("Advice", expr("case when avg=0 then 'Safe' " +  
                                                                        "when avg>0 and avg <2 then 'Wear sunglasses' " +
                                                                        "when avg>2 and avg <4 then 'Wear sunglasses, cover head and body and use low SPF sunscreen' " +
                                                                        "when avg>4 and avg <6 then 'Cover head and body, use high SPF sunscreen, limit sun from 11am-5am' " +
                                                                        "when avg>6 and avg <8 then 'Cover head and body, use high SPF sunscreen, avoid sun from 11am-5am' " +               
                                                                        "else 'Unknown' end"))
            dataFrameAverage.show()

        except Exception as e:
            print(e)
            pass

    

    lines.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()