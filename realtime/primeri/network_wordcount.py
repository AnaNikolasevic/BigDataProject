r"""
 To run this on your local machine, you need to first run a Netcat server
    `$ netcat -vvl -p 5858`
 and then run the example
    `$SPARK_HOME/bin/spark-submit spark/primeri/network_wordcount.py localhost 5858`
"""
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: network_wordcount.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)
    sc = SparkContext(appName="SparkStreamingNetworkWordLengthCount")
    quiet_logs(sc)
    ssc = StreamingContext(sc, 3)

    initialStateRDD = ["a", "a", "a", "a", "a", "a", "b", "c"]
    initialStateAsDStream = ssc.queueStream(initialStateRDD, False)

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    counts = lines.flatMap(lambda line: line.split(" "))\
                  .union(initialStateAsDStream)\
                  .filter(lambda word: not(word.startswith("a")))\
                  .map(lambda word: ("len_"+str(len(word)), 1))\
                  .reduceByKey(lambda a, b: a+b)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
