r"""
 To run this on your local machine, you need to first run a Netcat server
    `$ netcat -vvl -p 5858`
 and then run the example
    `$SPARK_HOME/bin/spark-submit spark/primeri/windowed_network_wordcount.py localhost 5858`
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
        print("Usage: windowed_network_wordcount.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)
    sc = SparkContext(appName="SparkStreamingWindowesNetworkWordLengthCount")
    quiet_logs(sc)
    ssc = StreamingContext(sc, 2)

    # Sets the context to periodically checkpoint the DStream operations for master fault-tolerance.
    # The graph will be checkpointed to the specified directory every batch interval.
    ssc.checkpoint("windowed_checkpoint_direcory")


    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    counts = lines.flatMap(lambda line: line.split(" "))\
                  .map(lambda word: (word, 1))\
                  .reduceByKeyAndWindow(lambda a, b: a+b, 6, 4)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
