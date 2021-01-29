r"""
 To run this on your local machine, you need to first run a Netcat server
    `$ netcat -vvl -p 5858`
 and then run the example
    `$SPARK_HOME/bin/spark-submit spark/primeri/stateful_network_wordcount.py localhost 5858`
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
        print("Usage: stateful_network_wordcount.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)
    
    sc = SparkContext(appName="SparkStreamingStatefulNetworkWordCount")
    quiet_logs(sc)
    ssc = StreamingContext(sc, 1)

    # Sets the context to periodically checkpoint the DStream operations for master fault-tolerance.
    # The graph will be checkpointed to the specified directory every batch interval.
    ssc.checkpoint("stateful_checkpoint_direcory")

    # RDD with initial state (key, value) pairs
    initialStateRDD = sc.parallelize([(u'asvsp', 1), (u'rules', 1)])

    def updateFunc(new_values, last_sum):
        return sum(new_values) + (last_sum or 0)

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    running_counts = lines.flatMap(lambda line: line.split(" "))\
                          .map(lambda word: (word, 1))\
                          .updateStateByKey(updateFunc, initialRDD=initialStateRDD)

    running_counts.pprint()

    ssc.start()
    ssc.awaitTermination()
