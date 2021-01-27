#!/usr/bin/python
### before spark-submit: export PYTHONIOENCODING=utf8

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import max as max_
def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

conf = SparkConf().setAppName("uni").setMaster("spark://spark-master:7077")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

quiet_logs(spark)

from pyspark.sql.types import *

# //////////////////////////
schemaString = "timestamp Lat Lon UV_Index"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)
df = spark.read.csv("hdfs://namenode:9000/user/dataset/pliz.csv", header=True, mode="DROPMALFORMED", schema=schema)
#df = df.coalesce(2)
print(df.rdd.getNumPartitions())

# //////////////////////////
schemaStringMelanoma = "Data_type Cancer_group Year Sex Territory Count Age_standardised_rate ICD10_codes"
fieldsMelanoma = [StructField(field_name, StringType(), True) for field_name in schemaStringMelanoma.split()]
schemaMelanoma = StructType(fieldsMelanoma)
dfMelanoma = spark.read.csv("hdfs://namenode:9000/user/dataset/cancer_incidence_and_mortality_by_state_and_territory.csv", header=True, mode="DROPMALFORMED", schema=schemaMelanoma)
dfMelanoma = dfMelanoma.coalesce(2)
print(dfMelanoma.rdd.getNumPartitions())

# extracting year, month, day from timestamp
df = df.withColumn("Year", year(col("timestamp")))\
    .withColumn("Month", month(col("timestamp")))\
    .withColumn("Day", dayofyear(col("timestamp")))
df.show(truncate=False)
df = df.withColumn("UV_Index", col("UV_Index").cast(FloatType()))

# adding territory column depending of Lat and Lon
df = df.withColumn("Territory", expr("case when Lat = -34.04 and Lon = 151.1 then 'New South Wales' " +
                                      "when Lat = -34.92 and Lon = 138.62 then 'South Australia' " +
                                      "when Lat = -37.73 and Lon = 145.1 then 'Victoria' " +
                                      "when Lat = -27.45 and Lon = 153.03 then 'Queensland' " +
                                      "when Lat = -31.92 and Lon = 115.96 then 'Western Australia' " +
                                      "when Lat = -42.99 and Lon = 147.29 then 'Tasmania' " +
                                      "when Lat = -35.31 and Lon = 149.2 then 'Australian Capital Territory' " +
                                      "when Lat = -12.43 and Lon = 130.89 then 'Northern Territory' " +
                                      "else 'Unknown' end"))
dfMelanoma = dfMelanoma.withColumn("Year", col("Year").cast(IntegerType()))
dfMelanoma = dfMelanoma.filter(dfMelanoma["Cancer_group"] == "Melanoma of the skin")\
                        .filter((dfMelanoma["Year"]>2013) & (dfMelanoma["Year"]<2016))
                        


# max UV Index for each year in whole Australia
# df.groupBy("Year")\
#     .agg(
#         max(col("UV_Index")).alias("max_UV_Index"),
#         avg(col("UV_Index")).alias("avg_UV_Index"),
#     ).show(truncate=False)

# # max UV Index for every territory in each year
# df.groupBy("Territory").pivot("Year").max("UV_Index").show(truncate=False)

# # avg UV Index for every territory in each year
# df.groupBy("Territory").pivot("Year").avg("UV_Index").show(truncate=False)

# # max UV Index for every year and month
# df.groupBy("Year").pivot("Month").max("UV_Index").show(truncate=False)

# # avg UV Index for every year and month
# df.groupBy("Year").pivot("Month").avg("UV_Index").show(truncate=False)
df.groupBy("Territory", "Year").agg(max(col("UV_Index")).alias("max_UV_Index")).show(truncate=False)
#df.join(dfMelanoma, df["Territory"] == dfMelanoma["Territory"] , "inner").show(truncate=False)


