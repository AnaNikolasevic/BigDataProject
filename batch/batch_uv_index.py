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

# //////////// READING FROM AUSTRALIA UV INDEX CSV FILE //////////////
schemaString = "timestamp Lat Lon UV_Index"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)
df = spark.read.csv("hdfs://namenode:9000/datasets/australia_uv_index.csv", header=True, mode="DROPMALFORMED", schema=schema)

# //////////// READING FROM CANCER INCIDENCE AND MORTALITY CSV FILE //////////////
schemaStringMelanoma = "Data_type Cancer_group Year Sex Territory Count Age_standardised_rate ICD10_codes"
fieldsMelanoma = [StructField(field_name, StringType(), True) for field_name in schemaStringMelanoma.split()]
schemaMelanoma = StructType(fieldsMelanoma)
dfMelanoma = spark.read.csv("hdfs://namenode:9000/datasets/cancer_incidence_and_mortality_by_state_and_territory.csv", header=True, mode="DROPMALFORMED", schema=schemaMelanoma)

# extracting year, month, day from timestamp
df = df.withColumn("Year", year(col("timestamp")))\
    .withColumn("Month", month(col("timestamp")))\
    .withColumn("Day", dayofyear(col("timestamp")))

df = df.filter(col("Year").isNotNull())
df = df.withColumn("UV_Index", col("UV_Index").cast(FloatType()))

# adding territory column depending of Lat and Lon
df = df.withColumn("Territory", expr("case when Lat = -34.04 and Lon = 151.1 then 'New South Wales' " +
                                      "when Lat = -34.92 and Lon = 138.62 then 'South Australia' " +
                                      "when Lat = -37.73 and Lon = 145.1 then 'Victoria' " +
                                      "when Lat = -27.45 and Lon = 153.03 then 'Queensland' " +
                                      "when Lat = -31.92 and Lon = 115.96 then 'Western Australia' " +
                                      "when Lat = -42.99 and Lon = 147.29 then 'Tasmania' " +
                                      "when Lat = -35.31 and Lon = 149.2 then 'Australian Capital Territory' " +
                                      "else 'Northern Territory' end"))

df.show(truncate=False)
# filtering second csv
dfMelanoma = dfMelanoma.withColumn("Year", col("Year").cast(IntegerType()))
dfMelanoma = dfMelanoma.filter(dfMelanoma["Cancer_group"] == "Melanoma of the skin")\
                        .filter((dfMelanoma["Year"]>2013) & (dfMelanoma["Year"]<2016))\
                        .filter(dfMelanoma["Sex"] == "Persons")\
                        .select("Territory", "Year", "Count", "Data_type")
dfMelanoma = dfMelanoma.withColumn("Count", col("Count").cast(FloatType()))

dfMelanomaIncidence = dfMelanoma.filter(dfMelanoma["Data_type"] == "Incidence").select("Territory", "Year", "Count", "Data_type").orderBy('Count', ascending=False)      
dfMelanomaMortality = dfMelanoma.filter(dfMelanoma["Data_type"] == "Mortality").select("Territory", "Year", "Count", "Data_type").orderBy('Count', ascending=False)                 

dfMelanomaIncidence.show(truncate=False)
dfMelanomaMortality.show(truncate=False)

#max and avg UV Index for each year in whole Australia
print("---------------------------------------------")
print("Maximum and average UV index in Australia")
dfMaxAvgAustralia = df.groupBy("Year")\
    .agg(
        max(col("UV_Index")).alias("max_UV_Index"),
        avg(col("UV_Index")).alias("avg_UV_Index"),
    )\
    .orderBy('Year', ascending=True)
dfMaxAvgAustralia.show(truncate=False)

print("---------------------------------------------")
print("Maximum UV Index for every territory in each year")
dfMaxTerritoryYear = df.groupBy("Territory").pivot("Year").max("UV_Index")
dfMaxTerritoryYear.show(truncate=False)
#dfMaxTerritoryYear.repartition(1).write.csv("hdfs://namenode:9000/results/MaxTerritoryYear.csv", sep='|')

print("---------------------------------------------")
print("Average UV Index for every territory in each year")
dfAvgTerritoryYear = df.groupBy("Territory").pivot("Year").avg("UV_Index")
dfAvgTerritoryYear.show(truncate=False)
#dfAvgTerritoryYear.repartition(1).write.csv("hdfs://namenode:9000/results/AvgTerritoryYear.csv", sep='|')

print("---------------------------------------------")
print("Maximum UV Index for every year and month")
dfMaxYearMonth = df.groupBy("Year").pivot("Month").max("UV_Index").orderBy('Year', ascending=True)
dfMaxYearMonth.show(truncate=False)
#dfMaxYearMonth.repartition(1).write.csv("hdfs://namenode:9000/results/MaxYearMonth.csv", sep='|')

# max UV Index for every year and month
print("---------------------------------------------")
print("Month with maximum UV Index in every year")
dfYearMax = df.groupBy("Year").max("UV_Index")
dfMonthMax = dfYearMax.join(df, (dfYearMax["Year"] == df["Year"]) & \
                                               (dfYearMax["max(UV_Index)"] == df["UV_Index"]) , "inner")\
                                               .select(df["Year"], "Month", "max(UV_Index)")\
                                               .orderBy('Year', ascending=True)
dfMonthMax.show(truncate=False)
#dfMonthMax.repartition(1).write.csv("hdfs://namenode:9000/results/MonthMax.csv", sep='|')

# group by territory and year
df = df.groupBy("Territory", "Year").max("UV_Index")

# join with dataset about risk and mortality
dfJoinIncidence = df.join(dfMelanomaIncidence, (df["Territory"] == dfMelanomaIncidence["Territory"]) & \
                                               (df["Year"] == dfMelanomaIncidence["Year"]) , "inner")\
                                               .select(df["Territory"], df["Year"], "max(UV_Index)", "Count", "Data_type")\
                                               .orderBy('Count', ascending=False)
dfJoinMortality = df.join(dfMelanomaMortality, (df["Territory"] == dfMelanomaMortality["Territory"]) & \
                                               (df["Year"] == dfMelanomaMortality["Year"]) , "inner")\
                                               .select(df["Territory"], df["Year"], "max(UV_Index)", "Count", "Data_type")\
                                               .orderBy('Count', ascending=False)
#dfJoinIncidence.repartition(1).write.csv("hdfs://namenode:9000/results/JoinIncidence.csv", sep='|')
#dfJoinMortality.repartition(1).write.csv("hdfs://namenode:9000/results/JoinMortality.csv", sep='|')
print("---------------------------------------------")
print("Territory by year, avg UV index and number of incidence")
dfJoinIncidence.show(truncate=False)

print("---------------------------------------------")
print("Territory by year, avg UV index and number of mortality")
dfJoinMortality.show(truncate=False)

print("---------------------------------------------")
print("The five cities with the highest number of deaths caused by melanoma of skin")
dfJoinMortalityYear = dfJoinMortality.filter(df["Year"] == "2014").orderBy('Count', ascending=False)
dfJoinMortalityYear.show(n=5,truncate=False)