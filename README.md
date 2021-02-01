# Big data project

This is a student project whose goal is to perform batch and real time processing of data on the topic of UV radiation index and its impact on human
health in Australia. Data are taken from the official Australian Government Data website: https://data.gov.au/data/organization/australian-radiation-protection-and-nuclear-safety-agency-arpansa

# Goals of batch processing are to show:
   Maximum and average UV index per year in Australia  
   Maximum UV Index for every territory in each year  
   Average UV Index for every territory in each year  
   Maximum UV Index for every year and month  
   Month with maximum UV Index in every year  
   Territory by year, avg UV index and number of incidence  
   Territory by year, avg UV index and number of mortality  
   The five cities with the highest number of deaths caused by melanoma of skin  
   
# Goals of stream processing are to show:
   Current UV index in Novi Sad
   Maximum UV index for that day in Novi Sad
   Warning of what types of protection need to be used
   
# Docker-compose
Navigate to the docker-specification folder, open powershell and run command 'docker-compose up'

# Batch proccessing
1. Unzip datasets.zip file available on this git repository and push .cvs files to HDFS following those steps:  
  a) open powershell from Datasets folder  
  b) type 'docker cp datasets/ namenode:/home'  
  c) type 'docker exec -it namenode bash'  
  d) create folder on hdfs 'hdfs dfs -mkdir /dataset'  
  e) navigate to the 'cd home/datasets'  
  f) put .csv files on hdfs by command 'hdfs dfs -put australia_uv_index.csv /datasets/'  
  g) repeat last step for second .csv file 'hdfs dfs -put cancer_incidence_and_mortality_by_state_and_territory.csv /dataset/'  
 
 2. Enter into spark-master bash and run python file  
  a) open powershell from BigDataProject folder  
  b) type 'docker cp batch spark-master:/home'  
  c) type 'docker exec -it spark-master bash'  
  d) navigate 'cd spark/batch'  
  e) run '/spark/bin/spark-submit batch_uv_index.py'  
  
# Stream proccessing
  1. open powershell from BigDataProject folder  
  2. type 'docker cp realtime spark-master:/home  
  3. type 'docker exec -it spark-master bash'  
  4. run '$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 $SPARK_HOME/realtime/kafka_uv_index.py zoo1:2181 uv_index'  
 
