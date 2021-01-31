# Big data project

This is a student project whose goal is to perform batch and real time processing of data on the topic of UV radiation index and its impact on human
health in Australia.

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
