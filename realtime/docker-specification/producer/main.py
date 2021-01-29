#!/usr/bin/python3

import os
import time
import praw
from kafka import KafkaProducer
import kafka.errors
import requests
import time as t
from datetime import datetime
import json

KAFKA_BROKER = os.environ["KAFKA_BROKER"]
TOPIC = "uv_index"

while True:
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER.split(","))
        print("Connected to Kafkaa!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)

while True:
    print("Entry")
    time = datetime.now()
    timeT = str(time).replace(" ", "T")
    timeT = timeT[:-3]
    timeT += 'Z'

    response = requests.get('https://api.openuv.io/api/v1/uv?lat=45.25&lng=19.77&dt=' + timeT, headers={'x-access-token': '4c2d13dad2f7f82c029078546a246bdd '})
    responseJSON = json.loads(response.text)
    uv = "UV index: " + str(responseJSON['result']['uv']) + ", " + "UV index max: " + str(responseJSON['result']['uv_max'])

    print(uv)
    
    producer.send(TOPIC, key=bytes(timeT, 'utf-8'), value=bytes(uv, 'utf-8'))
    t.sleep(15)