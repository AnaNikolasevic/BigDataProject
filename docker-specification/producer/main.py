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

    response = requests.get('https://api.openuv.io/api/v1/uv?lat=45.25&lng=19.77&dt=' + timeT, headers={'x-access-token': 'b099d5831f80f718c6c6bbd9611fbebe '})
    responseJSON = json.loads(response.text)
    r = str(responseJSON['result']['uv']) + " " + str(responseJSON['result']['uv_max_time']) + " " + str(responseJSON['result']['uv_max']) + " " + "NoviSad"

    print(r)
    
    producer.send(TOPIC, key=bytes(timeT, 'utf-8'), value=bytes(r, 'utf-8'))
    t.sleep(5)