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
    print(timeT)
    response = requests.get('https://api.openuv.io/api/v1/uv?lat=45.25&lng=19.77&dt=' + timeT, headers={'x-access-token': '5edef63e979bb094034b4b622a25e71e '})
    #responseJSON = response.json()['data']['result']['uv']
    responseJSON = json.loads(response.text)
    print(responseJSON)
    err= responseJSON['result']['uv']
    producer.send(TOPIC, key=bytes(timeT, 'utf-8'), value=bytes(str(err), 'utf-8'))
    t.sleep(60)