import json
import os

import time
import random

from confluent_kafka import Producer, Consumer, KafkaException, KafkaError, TopicPartition
import  certifi

curdir = os.getcwd()
print('---')
print(curdir)
config_dir=curdir+'/../config/config.json'
print(config_dir)
kafka_config = json.load(open(config_dir))
kafka_config['ssl.ca.location']= certifi.where()
kafka_config['client.id']= 'test-sw-1'

print(kafka_config)

producer = Producer(kafka_config)
topic = 'test_topic'

v = json.dumps({'test':str(random.randint(1,10))}).encode('utf-8')
producer.produce(topic, value=v, key=str(1))
producer.flush()


kafka_consumer_config = kafka_config
kafka_consumer_config['group.id']='grp-1'
kafka_consumer_config['auto.offset.reset']='latest'

consumer = Consumer(kafka_consumer_config)

tls = [TopicPartition(topic, 0)]
print(tls)
consumer.assign(tls)
cnt = 0
while (True):
    print('test')
    msg = consumer.poll(timeout=1.0)
    print(msg)
    if msg is None: continue
    message = json.loads(msg.value().decode("utf-8"))
    print(message)
    time.sleep(1)
    cnt=cnt + 1
    if(cnt>10):
        break
consumer.close()