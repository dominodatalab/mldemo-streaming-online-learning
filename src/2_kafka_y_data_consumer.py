import json
import pickle
import shutil

import pandas as pd
import sklearn.datasets
import random
import os
import uuid
import uuid as myuuid
from river import compose
from river import linear_model
from river import metrics
from river import preprocessing

import time
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError, TopicPartition
import  certifi

def consume_y_data():
    consumer = get_kafka_consumer()
    while (True):
        msg = consumer.poll(timeout=1.0)
        if msg is None: continue
        message = json.loads(msg.value().decode("utf-8"))
        uuid = message['uuid']
        with open(os.path.join(app_config['root_folder'],app_config['raw_data_folder'],app_config['truth_sub_folder'],uuid),'w') as f:
            f.write(json.dumps(message))

def get_kafka_consumer():
    curdir = os.getcwd()
    config_dir = curdir + '/../config/config.json'
    kafka_config = json.load(open(config_dir))
    kafka_consumer_config = kafka_config
    kafka_consumer_config['group.id'] = 'raw-consumer-y-4'
    kafka_consumer_config['auto.offset.reset'] = 'earliest'
    consumer = Consumer(kafka_consumer_config)

    tls = [TopicPartition(y_topic, 0),TopicPartition(y_topic, 1),TopicPartition(y_topic, 2),TopicPartition(y_topic, 3)]
    consumer.assign(tls)
    return consumer

app_config = json.load(open("../config/app_config.json"))
x_topic='X'
y_topic='Y'
y_hat_topic='X_Y_HAT'

if __name__ == '__main__':
    print('start consuming y')
    consume_y_data()
    '''
    data_folder = root_folder + data_sub_folder
    truth_folder = root_folder + truth_sub_folder
    datasets_folder = root_folder + datasets_sub_folder
    dataset_metadata_config = root_folder + dataset_metadata_config_file
    model_metadata_config = root_folder + model_metadata_config_file
    print('Initializing')
    initialize()
    '''
    #create_initial_data()
    #gen_data(data_folder,truth_folder)
    #create_versioned_data(dataset_metadata_config,truth_folder,data_folder,datasets_folder)
    #create_versioned_model()