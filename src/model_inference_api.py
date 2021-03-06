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

def predict():
    current_model_path = os.path.join(app_config['root_folder'], app_config['models_sub_folder'], app_config['latest_version'],
                                      app_config['model_f_name'])

    md=pd.read_pickle(current_model_path)
    model =md['model']

    consumer = get_kafka_consumer()
    producer = get_kafka_producer()
    while (True):
        msg = consumer.poll(timeout=1.0)
        if msg is None: continue


        message = json.loads(msg.value().decode("utf-8"))
        message['y_hat']=model.predict_one(message['x'])
        producer.produce(y_cap_topic,message)
        producer.flush()

        uuid = message['uuid']
        with open(os.path.join(app_config['root_folder'],app_config['raw_data_folder'],app_config['data_sub_folder'],uuid),'w') as f:
            f.write(json.dumps(message))


def get_kafka_consumer():
    curdir = os.getcwd()
    config_dir = curdir + '/../config/config.json'
    kafka_config = json.load(open(config_dir))
    kafka_consumer_config = kafka_config
    kafka_consumer_config['group.id'] = 'prediction-x-1'
    kafka_consumer_config['auto.offset.reset'] = 'earliest'
    consumer = Consumer(kafka_consumer_config)

    tls = [TopicPartition(x_topic, 0),TopicPartition(x_topic, 1),TopicPartition(x_topic, 2),TopicPartition(x_topic, 3)]
    consumer.assign(tls)
    return consumer

def get_kafka_producer():
    curdir = os.getcwd()
    config_dir = curdir + '/../config/config.json'
    kafka_config = json.load(open(config_dir))
    kafka_config['ssl.ca.location'] = certifi.where()
    kafka_config['client.id'] = 'test-sw-1'
    producer = Producer(kafka_config)
    return producer



app_config = json.load(open("../config/app_config.json"))
x_topic='X'
y_cap_topic='X_Y_CAP'

if __name__ == '__main__':
    print('Start consuming x-data')
    predict()
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