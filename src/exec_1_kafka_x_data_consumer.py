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


def predict(x, version='current'):
    app_config = json.load(open("../config/app_config.json"))
    current_model_path = os.path.join(app_config['root_folder'], app_config['models_sub_folder'],version,
                                      app_config['model_f_name'])
    md=pd.read_pickle(current_model_path)
    model = md['model']
    return model.predict_one(x)

def consume_x_data():
    st_time = time.time()
    current_model_path = os.path.join(app_config['root_folder'], app_config['models_sub_folder'], app_config['latest_version'],
                                      app_config['model_f_name'])
    md=pd.read_pickle(current_model_path)
    model = md['model']

    consumer = get_kafka_consumer()
    producer = get_kafka_producer()

    while (True):
        msg = consumer.poll(timeout=1.0)
        if msg is None: continue
        message = json.loads(msg.value().decode("utf-8"))

        #Predict here
        print(message['x'])
        val = model.predict_one(message['x'])
        print(val)
        message['y_hat'] = val
        print(message)
        print(message)
        producer.produce(y_hat_topic,json.dumps(message).encode('utf-8'))
        producer.flush()

        uuid = message['uuid']
        with open(os.path.join(app_config['root_folder'],app_config['raw_data_folder'],app_config['data_sub_folder'],uuid),'w') as f:
            f.write(json.dumps(message))

        current_time = time.time()
        #Refresh model every 10 seconds
        if(current_time-st_time)>10:
            print('refreshing model')
            current_model_path = os.path.join(app_config['root_folder'], app_config['models_sub_folder'],
                                              app_config['latest_version'],
                                              app_config['model_f_name'])
            md = pd.read_pickle(current_model_path)
            model = md['model']
            print('Current Model Version ' + str(md['version']))
            st_time = current_time


def get_kafka_producer():
    curdir = os.getcwd()
    config_dir = curdir + '/../config/config.json'
    kafka_config = json.load(open(config_dir))
    kafka_config['ssl.ca.location'] = certifi.where()
    kafka_config['client.id'] = 'test-sw-1'
    producer = Producer(kafka_config)
    return producer

def get_kafka_consumer():
    curdir = os.getcwd()
    config_dir = curdir + '/../config/config.json'
    kafka_config = json.load(open(config_dir))
    kafka_consumer_config = kafka_config
    kafka_consumer_config['group.id'] = 'raw-consumer-x-4'
    kafka_consumer_config['auto.offset.reset'] = 'latest'
    consumer = Consumer(kafka_consumer_config)

    tls = [TopicPartition(x_topic, 0),TopicPartition(x_topic, 1),TopicPartition(x_topic, 2),TopicPartition(x_topic, 3)]
    consumer.assign(tls)
    return consumer

app_config = json.load(open("../config/app_config.json"))
x_topic='X'
y_topic='Y'
y_hat_topic='X_Y_HAT'

if __name__ == '__main__':
    print('Start consuming x-data')
    consume_x_data()
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