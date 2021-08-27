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


def create_empty_model():
    model = compose.Pipeline(preprocessing.StandardScaler(),linear_model.LogisticRegression())
    acc_metric = metrics.Accuracy()
    recall_metric = metrics.Recall()

    current_model_path = os.path.join(app_config['root_folder'], app_config['models_sub_folder'], app_config['latest_version'],
                                      app_config['model_f_name'])
    pd.to_pickle({'model':model,'accuracy':acc_metric,'recall':recall_metric, 'version':0},current_model_path)


def initialize():
    try:
        shutil.rmtree(os.path.join(os.path.join(app_config['root_folder'], app_config['models_sub_folder'])))
    except:
        pass
    try:
        shutil.rmtree(os.path.join(os.path.join(app_config['root_folder'], app_config['datasets_sub_folder'])))
    except:
        pass
    try:
        shutil.rmtree(os.path.join(os.path.join(app_config['root_folder'], app_config['raw_data_folder'], app_config['data_sub_folder'])))
    except:
        pass
    try:
        shutil.rmtree(os.path.join(os.path.join(app_config['root_folder'],app_config['raw_data_folder'], app_config['truth_sub_folder'])))
    except:
        pass

    try:
        os.remove(os.path.join(os.path.join(app_config['root_folder'], app_config['dataset_metadata_config_file'])))
    except:
        pass
    try:
        os.remove(os.path.join(os.path.join(app_config['root_folder'], app_config['model_metadata_config_file'])))
    except:
        pass

    os.mkdir(os.path.join(os.path.join(app_config['root_folder'],app_config['raw_data_folder'], app_config['data_sub_folder'])))
    os.mkdir(os.path.join(os.path.join(app_config['root_folder'],app_config['raw_data_folder'], app_config['truth_sub_folder'])))
    os.mkdir(os.path.join(os.path.join(app_config['root_folder'], app_config['models_sub_folder'])))
    os.mkdir(os.path.join(os.path.join(app_config['root_folder'], app_config['models_sub_folder'],app_config['latest_version'])))
    os.mkdir(os.path.join(os.path.join(app_config['root_folder'], app_config['datasets_sub_folder'])))
    os.mkdir(os.path.join(os.path.join(app_config['root_folder'], app_config['datasets_sub_folder'],app_config['latest_version'])))
    dataset_version={"version":0}
    model_version = {"version": 0}

    with open(os.path.join(app_config['root_folder'],app_config['dataset_metadata_config_file']),'w', encoding='utf-8') as f:
        json.dump(dataset_version,f)
    with open(os.path.join(app_config['root_folder'],app_config['model_metadata_config_file']),'w', encoding='utf-8') as f:
        json.dump(model_version,f)
    create_empty_model()

def publish_data():
    model = compose.Pipeline(preprocessing.StandardScaler(), linear_model.LogisticRegression())

    acc_metric = metrics.Accuracy()
    producer = get_kafka_producer()
    random.seed(100)
    # Can set the number of rows, number of classes and number of features
    n_samples = 1000000
    n_features = 2
    columns = []
    for i in range(n_features):
        columns.append('v' + str(i))
    col_schema = ['v1', 'v2', 'v3', 'v4', 'v5']
    data = sklearn.datasets.make_classification(n_samples=n_samples, n_classes=2, n_clusters_per_class=1,
                                                n_features=n_features, n_informative=2, n_redundant=0, n_repeated=0)
    x = data[0]
    y = data[1]
    print(y)
    userid = []
    for i in range(1,10000):
        userid.append(random.randint(1,10000))

    uuids = []
    for i in range(n_samples):
        uuids.append(myuuid.uuid4().hex)

    for i in range(n_samples):
        uuid = uuids[i]
        y_data= False
        if(y[i]==1):
            y_data=True
        x_data= x[i]
        u_id = str(userid[i])
        x_d = {}
        for j in range(n_features):
            x_d['v' + str(j)] = x_data[j]
        print(x_d)
        data = {'x':x_d,'y_hat':'','uuid':uuid , 'uid':u_id, 'y_real':y_data}
        print(x_d)

        y_pred = model.predict_one(x_d)
        print(str(y_data) + "-" + str(y_pred))
        model = model.learn_one(x_d,y_data)

        acc_metric = acc_metric.update(y_data, y_pred)
        print(acc_metric)
        #with open(os.path.join(app_config['root_folder'],app_config['raw_data_folder'],app_config['data_sub_folder'],uuid),'w') as f:
            #f.write(json.dumps(data))
        producer.produce(x_topic, value=json.dumps(data).encode('utf-8'), key=uuid)
        producer.flush()
        #Delay in producing y value. Truth arrives later
        time.sleep(1)
        data = {'y':y_data,'uuid':uuid , 'uid':u_id}
        #with open(os.path.join(app_config['root_folder'],app_config['raw_data_folder'],app_config['truth_sub_folder'],uuid),'w') as f:
            #v = json.dumps(data).encode('utf-8')
        producer.produce(y_topic, value=json.dumps(data).encode('utf-8'), key=uuid)
        producer.flush()

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
y_topic='Y'
y_hat_topic='X_Y_HAT'

if __name__ == '__main__':
    print('Starting Data Publish')
    initialize()
    publish_data()
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