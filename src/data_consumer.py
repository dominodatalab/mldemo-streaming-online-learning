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





def gen_data(data_folder,truth_folder):
    #random.seed(100)
    # Can set the number of rows, number of classes and number of features
    n_samples = 100
    n_features = 5
    columns = []
    for i in range(n_features):
        columns.append('v' + str(i))
    col_schema = ['v1', 'v2', 'v3', 'v4', 'v5']
    data = sklearn.datasets.make_classification(n_samples=n_samples, n_classes=2, n_clusters_per_class=1,
                                                n_features=n_features, n_informative=2, n_redundant=0, n_repeated=0)
    x = data[0]
    y = data[1]
    userid = random.sample(range(1, 1000000), n_samples)
    uuids = []
    for i in range(n_samples):
        uuids.append(myuuid.uuid4().hex)

    for i in range(n_samples):
        uuid = uuids[i]
        y_data= str(y[i])
        x_data= x[i]
        u_id = str(userid[i])
        x_d = {}
        for j in range(n_features):
            x_d['v' + str(j)] = x_data[j]
        data = {'x':x_d,'y_cap':'','uuid':uuid , 'uid':u_id}
        #write_to_file(data_folder,uuid,data)

        data = {'y':y_data,'uuid':uuid , 'uid':u_id}
        #write_to_file(truth_folder, uuid, data)



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


def create_initial_data():
    initialize()
    random.seed(100)
    # Can set the number of rows, number of classes and number of features
    n_samples = 100
    n_features = 5
    columns = []
    for i in range(n_features):
        columns.append('v' + str(i))
    col_schema = ['v1', 'v2', 'v3', 'v4', 'v5']
    data = sklearn.datasets.make_classification(n_samples=n_samples, n_classes=2, n_clusters_per_class=1,
                                                n_features=n_features, n_informative=2, n_redundant=0, n_repeated=0)
    x = data[0]
    y = data[1]
    userid = random.sample(range(1, 1000000), n_samples)
    uuids = []
    for i in range(n_samples):
        uuids.append(myuuid.uuid4().hex)

    for i in range(n_samples):
        uuid = uuids[i]
        y_data= str(y[i])
        x_data= x[i]
        u_id = str(userid[i])
        x_d = {}
        for j in range(n_features):
            x_d['v' + str(j)] = x_data[j]
        data = {'x':x_d,'y_cap':'','uuid':uuid , 'uid':u_id}
        with open(os.path.join(app_config['root_folder'],app_config['raw_data_folder'],app_config['data_sub_folder'],uuid),'w') as f:
            f.write(json.dumps(data))

        data = {'y':y_data,'uuid':uuid , 'uid':u_id}
        with open(os.path.join(app_config['root_folder'],app_config['raw_data_folder'],app_config['truth_sub_folder'],uuid),'w') as f:
            f.write(json.dumps(data))

# Press the green button in the gutter to run the script.

app_config = json.load(open("../config/app_config.json"))

if __name__ == '__main__':
    print(app_config)
    create_initial_data()
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