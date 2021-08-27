import json
import pickle
import shutil
import time
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



def generate_data():
    random.seed(1000)
    # Can set the number of rows, number of classes and number of features
    n_samples = 100
    n_features = 5
    columns = []
    for i in range(n_features):
        columns.append('v' + str(i))
    col_schema = ['v1', 'v2', 'v3', 'v4', 'v5']
    while(True):
        data = sklearn.datasets.make_classification(n_samples=n_samples, n_classes=2, n_clusters_per_class=1,
                                                    n_features=n_features, n_informative=2, n_redundant=0, n_repeated=0)
        x = data[0]
        y = data[1]
        userid = random.sample(range(1, 1000000), n_samples)
        uuids = []
        for i in range(n_samples):
            uuids.append(myuuid.uuid4().hex)

        for i in range(n_samples):
            print('sleeping')
            time.sleep(3)
            print('generating')
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
    generate_data()
