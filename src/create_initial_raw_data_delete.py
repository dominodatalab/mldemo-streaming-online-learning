import json

import pandas as pd
import sklearn.datasets
import random
import os
import uuid
import uuid as myuuid



def gen_data_sample():
    random.seed(100)
    print('start')
    # Can set the number of rows, number of classes and number of features
    n_samples = 30
    n_features = 5
    columns = []
    for i in range(n_features):
        columns.append('v'+ str(i))
    col_schema = ['v1', 'v2', 'v3' , 'v4', 'v5']
    data = sklearn.datasets.make_classification(n_samples=n_samples, n_classes=2,n_clusters_per_class=1,
                                                n_features=n_features,n_informative=2, n_redundant=0, n_repeated=0)
    print(random.sample(range(1, 1000000), n_samples))
    x = data[0]
    y = data[1]
    id = random.sample(range(1, 1000000), n_samples)
    uuids = []
    for i in range(n_samples):
        message_id = uuid.uuid4().hex
        print(message_id)
        uuids.append(message_id)
    df = pd.DataFrame(data[0],columns=col_schema)
    df['label'] = data[1]
    df['user_id'] = id
    df['uuid'] = uuids
    print(df.head())

    #Now push to Kafka
    #Produce the X values
    #At a 1 minute delay publish Y (Separate thread). Produce data once every 1 second

def gen_data(data_folder,truth_folder):
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
    print(len(x))
    y = data[1]
    userid = random.sample(range(1, 1000000), n_samples)
    uuids = []
    for i in range(n_samples):
        uuids.append(myuuid.uuid4().hex)

    for i in range(n_samples):
        uuid = uuids[i]
        y_data= str(y[i])
        print(i)
        print(x[i])
        x_data= x[i]
        u_id = str(userid[i])
        x_d = {}
        for j in range(n_features):
            x_d['v' + str(j)] = x_data[j]
        data = {'x':x_d,'y_cap':'','uuid':uuid , 'uid':u_id}
        write_to_file(data_folder,uuid,data)

        data = {'y':y_data,'uuid':uuid , 'uid':u_id}
        write_to_file(truth_folder, uuid, data)


def write_to_file(folder,uuid,data):
    path = folder + '/' + uuid
    obj = open(path, 'w')
    print(data)
    str=json.dumps(data)
    print(str)
    obj.write(str)
    obj.close()

def create_training_dataset():
    pass
# Press the green button in the gutter to run the script.
root_folder= '/Users/sameerwadkar/domino_demos/online_learning/'
data_sub_folder = 'raw_data/data'
truth_sub_folder = 'raw_data/truth'
if __name__ == '__main__':
    #os.mkdir(root_folder)

    data_folder = root_folder + data_sub_folder
    truth_folder = root_folder + truth_sub_folder

    isExist = os.path.exists(data_folder)
    if not isExist:
        os.mkdir(data_folder)

    isExist = os.path.exists(truth_sub_folder)
    if not isExist:
        os.mkdir(truth_folder)

    gen_data(data_folder,truth_folder)