import json
import pickle

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


##This has to happen in a single transaction. This implementation is an approximation
def create_versioned_data(config_file,truth_folder,data_folder):
    config = json.load(open(config_file))
    version = config['version']
    truth_files = os.listdir(truth_folder);
    files_to_delete = []
    dataset = []
    for f in truth_files:
        truth = json.load(open(truth_folder+f))
        if(os.path.exists(data_folder+f)):
            data = json.load(open(data_folder+f))
            data['y']=truth['y']
            files_to_delete.append(f)
            dataset.append(data)
    ##Create dataset folder
    ds_folder = os.path.join(root_folder,datasets_sub_folder,'/v'+str(version))
    is_exist = os.path.exists(ds_folder)
    if is_exist:
        os.remove(ds_folder)
    os.mkdir(ds_folder)
    versioned_dataset = {'data':dataset}
    with open(os.path.join(ds_folder,dataset_file_name), 'w') as f:
        json.dump(versioned_dataset, f)

    for f in files_to_delete:
        os.remove(os.path.join(data_folder,f))
        os.remove(os.path.join(truth_folder, f))
    config['version']=version+1
    os.remove(config_file)
    with open(config_file, 'w') as f:
        json.dump(config, f)
    ##Copy all records here to a file
        ##Train model
        ##Save model
        ##Update config by incrementing version number
        ##Delete processed records
        ##All jobs constantly monitor config (once every 10 seconds to determine if new model is available)

def create_empty_model():
    model = compose.Pipeline(preprocessing.StandardScaler(),linear_model.LogisticRegression())
    acc_metric = metrics.Accuracy()
    precision_metric = metrics.Precision()
    print('XXXXXXXXXXX')
    pd.to_pickle({'model':model,'version':0},os.path.join(root_folder,models_sub_folder,'current', model_f_name))
    pd.to_pickle({'accuracy':acc_metric,'precision':precision_metric,'version':0}, os.path.join(root_folder,metrics_sub_folder,'current',metrics_f_name))


def save_versioned_model(version,model,accuracy,precision):
    model = compose.Pipeline(preprocessing.StandardScaler(),linear_model.LogisticRegression())
    acc_metric = metrics.Accuracy()
    precision_metric = metrics.Precision()

    version_folder ='v' + str(version)

    pd.to_pickle({'model':model,'version':version},os.path.join(root_folder,models_sub_folder,version_folder,model_f_name))
    pd.to_pickle({'accuracy':acc_metric,'precision':precision_metric,'version':version}, os.path.join(root_folder,metrics_sub_folder,version_folder,metrics_f_name))

    version_folder = 'current'
    pd.to_pickle({'model':model,'version':version},os.path.join(root_folder,models_sub_folder,version_folder,model_f_name))
    pd.to_pickle({'accuracy':acc_metric,'precision':precision_metric,'version':version}, os.path.join(root_folder,metrics_sub_folder,version_folder,metrics_f_name))

def create_versioned_model():
    #Get current model version. If 0 this is the first model. If not zero create empty model and read from pickle file
    config = json.load(open(os.path.join(root_folder,'config','model_metadata.json')))
    version = config['version']
    version_folder = 'v' + str(version)
    if not os.path.exists(os.path.join(root_folder,models_sub_folder,version_folder)):
        os.mkdir(os.path.join(root_folder,models_sub_folder,version_folder))

    if not os.path.exists(os.path.join(root_folder,metrics_sub_folder,version_folder)):
        os.mkdir(os.path.join(root_folder,metrics_sub_folder,version_folder))
    if(version==0):
        create_empty_model()
    model = pd.read_pickle(os.path.join(root_folder, models_sub_folder, 'current', model_f_name))['model']
    metric = pd.read_pickle(os.path.join(root_folder, metrics_sub_folder, 'current', metrics_f_name))
    acc_metric=metric['accuracy']
    precision_metric=metric['precision']
    #Read the current model and metrics
    #Get corresponding dataset version if it exists. If not return
    ds = os.path.join(root_folder, datasets_sub_folder, 'v' + str(version),dataset_file_name)
    print(ds)
    print(root_folder)
    dataset=[]
    with open(ds,'r') as f:
        dataset = json.load(f)['data']

    for d in dataset:
        model = model.learn_one(d['x'], d['y'])
        acc_metric.update(d['y'],d['y'])
        precision_metric.update(d['y'],d['y'])
    print(acc_metric.get())
    print(precision_metric.get())
    save_versioned_model(version,model,acc_metric,precision_metric)
    #Train model and update current metrics
    #Save model to verion and current . Save metrics to version and current
    #Update version

    '''
    config['version'] = version + 1
    os.remove(config_file)
    with open(config_file, 'w') as f:
        json.dump(config, f)
    '''
    return
# Press the green button in the gutter to run the script.

root_folder= '/Users/sameerwadkar/domino_demos/online_learning/'
data_sub_folder = 'raw_data/data/'
truth_sub_folder = 'raw_data/truth/'
models_sub_folder = 'models'
metrics_sub_folder = 'metrics'
dataset_metadata_config_file = 'config/model_metadata.json'
model_metadata_config_file = 'config/model_metadata.json'
datasets_sub_folder = 'datasets'
dataset_file_name = 'dataset.json'
model_f_name='model.pkl'
metrics_f_name='metrics.pkl'


if __name__ == '__main__':
    data_folder = root_folder + data_sub_folder
    truth_folder = root_folder + truth_sub_folder
    datasets_folder = root_folder + datasets_sub_folder
    dataset_metadata_config = root_folder + dataset_metadata_config_file
    model_metadata_config = root_folder + model_metadata_config_file
    isExist = os.path.exists(data_folder)
    #gen_data(data_folder,truth_folder)
    #create_versioned_data(dataset_metadata_config,truth_folder,data_folder,datasets_folder)
    create_versioned_model()