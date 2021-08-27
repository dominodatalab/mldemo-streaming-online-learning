import json

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




##This has to happen in a single transaction. This implementation is an approximation
def create_versioned_data():
    config = {}
    config_file = os.path.join(app_config['root_folder'], app_config['dataset_metadata_config_file'])
    with open(config_file, 'r', encoding='utf-8') as f:
        config = json.load(f)
    version = config['version']
    files_to_delete = []
    dataset = []
    truth_folder = os.path.join(app_config['root_folder'], app_config['raw_data_folder'],app_config['truth_sub_folder'])
    data_folder = os.path.join(app_config['root_folder'], app_config['raw_data_folder'],app_config['data_sub_folder'])
    truth_files = os.listdir(os.path.join(truth_folder))

    for f in truth_files:
        with open(os.path.join(truth_folder,f)) as fd:
            truth = json.load(fd)
        df = os.path.join(data_folder,f)
        if(os.path.exists(df)):
            with open(df) as fd:
                data = json.load(fd)
            data['y']=truth['y']
            files_to_delete.append(f)
            dataset.append(data)
    ##Create dataset folder
    ds_folder = os.path.join(app_config['root_folder'],app_config['datasets_sub_folder'],'v'+str(version))
    is_exist = os.path.exists(ds_folder)
    if is_exist:
        shutil.rmtree(ds_folder)
    os.mkdir(ds_folder)
    versioned_dataset = {'data':dataset}
    with open(os.path.join(ds_folder,app_config['dataset_file_name']), 'w') as f:
        json.dump(versioned_dataset, f)
    print('Deleting files #' + str(len(files_to_delete)))
    for f in files_to_delete:
        os.remove(os.path.join(data_folder,f))
        os.remove(os.path.join(truth_folder, f))
        print(f)
    config['version']=version+1
    #os.remove(config_file)
    with open(config_file, 'w') as f:
        json.dump(config, f)




def save_versioned_model(version,model,accuracy,recall):
    version_folder ='v' + str(version)
    versioned_model_path = os.path.join(app_config['root_folder'], app_config['models_sub_folder'], version_folder,
                                      app_config['model_f_name'])
    m={'model':model,'accuracy':accuracy,'recall':recall, 'version':version}
    pd.to_pickle(m,versioned_model_path)

    current_model_path = os.path.join(app_config['root_folder'], app_config['models_sub_folder'], app_config['latest_version'],
                                      app_config['model_f_name'])
    pd.to_pickle(m, current_model_path)

def create_versioned_model():
    #Get current model version. If 0 this is the first model. If not zero create empty model and read from pickle file
    config_file = os.path.join(app_config['root_folder'],app_config['model_metadata_config_file'])
    config = json.load(open(config_file))
    version = config['version']
    version_folder = 'v' + str(version)

    models_folder = os.path.join(app_config['root_folder'],app_config['models_sub_folder'],version_folder)
    if not os.path.exists(models_folder):
        os.mkdir(models_folder)
    print(version)
    current_model_path = os.path.join(app_config['root_folder'], app_config['models_sub_folder'], app_config['latest_version'],
                                      app_config['model_f_name'])

    md=pd.read_pickle(current_model_path)
    model =md['model']
    acc_metric=md['accuracy']
    recall_metric=md['recall']

    new_model = model.clone()
    #Read the current model and metrics
    #Get corresponding dataset version if it exists. If not return
    ds = os.path.join(app_config['root_folder'], app_config['datasets_sub_folder'], 'v' + str(version),app_config['dataset_file_name'])
    dataset=[]
    with open(ds,'r') as f:
        dataset = json.load(f)['data']

    for d in dataset:
        val = False
        if(d['y']=='1'):
            val = True
        new_model = new_model.learn_one(d['x'], val)
        print(str(d['y']) + '-' + str(d['y_hat']))
        acc_metric.update(d['y'],d['y_hat'])
        recall_metric.update(d['y'],d['y_hat'])

    print(acc_metric.get())
    print(recall_metric.get())

    with open('../dominostats.json', 'w') as f:
        f.write(json.dumps({"Accuracy": acc_metric.get(), "Recall": recall_metric.get()}))
    save_versioned_model(version,new_model,acc_metric,recall_metric)
    #Train model and update current metrics
    #Save model to verion and current . Save metrics to version and current
    #Update version


    config['version'] = version + 1
    #os.remove(config_file)
    with open(config_file, 'w') as f:
        json.dump(config, f)

    return
# Press the green button in the gutter to run the script.

app_config = json.load(open("../config/app_config.json"))

def execute():
    create_versioned_data()
    create_versioned_model()   

if __name__ == '__main__':
    execute()

