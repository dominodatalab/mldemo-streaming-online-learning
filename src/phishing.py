from pprint import pprint
import pandas as pd
import sklearn.datasets
from river import datasets
from river import compose
from river import linear_model
from river import metrics
from river import preprocessing
import pickle
'''
dataset = datasets.Phishing()
print(dataset.n_samples)
model = compose.Pipeline(
     preprocessing.StandardScaler(),
    linear_model.LogisticRegression()
)
metric = metrics.Accuracy()
for x, y in dataset:
    y_pred = model.predict_one(x)  # make a prediction
    print(y_pred)
    metric = metric.update(y, y_pred)
    model = model.learn_one(x, y)

print(metric)
'''

data = sklearn.datasets.make_classification(n_samples=2000, n_classes=2, n_clusters_per_class=1,
                                            n_features=2, n_informative=2, n_redundant=0, n_repeated=0)
x = data[0]
y = data[1]
print(x)
model = compose.Pipeline(
     preprocessing.StandardScaler(),
    linear_model.LogisticRegression()
)
metric = metrics.Accuracy()
for i in range(2000):
    x_d = {}
    for j in range(2):
        x_d['v' + str(j)] = x[i][j]
    print(x_d)
    y_pred = model.predict_one(x_d)  # make a prediction
    print(x_d)
    print(y[i])
    print(y_pred)
    v = False
    if(y[i]==1):
        v=True
    metric = metric.update(v, y_pred)

    model = model.learn_one(x_d, y[i])
    if(i%10==0):
        print('XXXXXX')
        pd.to_pickle({'model': model, 'accuracy': metric, 'version': 0}, '/tmp/models/model.pkl')
        pd.to_pickle({'model': model, 'accuracy': metric, 'version': 0}, '/tmp/models/model' + str(i))
        md = pd.read_pickle('/tmp/model.pkl')
        model = md['model']
        metric = md['accuracy']
    print(metric)
print(metric)
#for x, y in dataset:
#    pprint(x)
#    print(y)
