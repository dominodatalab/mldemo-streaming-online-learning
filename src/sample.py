from pprint import pprint
import pandas as pd
import sklearn.datasets
from river import datasets
from river import compose
from river import linear_model
from river import metrics
from river import preprocessing


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
