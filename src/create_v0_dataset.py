import pandas as pd
import sklearn.datasets
import random

def gen_data():
    random.seed(100)
    print('start')
    # Can set the number of rows, number of classes and number of features
    n_samples = 10
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

    df = pd.DataFrame(data[0],columns=col_schema)
    df['label'] = data[1]
    df['id'] = id

    #Now push to Kafka
    #Produce the X values
    #At a 1 minute delay publish Y (Separate thread). Produce data once every 1 second


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    gen_data()