'''
2022-12-13 learn how to do decision-tree in sklearn
'''

from typing import Dict
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn import datasets
from sklearn.preprocessing import LabelEncoder
from sklearn import  tree
import sys

data = pd.read_csv("testdata/relation.csv")
print('origin table\n', data)

# encode
encoders:Dict[str, LabelEncoder] = {}
for col in data.columns:
    en = LabelEncoder()
    data[col] = en.fit_transform(data[col])
    encoders[col] = en
    print(f"encode {col} = {en.classes_}")
print('encode\n' ,data)

# decode test
if False:
    for col in data.columns:
        data[col] = encoders[col].inverse_transform(data[col])
    print('test decode\n', data)
    sys.exit(0)

# decision-tree for
all_cols = data.columns
featrues, target = list(all_cols[:-1]), all_cols[-1]
print(f"featrues = {featrues}, targert = {target}")
dt = tree.DecisionTreeClassifier()
dt.fit(data[featrues], data[target])
dt_text = tree.export_text(dt, feature_names = featrues)
print("dt text", dt_text, sep='\n')

# convert dt_text to REE
'''
|--- ac <= 1.50
|   |--- cc <= 0.50
|   |   |--- class: 0
|   |--- cc >  0.50
|   |   |--- class: 2
|--- ac >  1.50
|   |--- pn <= 2.00
|   |   |--- class: 1
|   |--- pn >  2.00
|   |   |--- class: 3
'''
rules = []
for line in dt_text.split('\n'):
    if line.startswith("|---"):
        pass # a new empty rule
    else:
        pass # append pred to rules[-1]

