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

data = pd.read_csv("testdata/relation.csv")
print('origin table\n', data)

# encode
encoders:Dict[str, LabelEncoder] = {}
for col in data.columns:
    en = LabelEncoder()
    data[col] = en.fit_transform(data[col])
    encoders[col] = en
print('encode\n' ,data)

# decode test
for col in data.columns:
    data[col] = encoders[col].inverse_transform(data[col])
print('test decode\n', data)


