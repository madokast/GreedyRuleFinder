'''
2022-12-13 learn how to do decision-tree in sklearn
'''

from typing import Dict, List, Tuple
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn import datasets
from sklearn.preprocessing import LabelEncoder
from sklearn import tree
import sys, os

data = pd.read_csv("testdata/relation.csv", dtype=str)
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
featrues, target = list(all_cols[:-1]), str(all_cols[-1])
print(f"featrues = {featrues}, targert = {target}")
dt = tree.DecisionTreeClassifier(max_depth=10)
dt.fit(data[featrues], data[target])
dt_text = tree.export_text(dt, feature_names = featrues)
print("dt text", dt_text, sep='\n')

# convert dt_text to REE
'''
|--- ac <= 1.50
|   |--- weights: [2.00, 0.00, 2.00, 0.00] class: 0
|--- ac >  1.50
|   |--- weights: [0.00, 3.00, 0.00, 1.00] class: 1
'''

rowSize = len(data)
rules:List[Tuple[List[str], str]] = []
curPred:List[str] = []
leading = "|--- "
for line in dt_text.split('\n'):
    if len(line) < len(leading):
        continue
    pred = line[line.index(leading) + len(leading):]
    if "class: " in pred:
        typeid = int(pred[len("class: "):])
        ori = encoders[target].inverse_transform((typeid,))[0]
        rules.append((curPred[:], ori))
    else:
        depth = line.count('|')
        curPred = curPred[:depth-1] + [pred]

for rule in rules:
    print(rule)


