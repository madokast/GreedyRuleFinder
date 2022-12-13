'''
2022-12-13 any feature has value 0/1
it is bad beacuse the 0 (t0.xx <> xx) is not a good rule
'''

from typing import Dict, List, Tuple
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn import tree
import sys

data = pd.read_csv("testdata/relation.csv", dtype=str)
print('origin table\n', data)

# chenge to onehot
onehotColMap:Dict[str, str] = {}
all_features:List[str] = []
onehot_id = 0
for col in data.columns:
    for val in set(data[col]):
        onehot_col = f"o{onehot_id}"
        onehot_pred = f"{col} = '{val}'"
        all_features.append(onehot_col)
        onehotColMap[onehot_col] = onehot_pred
        data[onehot_col] = (data[col] == val).astype(int)
        onehot_id += 1
print(onehotColMap)
print(all_features)
print(data)

# decision-tree
features, target = all_features[:-1], all_features[-1]
print(f"features = {features}, targert = {onehotColMap[target]}")
dt = tree.DecisionTreeClassifier(max_depth=10)
dt.fit(data[features], data[target])
dt_text = tree.export_text(dt, feature_names = features)
print("dt text", dt_text, sep='\n')




