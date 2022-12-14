from typing import Dict, List, Set, Tuple
from rule import Predicate, Rule, ruleRun
import pandas as pd
from utils import countByValue

Y = Predicate
PredKey = str
NegPred = Predicate

def all_structual_predicates(table:pd.DataFrame, ignore_columns:List[str] = [])->List[Predicate]:
    return [Predicate.newStruct(c) for c in table.columns if c not in ignore_columns]

def all_constant_predicates(table:pd.DataFrame, singleLine:bool, threshold:float = 0.1, ignore_columns:List[str] = [])->List[Tuple[Predicate]]:
    cps:List[Tuple[Predicate]] = []
    threshold = len(table) * threshold
    for col in table.columns:
        if col in ignore_columns:
            continue
        cnt_map = countByValue(table[col])
        for val, cnt in cnt_map.items():
            if cnt >= threshold:
                const = str(val)
                if singleLine:
                    cps.append((Predicate.newConst0(col, const)))
                else:
                    cps.append((Predicate.newConst0(col, const), Predicate.newConst1(col, const)))
    return cps

def first_generation(structual_predicates:List[Predicate])->List[Rule]:
    rules:List[Rule] = []
    for y in structual_predicates:
        for x in structual_predicates:
            if x.compatible(y):
                rules.append(Rule(Xs = [x], y = y))
    return rules

def next_generation(fathers:List[Rule], resultRules:List[Rule], structual_predicates:List[Predicate], constant_predicates:List[Tuple[Predicate]])->List[Rule]:
    # all negetive predicates of a y
    negetive_predicates_map:Dict[Y, Dict[PredKey, NegPred]] = {}
    negetive_columns_map:Dict[Y, Set[str]] = {}
    for rule in resultRules:
        negetive_predicates_set = negetive_predicates_map.get(rule.y, {})
        negetive_columns = negetive_columns_map.get(rule.y, set())
        for x in rule.Xs:
            neget = x if x.negative else x.negate()
            negetive_predicates_set[str(neget)] = neget
            for c in neget.columns:
                negetive_columns.add(c)
        negetive_predicates_map[rule.y] = negetive_predicates_set
        negetive_columns_map[rule.y] = negetive_columns

    children:List[Rule] = []
    for father in fathers:
        negetive_predicates:List[Predicate] = list(negetive_predicates_map.get(father.y, {}).values())
        negetive_columns = negetive_columns_map.get(father.y, set())
        for sp in structual_predicates:
            if father.compatible(sp) and len(negetive_columns & sp.columns) == 0:
                children.append(Rule(Xs = negetive_predicates + father.Xs + [sp], y = father.y))
        for cp in constant_predicates:
            if father.compatible(cp[0]) and len(negetive_columns & cp[0].columns) == 0:
                children.append(Rule(Xs = negetive_predicates + father.Xs + list(cp), y = father.y))
    return children

def remaining_row_size(table:pd.DataFrame, found_rules:List[Rule], singleLine:bool):
    row_size = 0
    if singleLine:
        row_size = len(table)
    else:
        if found_rules[0].sameTable:
            row_size = len(table) * (len(table) - 1)
        else:
            row_size = len(table) * len(table)
    all_negetive_predicates:Set[Predicate] = set()
    for rule in found_rules:
        for p in rule.Xs:
            if p.negative:
                all_negetive_predicates.add(p)
    rule_proxy = Rule(Xs = list(all_constant_predicates))
    ruleRun([rule_proxy], table, table)
    return row_size - rule_proxy.xSupp

if __name__ == '__main__':
    cover, confidence = 0.001, 1.0
    data = pd.read_csv("testdata/relation.csv", dtype=str)
    print(data)

    sps = all_structual_predicates(data)
    cps = all_constant_predicates(data, singleLine=False)
    rules = first_generation(sps)

    result = []
    while True:
        ruleRun(rules, data)
        fathers = []
        for rule in rules:
            if rule.ok(cover, confidence):
                result.append(rule)
            elif rule.fertile(cover):
                fathers.append(rule)
        if len(fathers) == 0:
            break
        rules = next_generation(fathers, result, sps, cps)

    for rule in result:
        print(rule)