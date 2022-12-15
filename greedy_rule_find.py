from typing import Dict, List, Set, Tuple
from rule import Predicate, Rule, RuleExecutor
import pandas as pd
from utils import countByValue, foreach, groupByKey

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

def next_generation(fathers:List[Rule], resultRules:List[Rule], structual_predicates:List[Predicate], constant_predicates:List[Tuple[Predicate]], re:RuleExecutor, cover:float)->List[Rule]:
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
    
    no_create_children_Y:Set[Y] = set()
    for y, neg_preds in negetive_predicates_map.items():
        negetive_predicates = list(neg_preds.values())
        rule_proxy = Rule(Xs = negetive_predicates, y = y)
        re.execute([rule_proxy])
        if rule_proxy.cover() < cover:
            no_create_children_Y.add(y)

    generation = fathers[0].generation + 1
    children:List[Rule] = []
    for father in fathers:
        if father.y in no_create_children_Y:
            continue
        negetive_predicates:List[Predicate] = list(negetive_predicates_map.get(father.y, {}).values())
        negetive_columns = negetive_columns_map.get(father.y, set())
        for sp in structual_predicates:
            if father.compatible(sp) and len(negetive_columns & sp.columns) == 0:
                children.append(Rule(Xs = negetive_predicates + father.Xs + [sp], y = father.y, generation = generation))
        for cp in constant_predicates:
            if father.compatible(cp[0]) and len(negetive_columns & cp[0].columns) == 0:
                children.append(Rule(Xs = negetive_predicates + father.Xs + list(cp), y = father.y, generation = generation))
    
    
    return children

if __name__ == '__main__':
    cover, confidence = 0.01, 1.0
    data = pd.read_csv(r"testdata/relation.csv", dtype=str)
    print(f"Table length {len(data)} with {len(data.columns)} columns {list(data.columns)}")
    re = RuleExecutor(data)

    sps = all_structual_predicates(data)
    print(f"Create {len(sps)} structual predicates. Such as {sps[:3]}")
    cps = all_constant_predicates(data, singleLine=False)
    print(f"Create {len(cps) * len(cps[0])} constant predicates. Such as {cps[:3]}")
    rules = first_generation(sps)

    result:List[Rule] = []
    for _ in range(20):
        re.execute(rules)
        fathers = []
        for rule in rules:
            if rule.ok(cover, confidence):
                result.append(rule)
            elif rule.fertile(cover):
                fathers.append(rule)
        if len(fathers) == 0:
            break
        rules = next_generation(fathers, result, sps, cps, re, cover)

    for rules in groupByKey(result, lambda rule:rule.y).values():
        if len(rules) > 0:
            print(f"RHS: {rules[0].y}")
        for rule in sorted(rules, key = lambda rule:rule.generation):
            print("|" + "----" * (rule.generation -1) + str(rule))