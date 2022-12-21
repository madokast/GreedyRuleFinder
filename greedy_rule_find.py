import sys
import time
from typing import Dict, List, Set, Tuple
from rule import Predicate, Rule, RuleExecutor, Y, NegPred
import pandas as pd
from utils import countByValue, foreach, groupByKey

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

def next_generation(fathers:List[Rule], new_found_rules:List[Rule], all_found_rules:List[Rule], 
        structual_predicates:List[Predicate], constant_predicates:List[Tuple[Predicate]], re:RuleExecutor, cover:float)->List[Rule]:
    # The data of Y has changed ? Bacause Y appears in the new found rules.
    changed_Ys:Set[Y] = {rule.y for rule in new_found_rules}
    # all negetive predicates of Y
    negetive_predicates_map:Dict[Y, Set[NegPred]] = {rule:set() for rule in structual_predicates}
    for y, rules in groupByKey(all_found_rules, lambda rule:rule.y).items():
        foreach(rules, lambda rule:foreach(rule.Xs, lambda x:negetive_predicates_map[y].add(x if x.negative else x.negate())))
    # all columns of all negetive predicates of Y
    negetive_columns_map:Dict[Y, Set[str]] = {rule:set() for rule in structual_predicates}
    for y, ps in negetive_predicates_map.items():
        foreach(ps, lambda p:negetive_columns_map[y].update(p.columns))

    
    no_create_children_Ys:Set[Y] = set()
    for y, neg_preds in negetive_predicates_map.items():
        rule_proxy = Rule(Xs = list(neg_preds), y = y)
        re.execute([rule_proxy], progressBar=False)
        if rule_proxy.cover() < cover:
            no_create_children_Ys.add(y)
    
    generation = fathers[0].generation + 1
    children:List[Rule] = []

    # Rules about data-changed Y
    for y in changed_Ys:
        if y in no_create_children_Ys:
            continue
        for sp in structual_predicates:
            if sp.compatible(y) and len(sp.columns & negetive_columns_map.get(y, set())) == 0:
                children.append(Rule(Xs = list(negetive_predicates_map[y]) + [sp], y = y, generation = generation))

    # Rules about fathers' children
    for father in fathers:
        y = father.y
        if y in no_create_children_Ys:
            continue
        if y in changed_Ys:
            continue
        for sp in structual_predicates:
            if father.compatible(sp):
                children.append(Rule(Xs = father.Xs + [sp], y = y, generation = generation))
        for cp in constant_predicates:
            if father.compatible(cp[0]):
                children.append(Rule(Xs = father.Xs + list(cp), y = y, generation = generation))
        
    return children

if __name__ == '__main__':
    greedy = True
    start = time.time()
    cover, confidence = 0.0000001, 1.0
    data = pd.read_csv(r"D:\Downloads\testdata\查错\datasets\flights\clean.csv", dtype=str)
    print(f"Table length {len(data)} with {len(data.columns)} columns {list(data.columns)}")
    re = RuleExecutor(data)
    ignore_columns = ['row_id', 'source_data_id', 'last_update_time', 'batch_id', 'uuid', 'tuple_id']

    sps = all_structual_predicates(data, ignore_columns=ignore_columns)
    print(f"Create {len(sps)} structual predicates. Such as {sps[:3]}")
    cps = all_constant_predicates(data, singleLine=False, ignore_columns=ignore_columns, threshold=0.01)
    print(f"Create {len(cps) * (0 if len(cps) == 0 else len(cps[0]))} constant predicates. Such as {cps[:3]}")
    rules = first_generation(sps)

    all_found_rules:List[Rule] = []
    for _ in range(20):
        rules = re.execute_parallel(rules)
        fathers, new_found_rules = [], []
        for rule in rules:
            if rule.ok(cover, confidence):
                new_found_rules.append(rule)
            elif rule.reproducible(cover):
                fathers.append(rule)
        all_found_rules.extend(new_found_rules)
        if len(fathers) == 0:
            break
        rules = next_generation(fathers, new_found_rules if greedy else [], all_found_rules, sps, cps, re, cover)

    for rules in groupByKey(all_found_rules, lambda rule:rule.y).values():
        if len(rules) > 0:
            print(f"RHS: {rules[0].y}")
        for rule in sorted(rules, key = lambda rule:rule.generation):
            print("|---" * (rule.generation -1) + str(rule))
    print(f"Rules number {len(all_found_rules)} duration {time.time() - start}s")