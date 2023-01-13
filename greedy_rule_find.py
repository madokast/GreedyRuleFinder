"""
2022年12月 挖掘全量规则
采用 level-wise，挖掘第一层，然后所有 X 谓词作为下一层的 excluding
"""

import time
from typing import Callable, Dict, List, Set, Tuple
from rule import Predicate, Rule, RuleExecutor, Y, NegPred
import pandas as pd
from utils import countByValue, foreach, groupByKey, collect

def all_structual_predicates(table:pd.DataFrame, ignore_column:Callable[[str], bool] = lambda c:False)->List[Predicate]:
    return [Predicate.newStruct(c) for c in table.columns if not ignore_column(c)]

def all_constant_predicates(table:pd.DataFrame, singleLine:bool, threshold:float = 0.1, 
        ignore_column:Callable[[str], bool] = lambda c:False)->List[Tuple[Predicate]]:
    cps:List[Tuple[Predicate]] = []
    threshold = len(table) * threshold
    for col in table.columns:
        if ignore_column(col):
            continue
        cnt_map = countByValue(table[col])
        for val, cnt in cnt_map.items():
            if cnt >= threshold:
                const = str(val)
                if singleLine:
                    cps.append((Predicate.newConst0(col, const), ))
                else:
                    cps.append((Predicate.newConst0(col, const), Predicate.newConst1(col, const)))
    return cps

def first_generation(structual_predicates:List[Predicate] = [], constant_predicates:List[Tuple[Predicate]] = [], 
        x_column:Callable[[str], bool] = lambda c:True, y_column:Callable[[str], bool] = lambda c:True, 
        constant_y_in_multi_rule:bool=False)->List[Rule]:
    rules:List[Rule] = []
    for y in structual_predicates:
        if not all((y_column(col) for col in y.columns)):
            continue
        for x in structual_predicates:
            if not all((x_column(col) for col in x.columns)):
                continue
            if x.compatible(y):
                rules.append(Rule(Xs = [x], y = y))
    for cp in constant_predicates:
        y = cp[-1] # last one
        if not all((y_column(col) for col in y.columns)):
            continue
        if len(cp) == 2:
            if constant_y_in_multi_rule:
                rules.append(Rule(Xs = [cp[0]], y = y))
            else:
                continue
        else:
            for xcp in constant_predicates:
                x = xcp[0]
                if not all((x_column(col) for col in x.columns)):
                    continue
                if x.compatible(y):
                    rules.append(Rule(Xs = [x], y = y))
    return rules

def next_generation(fathers:List[Rule], ruleExecutor:RuleExecutor, new_found_rules:List[Rule], all_found_rules:List[Rule], 
        structual_predicates:List[Predicate] = [], constant_predicates:List[Tuple[Predicate]] = [], cover:float = 0.01, 
        x_column:Callable[[str], bool] = lambda c:True, greedy:bool = True)->List[Rule]:
    if not greedy:
        new_found_rules = []
    # The data of Y has changed ? Bacause Y appears in the new found rules.
    changed_Ys:Set[Y] = {rule.y for rule in new_found_rules}
    # all negetive predicates of Y
    negetive_predicates_map:Dict[Y, Set[NegPred]] = {}
    for y, rules in groupByKey(all_found_rules, lambda rule:rule.y).items():
        negetive_predicates_map[y] = set()
        foreach(rules, lambda rule:foreach(rule.Xs, lambda x:negetive_predicates_map[y].add(x if x.negative else x.negate())))
    # all columns of all negetive predicates of Y
    negetive_columns_map:Dict[Y, Set[str]] = {y:set() for y in negetive_predicates_map.keys()}
    for y, ps in negetive_predicates_map.items():
        foreach(ps, lambda p:negetive_columns_map[y].update(p.columns))

    no_create_children_Ys:Set[Y] = set()
    for y, neg_preds in negetive_predicates_map.items():
        rule_proxy = Rule(Xs = list(neg_preds)[:900], y = y)
        ruleExecutor.execute([rule_proxy], progressBar=False)
        if rule_proxy.cover() < cover:
            no_create_children_Ys.add(y)
    
    generation = fathers[0].generation + 1
    children:List[Rule] = []

    # Rules about data-changed Y
    for y in changed_Ys:
        if y in no_create_children_Ys:
            continue
        if y.isConst():
            if y.t1_col is not None: # t0.a=1
                for cp in constant_predicates:
                    if all((x_column(c) for c in cp[0].columns)) and cp[0].compatible(y) and len(cp[0].columns & negetive_columns_map.get(y, set())) == 0:
                        children.append(Rule(Xs = list(negetive_predicates_map[y]) + [cp[0]], y = y, generation = generation))
            else: # t1.a=1
                for cp in constant_predicates:
                    if all((x_column(c) for c in cp[0].columns)) and cp[0] == y and len(cp[0].columns & negetive_columns_map.get(y, set())) == 0: # x only t0.a=1
                        children.append(Rule(Xs = list(negetive_predicates_map[y]) + [cp[0]], y = y, generation = generation))
        else:
            for sp in structual_predicates:
                if all((x_column(c) for c in sp.columns)) and sp.compatible(y) and len(sp.columns & negetive_columns_map.get(y, set())) == 0:
                    children.append(Rule(Xs = list(negetive_predicates_map[y]) + [sp], y = y, generation = generation))

    # Rules about fathers' children
    for father in fathers:
        y = father.y
        if y in no_create_children_Ys:
            continue
        if y in changed_Ys:
            continue
        for sp in structual_predicates:
            if all((x_column(c) for c in sp.columns)) and father.compatible(sp):
                children.append(Rule(Xs = father.Xs + [sp], y = y, generation = generation))
        for cp in constant_predicates:
            if all((x_column(c) for c in cp[0].columns)) and father.compatible(cp[0]):
                children.append(Rule(Xs = father.Xs + list(cp), y = y, generation = generation))
        
    return children

def rule_find(tables:Dict[str, pd.DataFrame], cover:float = 0.01, confidence:float = 0.8, constant_threshold:float = 0.1,
        ignore_column:Callable[[str], bool] = lambda c:False, x_column:Callable[[str], bool] = lambda c:True,
        y_column:Callable[[str], bool] = lambda c:True, max_levelwise_depth:int = 20, sql_thread_num:int = 1, 
        single_line:bool = True, multi_line:bool = True, cross_table:bool = False, greedy:bool = True)->List[Rule]:
    result:List[Rule] = []
    for tabName, data in tables.items():
        print(f"Start rule-find on table {tabName}. Length {len(data)} with {len(data.columns)} columns {list(data.columns)}")
        re = RuleExecutor(data)
        if single_line:
            cps = all_constant_predicates(data, singleLine=True, ignore_column=ignore_column, threshold=constant_threshold)
            rules = first_generation(constant_predicates=cps, x_column=x_column, y_column=y_column)
            all_found_rules:List[Rule] = []
            for _ in range(max_levelwise_depth):
                rules = re.execute_parallel(rules, workerNum=sql_thread_num) if sql_thread_num > 1 else re.execute(rules)
                new_found_rules = collect(rules, lambda r:r.ok(cover, confidence))
                fathers = collect(rules, lambda r:(not r.ok(cover, confidence)) and r.reproducible(cover))
                all_found_rules.extend(new_found_rules)
                if len(fathers) == 0:
                    break
                rules = next_generation(fathers, re, new_found_rules, all_found_rules, constant_predicates=cps, cover=cover, x_column=x_column, greedy=greedy)
            result.extend(all_found_rules)
        if multi_line:
            sps = all_structual_predicates(data, ignore_column=ignore_column)
            cps = all_constant_predicates(data, singleLine=False, ignore_column=ignore_column, threshold=constant_threshold)
            rules = first_generation(sps, cps, x_column=x_column, y_column=y_column)
            all_found_rules:List[Rule] = []
            for _ in range(max_levelwise_depth):
                rules = re.execute_parallel(rules, workerNum=sql_thread_num) if sql_thread_num > 1 else re.execute(rules)
                new_found_rules = collect(rules, lambda r:r.ok(cover, confidence))
                fathers = collect(rules, lambda r:(not r.ok(cover, confidence)) and r.reproducible(cover))
                all_found_rules.extend(new_found_rules)
                if len(fathers) == 0:
                    break
                rules = next_generation(fathers, re, new_found_rules, all_found_rules, structual_predicates=sps, constant_predicates=cps, cover=cover, x_column=x_column, greedy=greedy)
            result.extend(all_found_rules)
    return result

if __name__ == '__main__':
    start = time.time()
    data = pd.read_csv(r"D:\work\20221104_规则发现查错效果分析\repy\data2\beers\dirty_l.csv", dtype=str)
    ignore_columns = ['id', 'aic', '_aic', 'index', '_index', 'row_id', 'source_data_id', 'last_update_time', 'batch_id', 'uuid', 'tuple_id']


    all_found_rules = rule_find({"flights":data}, cover = 1e-10, confidence=1.0, constant_threshold=1e-10, 
        ignore_column=lambda c:c in ignore_columns, sql_thread_num=1, 
        single_line=True, multi_line=True, greedy=True,
        x_column = lambda c:not c.startswith('_'),
        y_column = lambda c:c.startswith('_'),
    )

    # for rules in groupByKey(all_found_rules, lambda rule:rule.y).values():
    #     if len(rules) > 0:
    #         print(f"RHS: {rules[0].y}")
    #     for rule in sorted(rules, key = lambda rule:rule.generation):
    #         print("|---" * (rule.generation -1) + str(rule))

    with open("rules.txt", mode = "bw") as f:
        foreach(all_found_rules, lambda r:f.write((r.ree("flights")+'\r\n').encode('utf-8')))

    print(f"Rules number {len(all_found_rules)} duration {time.time() - start}s")