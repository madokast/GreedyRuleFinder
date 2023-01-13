from typing import Callable, List, Tuple
from rule import Predicate, Rule, RuleExecutor, Y
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

def topk_rule_find(table:pd.DataFrame, structual_predicates:List[Predicate], constant_predicates:List[Tuple[Predicate]], 
        single_line:bool = True, multi_line:bool = True, topk:int = 1, cover:float = 0.01, confidence:float = 0.8,
        x_column:Callable[[str], bool] = lambda c:True, y_column:Callable[[str], bool] = lambda c:True,
        level2:bool=False) -> List[Rule]:
    results:List[Rule] = []
    re = RuleExecutor(table)
    if single_line:
        for Y in (ys[0] for ys in constant_predicates if all(y_column(c) for c in ys[0].columns)):
            excluding_xs:List[Predicate] = []
            while True:
                print(f"single line find y = {Y} excluding_xs {excluding_xs}")
                rules = find_rules([], constant_predicates, Y, excluding_xs, re, single_line=True, multi_line=False, topk=topk, cover=cover, confidence=confidence, x_column=x_column, level2=level2)
                if len(rules) == 0:
                    break
                results.extend(rules)
                foreach(rules, lambda r:excluding_xs.extend((x.negate() for x in r.Xs if not x.negative)))
                excluding_xs = list(set(excluding_xs)) # distinct
    if multi_line:
        for Y in (y for y in structual_predicates if all(y_column(c) for c in y.columns)):
            excluding_xs:List[Predicate] = []
            while True:
                print(f"multi_ line find y = {Y} excluding_xs {excluding_xs}")
                rules = find_rules(structual_predicates, constant_predicates, Y, excluding_xs, re, single_line=False, multi_line=True, topk=topk, cover=cover, confidence=confidence, x_column=x_column, level2=level2)
                if len(rules) == 0:
                    break
                results.extend(rules)
                foreach(rules, lambda r:excluding_xs.extend((x.negate() for x in r.Xs if not x.negative)))
                excluding_xs = list(set(excluding_xs)) # distinct
    return results

def find_rules(structual_predicates:List[Predicate], constant_predicates:List[Tuple[Predicate]], 
        Y:Y, excluding_xs:List[Predicate], re:RuleExecutor, single_line:bool, multi_line:bool, topk:int = 1, 
        cover:float = 0.01, confidence:float = 0.8, x_column:Callable[[str], bool] = lambda c:True, level2:bool=False)->List[Rule]:
    rules:List[Rule] = []; precursors:List[Rule] = []

    def put_precursor(*xs:Predicate):
        for x in xs:
            if (not x.compatible(*excluding_xs)) or (not x.compatible(Y)) or (not all((x_column(c) for c in x.columns))):
                return
        precursors.append(Rule(Xs = excluding_xs + list(xs), y = Y))
    def execute_precursors():
        if len(precursors) > 0:
            run = re.execute(precursors)
            foreach(run, lambda r:r.ok(cover, confidence, lambda t:rules.append(t)))
            precursors.clear()

    # level = 1
    if single_line:
        for x in (xs[0] for xs in constant_predicates):
            put_precursor(x)
    if multi_line:
        for x in structual_predicates:
            put_precursor(x)
        for xs in constant_predicates:
            put_precursor(*xs)
    execute_precursors()
    if len(rules) >= topk:
        rules = sorted(rules, key=lambda r:r.cover(), reverse=True)
        return rules[:topk]

    if level2:
        if single_line:
            for x1 in (xs[0] for xs in constant_predicates):
                for x2 in (xs[0] for xs in constant_predicates):
                    if x1.compatible(x2):
                        put_precursor(x1, x2)
        if multi_line:
            for x1 in structual_predicates:
                for x2 in structual_predicates:
                    if x1.compatible(x2):
                        put_precursor(x1, x2)
            for xs1 in constant_predicates:
                for xs2 in constant_predicates:
                    if xs1[0].compatible(xs2[0]):
                        put_precursor(xs1[0], xs1[1], xs2[0], xs2[1])
        execute_precursors()
        rules = sorted(rules, key=lambda r:r.cover(), reverse=True)
    return rules[:topk]
    
if __name__ == '__main__':
    data = pd.read_csv(r"D:\work\20221104_规则发现查错效果分析\repy\data2\beers\dirty_l.csv", dtype=str)
    print(data)
    ignore_columns = ['id', 'aic', '_aic', 'index', '_index', 'row_id', 'source_data_id', 'last_update_time', 'batch_id', 'uuid', 'tuple_id']

    sps = all_structual_predicates(data, ignore_column=lambda c:c in ignore_columns)
    # cps_s = all_constant_predicates(data, singleLine=True, ignore_column=lambda c:c in ignore_columns)
    cps_m = all_constant_predicates(data, singleLine=False, ignore_column=lambda c:c in ignore_columns, threshold=1e-10)

    rules = topk_rule_find(data, sps, cps_m, single_line=True, multi_line=True, topk=10, cover=1e-10, confidence=1, 
        x_column = lambda c:not c.startswith('_'),
        y_column = lambda c:c.startswith('_'),
        level2 = False)

    with open("rules.txt", mode = "bw") as f:
        foreach(rules, lambda r:f.write((r.ree("beers", statistics=False)+'\r\n').encode('utf-8')))

    print(f"Rules number {len(rules)}")