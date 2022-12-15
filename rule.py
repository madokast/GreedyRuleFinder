from typing import List, Optional, Set
import pandas as pd
from tqdm import tqdm
import sqlite3
import os 
import time
import copy

SQLITE3_TEMP_FILE = 'sqlite3.tmp'
SQL_TAB0 = "tab0"
SQL_TAB1 = "tab1"
SQL_ID_COL = "id1213"

class Predicate:
    def __init__(self, t0_col:str, t1_col:str, operator:str, constant:str) -> None:
        self.t0_col = t0_col
        self.t1_col = t1_col
        self.operator = operator
        self.constant = constant
        self.columns = set([t0_col]) if t1_col is None else set([t0_col, t1_col])
        # new creating predicates are not negative
        self.negative = False
    
    @staticmethod
    def newConst0(col:str, constant:str, operator:str = '=')->'Predicate':
        return Predicate(col, None, operator, constant)

    @staticmethod
    def newConst1(col:str, constant:str, operator:str = '=')->'Predicate':
        return Predicate(None, col, operator, constant)

    @staticmethod
    def newStruct(t0_col:str, operator:str = '=', t1_col:str = None)->'Predicate':
        if t1_col is None:
            t1_col = t0_col
        return Predicate(t0_col, t1_col, operator, None)
    
    def isConst(self)->bool:
        return self.constant is not None
    
    def copy(self)->'Predicate':
        return copy.deepcopy(self)
    
    def negate(self)->'Predicate':
        if self.negative:
            raise Exception("Double negation on " + str(self))

        def negateOp(op:str)->str:
            if op == '=':
                return '<>'
            else:
                raise Exception('NoImpl ' + op)

        p = self.copy()
        p.operator = negateOp(p.operator)
        p.negative = True
        return p

    def compatible(self, another:'Predicate')->bool:
        return len(self.columns & another.columns) == 0
    
    def __str__(self, right_tuple_id:int = 1) -> str:
        return self.__repr__(right_tuple_id)
    
    def __repr__(self, right_tuple_id:int = 1) -> str:
        if self.constant is None:
            return f"t0.{self.t0_col} {self.operator} t{right_tuple_id}.{self.t1_col}"
        else:
            if self.t1_col is None:
                return f"t0.{self.t0_col} {self.operator} '{self.constant}'"
            else:
                return f"t{right_tuple_id}.{self.t1_col} {self.operator} '{self.constant}'"

    def sql(self)->str:
        return self.__repr__()

class Rule:
    def __init__(self, Xs:List[Predicate] = [], y:Predicate = None, sameTable:bool = True, rowSize:int = 0, xSupp:int = 0, supp:int = 0, generation:int = 1) -> None:
        self.Xs = Xs
        self.y = y
        self.sameTable = sameTable
        self.rowSize = rowSize
        self.xSupp = xSupp
        self.supp = supp
        self.generation = generation

    def copy(self)->'Rule':
        return copy.deepcopy(self)

    def allPredicates(self)->List[Predicate]:
        ps = []
        ps.extend(self.Xs)
        ps.append(self.y)
        return ps

    def columns(self)->Set[str]:
        s = set()
        for p in self.allPredicates():
            for c in p.columns:
                s.add(c)
        return s
    
    def compatible(self, predicate:Predicate)->bool:
        return len(self.columns() & predicate.columns) == 0

    def singleLine(self)->bool:
        for p in self.allPredicates():
            if not p.isConst():
                return False
        return True

    def rowSizeSQL(self)->str:
        if self.singleLine():
            return f"SELECT count(*) FROM {SQL_TAB0} AS t0"
        else:
            sql = f"SELECT count(*) FROM {SQL_TAB0} AS t0, {SQL_TAB1} AS t1"
            if self.sameTable:
                sql += f" WHERE t0.{SQL_ID_COL} <> t1.{SQL_ID_COL}"
            return sql
    
    def xSuppSQL(self)->str:
        sql = f"SELECT count(*) FROM {SQL_TAB0} AS t0"
        wheres = [p.sql() for p in self.Xs]

        if not self.singleLine():
            sql += f", {SQL_TAB1} AS t1"
            if self.sameTable:
                wheres.append(f"t0.{SQL_ID_COL} <> t1.{SQL_ID_COL}")

        where = " AND ".join(wheres)
        return sql + " WHERE " + where
    
    def cover(self)->float:
        return 0. if self.rowSize == 0 else self.supp/self.rowSize
    
    def confidence(self)->float:
        return 0. if self.xSupp == 0 else self.supp/self.xSupp

    def ok(self, cover:float = 0.1, confidence:float = 0.8)->bool:
        return self.cover() >= cover and self.confidence() >= confidence
    
    def fertile(self, cover:float = 0.1)->bool:
        return self.cover() >= cover

    def suppSQL(self)->str:
        return self.xSuppSQL() + " AND " + self.y.sql()
    
    def __str__(self, right_tuple_id:int = 1) -> str:
        return self.__repr__(right_tuple_id)

    def __repr__(self, right_tuple_id:int = 1) -> str:
        xs = " ^ ".join((p.__str__(right_tuple_id) for p in self.Xs))
        cover = round(self.cover(), 2)
        conf = round(self.confidence(), 2)
        return f"{xs} -> {self.y.__str__(right_tuple_id)}, rowSize={self.rowSize}, xSupp={self.xSupp}, supp={self.supp}, covre={cover}, conf={conf}"

    def ree(self, t0_tab:str, t1_tab:Optional[str] = None)->str:
        if t1_tab is None:
            t1_tab = t0_tab
        
        if self.singleLine():
            return f"{t0_tab}(t0) ^ {self}"
        else:
            right_tuple_id = 1 if t0_tab == t1_tab else 2
            return f"{t0_tab}(t0) ^ {t1_tab}(t{right_tuple_id}) ^ {self.__str__(right_tuple_id)}"

class RuleExecutor:
    def __init__(self, t0:pd.DataFrame, t1:pd.DataFrame = None) -> None:
        t0 = copy.deepcopy(t0)
        t0[SQL_ID_COL] = range(len(t0))

        sameTable = t1 is None
        if sameTable:
            t1 = t0
        else:
            t1 = copy.deepcopy(t1)
            t1[SQL_ID_COL] = range(len(t1))

        RuleExecutor._rm(SQLITE3_TEMP_FILE)
        conn:sqlite3.Connection = sqlite3.connect(SQLITE3_TEMP_FILE)
        t0.to_sql(SQL_TAB0, conn)
        t1.to_sql(SQL_TAB1, conn)

        self.conn = conn
        self.t0_len = len(t0)
        self.t1_len = len(t1)

        print("Rule executor initing")
    
    def predicatesSuppport(self, predicates:List[Predicate], sameTable:bool)->int:
        assert len(predicates) > 0
        rule_proxy = Rule(Xs = predicates, sameTable = sameTable)
        return self.conn.execute(rule_proxy.xSuppSQL()).fetchone()[0]

    def execute(self, rules:List[Rule]):
        for rule in (rules if len(rules) < 2 else tqdm(rules, desc = "Executing")):
            # print(rule, rule.xSuppSQL(), rule.suppSQL(), sep = '\n')
            # rule.rowSize = self.conn.execute(rule.rowSizeSQL()).fetchone()[0]
            rule.xSupp = self.conn.execute(rule.xSuppSQL()).fetchone()[0]
            rule.supp = self.conn.execute(rule.suppSQL()).fetchone()[0]

            if rule.singleLine():
                rule.rowSize = self.t0_len
            else:
                if rule.sameTable:
                    rule.rowSize = self.t0_len * (self.t0_len - 1)
                else:
                    rule.rowSize = self.t0_len * self.t1_len
            # print(rule)
    
    def __del__(self):
        self.conn.close()
        RuleExecutor._rm(SQLITE3_TEMP_FILE)
    

    @staticmethod
    def _rm(file:str):
        if os.path.exists(file):
            time.sleep(1e-10)
            os.remove(file)


if __name__ == '__main__':
    data = pd.read_csv("testdata/relation.csv", dtype=str)
    print(data)
    rule1 = Rule(Xs = [Predicate.newConst0("pn", "2222222"), Predicate.newConst0("ac", "908", "<>"), Predicate.newConst0("ct", "EDI", "<>")], 
        y = Predicate.newConst0("cc", "01"))
    rule2 = Rule(Xs = [Predicate.newStruct("cc")], y = Predicate.newStruct("ac"))
    print(rule1.suppSQL())
    print(rule2.suppSQL())
    RuleExecutor(data).execute([rule1, rule2])
    print(rule1)
    print(rule2)

    print(rule2.ree("r1", "r1"))
    print(rule2.ree("r1", "r2"))
