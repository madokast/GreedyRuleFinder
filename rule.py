from typing import List
import pandas as pd
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
    
    def __str__(self) -> str:
        return self.__repr__()
    
    def __repr__(self) -> str:
        if self.constant is None:
            return f"t0.{self.t0_col} {self.operator} t1.{self.t1_col}"
        else:
            if self.t1_col is None:
                return f"t0.{self.t0_col} {self.operator} '{self.constant}'"
            else:
                return f"t1.{self.t1_col} {self.operator} '{self.constant}'"

    def sql(self)->str:
        return self.__repr__()

class Rule:
    def __init__(self, Xs:List[Predicate] = [], y:Predicate = None, rowSize:int = 0, xSupp:int = 0, supp:int = 0) -> None:
        self.Xs = Xs
        self.y = y
        self.rowSize = rowSize
        self.xSupp = xSupp
        self.supp = supp

    def singleLine(self)->bool:
        for x in self.Xs:
            if not x.isConst():
                return False
        
        return self.y.isConst()

    def rowSizeSQL(self, sameTable:bool = True)->str:
        if self.singleLine():
            return f"SELECT count(*) FROM {SQL_TAB0} AS t0"
        else:
            sql = f"SELECT count(*) FROM {SQL_TAB0} AS t0, {SQL_TAB1} AS t1"
            if sameTable:
                sql += f" WHERE t0.{SQL_ID_COL} <> t1.{SQL_ID_COL}"
            return sql
    
    def xSuppSQL(self, sameTable:bool = True)->str:
        sql = f"SELECT count(*) FROM {SQL_TAB0} AS t0 "
        wheres = [p.sql() for p in self.Xs]
        if not self.singleLine():
            sql += f", {SQL_TAB1} AS t1 "
            if sameTable:
                wheres.append(f"t0.{SQL_ID_COL} <> t1.{SQL_ID_COL}")
        return sql + " WHERE " + " AND ".join(wheres)

    def suppSQL(self, sameTable:bool = True)->str:
        return self.xSuppSQL(sameTable) + " AND " + self.y.sql()
    
    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        x = " ".join((str(p) for p in self.Xs))
        cover = round(self.supp/self.rowSize, 2)
        conf = round(self.supp/self.xSupp, 2)
        return f"{x} -> {self.y}, rowSize={self.rowSize}, xSupp={self.xSupp}, supp={self.supp}, covre={cover}, conf={conf}"

def _rm(file:str):
    if os.path.exists(file):
        time.sleep(1e-10)
        os.remove(file)

def ruleCheck(rules:List[Rule], t0:pd.DataFrame, t1:pd.DataFrame = None):
    t0 = copy.deepcopy(t0)
    t0[SQL_ID_COL] = range(len(t0))
    
    sameTable = t1 is None
    if sameTable:
        t1 = t0
    else:
        t1 = copy.deepcopy(t1)
        t1[SQL_ID_COL] = range(len(t1))
    
    _rm(SQLITE3_TEMP_FILE)
    conn:sqlite3.Connection = sqlite3.connect(SQLITE3_TEMP_FILE)
    try:
        t0.to_sql(SQL_TAB0, conn)
        t1.to_sql(SQL_TAB1, conn)
        for rule in rules:
            rule.rowSize = conn.execute(rule.rowSizeSQL(sameTable)).fetchone()[0]
            rule.xSupp = conn.execute(rule.xSuppSQL(sameTable)).fetchone()[0]
            rule.supp = conn.execute(rule.suppSQL(sameTable)).fetchone()[0]
    finally:
        conn.close()
        _rm(SQLITE3_TEMP_FILE)

    



if __name__ == '__main__':
    data = pd.read_csv("testdata/relation.csv", dtype=str)
    print(data)

    rule1 = Rule([Predicate.newConst0("ac", "908")], Predicate.newConst0("cc", "01"))
    rule2 = Rule([Predicate.newStruct("cc")], Predicate.newStruct("ac"))
    ruleCheck([rule1, rule2], data)
    print(rule1)
    print(rule2)
