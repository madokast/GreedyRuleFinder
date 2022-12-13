# GreedyRuleFinder
王博士提出新规则发现算法，本文中我称之为贪心规则发现。基本算法是决策树+结构谓词。

## 基本算法
```python
for sp in sp_set: # 所有结构谓词
  deep_search(tab, xs = [], y = sp)

# 递归
def deep_search(tab, xs, y):
  if rule(xs, y, tab): # 如果 xs -> y 成立，输出，退出迭代
    output((xs, y))
    return
  else:
    remain_tab, rules = decision_tree(tab, xs, y) # 决策树进行规则发现，返回规则和规则涉及不到的元组
    output(rules) # 输出规则
    
    if not terminate(remain_tab): # 表长度足够挖出覆盖率达标的规则
      # xs 中加入新结构谓词
      for next_sp in sp_set:
        xs.push(next_p)
        deep_search(tab, xs, y)
        xs.pop()
```