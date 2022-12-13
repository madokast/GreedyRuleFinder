# GreedyRuleFinder
A new greedy rule-digging algorithm leveraging decision-tree and recursion, proposed by Ph.D Wang.

## Pseudocode
```python
for sp in sp_set: # Structural predicates
  deep_search(tab, xs = [], y = sp)

# recursion
def deep_search(tab, xs, y):
  if rule(xs, y, tab): # exit recursion if rule xs -> y ok.
    output((xs, y))
    return
  else:
    remain_tab, rules = decision_tree(tab, xs, y) # find rules "xs ^ Constant predicates -> y"
    output(rules)

    if not terminate(remain_tab): # If the length of remaining table is enough to find ok rule.
      # add a new structural predicate into xs and go on
      for next_sp in sp_set:
        xs.push(next_p)
        deep_search(tab, xs, y)
        xs.pop()
```