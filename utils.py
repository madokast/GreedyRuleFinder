from typing import Callable, Dict, Iterable, List, TypeVar
K = TypeVar('K')
V = TypeVar('V')

__all__ = ["countByValue", "groupByKey", "foreach"]

def countByValue(values:Iterable[V])->Dict[V, int]:
    m:Dict[V, int] = {}
    for v in values:
        m[v] = m.get(v, 0) + 1
    return m

def groupByKey(values:Iterable[V], keyFunc:Callable[[V], K])->Dict[K, List[V]]:
    m:Dict[K, List[V]] = {}
    for v in values:
        k = keyFunc(v)
        vs = m.get(k, [])
        vs.append(v)
        m[k] = vs
    return m

def foreach(values:Iterable[V], consumer:Callable[[V], None])->None:
    for v in values:
        consumer(v)