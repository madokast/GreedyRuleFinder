from typing import Dict, Iterable, TypeVar
T = TypeVar('T')

__all__ = ["countByValue"]

def countByValue(values:Iterable[T])->Dict[T, int]:
    m:Dict[T, int] = {}
    for v in values:
        m[v] = m.get(v, 0) + 1
    return m
