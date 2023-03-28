# @Time    : 2023/03/21 17:07
# @Author  : fyq
# @File    : convert.py
# @Software: PyCharm

__author__ = 'fyq'

from typing import Union, List, Dict, Any, TypeVar, Callable

from search.exceptions import SearchUtilException

TypeOrCallable = TypeVar("TypeOrCallable", bound=Union[type, Callable])


def data2obj(data: Union[Dict[str, Any], List], cls: TypeOrCallable) -> Union[Any, List[Any]]:
    error = "无法映射非Dict或者List的数据"
    if isinstance(data, List):
        data_list: List[cls] = []
        for d in data:
            if not isinstance(d, Dict):
                raise SearchUtilException(error)
            obj = cls()
            data_list.append(obj)
            for k, v in d.items():
                setattr(obj, k, v)
        return data_list
    elif isinstance(data, Dict):
        obj = cls()
        for k, v in data.items():
            setattr(obj, k, v)
        return obj
    else:
        raise SearchUtilException(error)

