# @Time    : 2023/03/21 17:07
# @Author  : fyq
# @File    : convert.py
# @Software: PyCharm

__author__ = 'fyq'

from typing import Union, List, Dict, Any, TypeVar, Callable

import munch

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


def data2DictOrList(data: Union[Dict, List]):
    if isinstance(data, Dict):
        res = {}
        for k, v in data.items():
            if isinstance(v, Dict):
                res[k] = data2DictOrList(v)
            elif isinstance(v, List):
                res[k] = data2DictOrList(v)
            elif hasattr(v, "to_dict"):
                res[k] = v.to_dict()
            else:
                res[k] = v
        return res
    elif isinstance(data, List):
        res = []
        for d in data:
            if isinstance(d, Dict):
                res.append(data2DictOrList(d))
            elif isinstance(d, List):
                res.append(data2DictOrList(d))
            elif hasattr(d, "to_dict"):
                res.append(d.to_dict())
            else:
                res.append(d)
        return res


def data2munch(data: Union[Dict, List]):
    if isinstance(data, Dict):
        res = {}
        for k, v in data.items():
            if isinstance(v, Dict):
                res[k] = data2munch(v)
            elif isinstance(v, List):
                res[k] = data2munch(v)
            else:
                res[k] = v
        return munch.Munch(res)
    elif isinstance(data, List):
        res = []
        for d in data:
            if isinstance(d, Dict):
                res.append(data2munch(d))
            elif isinstance(d, List):
                res.append(data2munch(d))
            else:
                res.append(d)
        return res
