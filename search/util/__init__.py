# @Time    : 2023/03/21 17:06
# @Author  : fyq
# @File    : __init__.py.py
# @Software: PyCharm

__author__ = 'fyq'

from typing import List


def repeat(datas: List, key: str) -> str:

    try:
        value_list = [getattr(data, key) for data in datas if hasattr(data, key)]
        return ",".join([value for value in value_list if value_list.count(value) > 1])
    except (ValueError, IndexError) as e:
        return ""
