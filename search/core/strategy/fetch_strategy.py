# @Time    : 2023/04/25 11:54
# @Author  : fyq
# @File    : fetch_strategy.py
# @Software: PyCharm

__author__ = 'fyq'

from abc import ABCMeta, abstractmethod
from typing import List


class IFetchStrategy(metaclass=ABCMeta):

    @abstractmethod
    def get_fetch_rows(self, cols: List[str]) -> int:
        pass


class FetchLengthStrategy(IFetchStrategy):
    """
        根据字段多少返回每次需要迭代的数量
    """

    def get_fetch_rows(self, cols: List[str]) -> int:
        cols_len = len(cols)
        if cols_len > 30:
            return 200000
        elif cols_len > 25:
            return 250000
        elif cols_len > 20:
            return 300000
        elif cols_len > 15:
            return 350000
        elif cols_len > 10:
            return 400000
        else:
            return 500000
