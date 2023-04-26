# @Time    : 2023/04/25 14:31
# @Author  : fyq
# @File    : search_strategy.py
# @Software: PyCharm

__author__ = 'fyq'


from abc import ABCMeta, abstractmethod
from typing import List
from search import constant
from search.core.search_context import SearchContext

from search.util.redis import redis_lock


class ISearchStrategy(metaclass=ABCMeta):

    @abstractmethod
    def add_lock(self, f):
        pass

    @abstractmethod
    def can_lock(self, f) -> bool:
        pass

    @abstractmethod
    def get_search_type(self, search_context: SearchContext) -> str:
        pass


class RedisStrategySearch(ISearchStrategy):

    def get_search_type(self, search_context: SearchContext) -> str:
        pass

    def add_lock(self, f):
        with redis_lock(constant.RedisKey.SEARCH_STRATEGY_LOCK, forever=True) as res:
            if res:
                pass

    def can_lock(self, f) -> bool:
        pass
