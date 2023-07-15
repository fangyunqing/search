# @Time    : 2023/07/14 13:11
# @Author  : fyq
# @File    : key.py
# @Software: PyCharm

__author__ = 'fyq'

from abc import ABCMeta, abstractmethod

from search import constant
from search.core.search_context import SearchContext


class Key(metaclass=ABCMeta):

    @abstractmethod
    def generate(self, search_context: SearchContext) -> str:
        pass


class ThreadKey(Key):

    def generate(self, search_context: SearchContext) -> str:
        return (f"{constant.RedisKey.THREAD_LOCK_PREFIX}"
                "_"
                f"{constant.RedisKey.SEARCH}_{search_context.search_key}")


thread_key = ThreadKey()
