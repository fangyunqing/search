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
    def generate(self, search_context: SearchContext, **kwargs) -> str:
        pass


class ThreadKey(Key):

    def generate(self, search_context: SearchContext, **kwargs) -> str:
        search_type = kwargs.pop('search_type', 'search')
        return (f"{constant.RedisKey.THREAD_LOCK_PREFIX}"
                "_"
                f"{search_type}_{search_context.search_key}")


class ExportTarNoticeKey(Key):

    def generate(self, search_context: SearchContext, **kwargs) -> str:
        return (f"{constant.RedisKey.EXPORT_TAR_NOTICE_PREFIX}"
                "-"
                f"{search_context.search_key}")


thread_key = ThreadKey()
export_tar_notice_key = ExportTarNoticeKey()
