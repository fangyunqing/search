# @Time    : 2023/07/12 13:52
# @Author  : fyq
# @File    : search_param.py
# @Software: PyCharm

__author__ = 'fyq'

from abc import ABCMeta, abstractmethod, abstractproperty
from typing import List

import redis
from munch import Munch

from search.core.search_context import SearchContext, scm
from search.exceptions import SearchException
from search.extend import redis_pool
from search.generate_key import thread_key

_params: List["SearchParam"] = []


class SearchParam(metaclass=ABCMeta):
    name: str

    text: str

    default: bool = False

    @abstractmethod
    def __call__(self, value: bool, do_search: bool, search_context: SearchContext, search_type: str):
        pass

    def __init_subclass__(cls, **kwargs):
        _params.append(cls())

    @property
    def param(self) -> dict:
        return {
            "name": self.name,
            "text": self.text
        }


class TopInHistorySearchParam(SearchParam):
    name = "top_in_history"

    text = "Top历史库"

    default = False

    def __call__(self, value: bool, do_search: bool, search_context: SearchContext, search_type: str):
        pass


class ClearCacheSearchParam(SearchParam):
    name = "clear_cache"

    text = "清空缓存"

    default = False

    def __call__(self, value: bool, do_search: bool, search_context: SearchContext, search_type: str):
        if value:
            if (search_type == "search" and do_search) or (search_type == "export"):
                key = thread_key.generate(search_context=search_context, search_type=search_type)
                r = redis.Redis(connection_pool=redis_pool)
                if r.exists(key):
                    raise SearchException("无法清空缓存，缓存正在生成中，请稍后再试")
                scm.clear_cache(search_context=search_context, search_type=search_type)


class EmailNotification4Export(SearchParam):
    def __call__(self, value: bool, do_search: bool, search_context: SearchContext, search_type: str):
        pass

    name = "email_notification_4_Export"

    text = "导出完成后邮件通知"

    default = False


class ISearchParamHandler(metaclass=ABCMeta):

    @property
    @abstractmethod
    def params(self) -> list:
        pass

    @abstractmethod
    def handle(self, params: Munch, search_context: SearchContext):
        pass


class SearchParamHandler(ISearchParamHandler):

    @property
    def params(self) -> list:
        return [param.param for param in _params]

    def handle(self, params: Munch, search_context: SearchContext):
        if "do_search" not in params:
            params["do_search"] = False
        if 'search_type' not in params:
            params["search_type"] = 'search'

        for param in _params:
            if param.name not in params:
                params[param.name] = param.default
            param(value=params[param.name],
                  search_context=search_context,
                  do_search=params["do_search"],
                  search_type=params["search_type"])


sph = SearchParamHandler()
