# @Time    : 2023/07/12 13:52
# @Author  : fyq
# @File    : search_param.py
# @Software: PyCharm

__author__ = 'fyq'

from abc import ABCMeta, abstractmethod, abstractproperty
from typing import List

from munch import Munch

from search.core.search_context import SearchContext, scm

_params: List["SearchParam"] = []


class SearchParam(metaclass=ABCMeta):
    name: str

    text: str

    default: bool = False

    @abstractmethod
    def __call__(self, value: bool, do_search: bool, search_context: SearchContext):
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

    def __call__(self, value: bool, do_search: bool, search_context: SearchContext):
        pass


class ClearCacheSearchParam(SearchParam):
    name = "clear_cache"

    text = "清空缓存"

    default = False

    def __call__(self, value: bool, do_search: bool, search_context: SearchContext):
        if do_search and value:
            scm.clear_cache(search_context=search_context)


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

        for param in _params:
            if param.name not in params:
                params[param.name] = param.default
            param(value=params[param.name], search_context=search_context, do_search=params["do_search"])


sph = SearchParamHandler()
