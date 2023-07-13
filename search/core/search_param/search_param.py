# @Time    : 2023/07/12 13:52
# @Author  : fyq
# @File    : search_param.py
# @Software: PyCharm

__author__ = 'fyq'

from abc import ABCMeta, abstractmethod, abstractproperty
from typing import List

from munch import Munch

from search.core.search_context import SearchContext

_params: List["SearchParam"] = []


class SearchParam(metaclass=ABCMeta):
    name: str

    text: str

    default: bool = False

    @abstractmethod
    def __call__(self, value: bool, search_context: SearchContext):
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

    def __call__(self, value: bool, search_context: SearchContext):
        pass


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
        for param in _params:
            if param.name not in params:
                params[param.name] = param.default
            param(value=params[param.name], search_context=search_context)


sph = SearchParamHandler()
