# @Time    : 2023/07/15 11:30
# @Author  : fyq
# @File    : function.py
# @Software: PyCharm

__author__ = 'fyq'

import abc
import typing

import munch

from search import models, CommonResult, db
from search.core.decorator import transactional
from search.exceptions import SearchException
from search.util.convert import data2obj


class ISearchFunctionConfig(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def search_function(self, data: dict) -> typing.Dict:
        pass

    @abc.abstractmethod
    def function(self, function_id: int) -> typing.Dict:
        pass

    @abc.abstractmethod
    def save(self, data: dict) -> typing.Dict:
        pass

    @abc.abstractmethod
    def delete(self, function_id: int) -> typing.Dict:
        pass


class SearchFunctionConfig(ISearchFunctionConfig):

    @transactional
    def delete(self, function_id: int) -> typing.Dict:
        models.SearchFunction.query.filter_by(id=function_id).delete()
        return CommonResult.success()

    @transactional
    def save(self, data: dict) -> typing.Dict:
        m = munch.Munch(data)
        if "id" in m:
            search_function = (models.SearchFunction.query
                               .filter_by(id=m.id, version=m.version)
                               .first())
            if not search_function:
                raise SearchException(f"查询[{m.name}]未找到到或者数据版本不正确")
            m.version += 1
            models.SearchFunction.query.filter_by(id=m.id).update(m)
        else:
            search_function: models.SearchFunction = data2obj(m, models.SearchFunction)
            db.session.add(search_function)

        return CommonResult.success()

    def search_function(self, data: dict) -> typing.Dict:
        m = munch.Munch(data)
        s_q = models.SearchFunction.query
        q = []
        for k, v in m.items():
            if v:
                q.append(getattr(models.SearchFunction, k).like(f"%{v}%"))
        if len(q) > 0:
            s_q = s_q.filter(*q)
        search_function_list: typing.List[models.SearchFunction] = s_q.all()
        return CommonResult.success(data=[{"name": search_function.name, "id": search_function.id}
                                          for search_function in search_function_list])

    def function(self, function_id: int) -> typing.Dict:
        return CommonResult.success(data=models.SearchFunction.query.filter_by(id=function_id).first())


search_function_config = SearchFunctionConfig()
