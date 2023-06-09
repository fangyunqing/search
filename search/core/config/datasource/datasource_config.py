# @Time    : 2023/03/21 16:29
# @Author  : fyq
# @File    : datasource_config.py
# @Software: PyCharm

__author__ = 'fyq'

from abc import ABCMeta, abstractmethod

import munch
import simplejson

from search import models, db, dm
from search.core.decorator import transactional
from search.entity import CommonResult, MessageCode
from search.util import convert


class IDatasourceConfig(metaclass=ABCMeta):

    @abstractmethod
    def get_datasource(self) -> dict:
        pass

    @abstractmethod
    def usable(self, datasource_id: int) -> dict:
        pass

    @abstractmethod
    def disable(self, datasource_id: int) -> dict:
        pass

    @abstractmethod
    def modify(self, data: str) -> dict:
        pass

    @abstractmethod
    def add(self, data: str) -> dict:
        pass


class DataSourceConfig(IDatasourceConfig):

    @transactional
    def add(self, data: str) -> dict:
        m = munch.Munch(simplejson.loads(data))
        search_datasource: models.SearchDatasource = convert.data2obj(m, models.SearchDatasource)
        db.session.add(search_datasource)
        db.session.flush()
        dm.register(search_datasource)
        return CommonResult.success()

    def get_datasource(self) -> dict:
        search_datasource_list = models.SearchDatasource.query.all()
        return CommonResult.success(data=[s.to_dict() for s in search_datasource_list])

    @transactional
    def usable(self, datasource_id: int) -> dict:
        search_datasource: models.SearchDatasource = models.SearchDatasource.query.filter_by(id=datasource_id).first()
        if search_datasource:
            search_datasource.usable = '1'
            db.session.flush()
            dm.register(search_datasource)
            return CommonResult.success()
        else:
            return CommonResult.fail(MessageCode.NOT_FOUND.code, message=f"{datasource_id}未找到")

    @transactional
    def disable(self, datasource_id: int) -> dict:
        search_datasource: models.SearchDatasource = models.SearchDatasource.query.filter_by(id=datasource_id).first()
        if search_datasource:
            search_datasource.usable = '0'
            db.session.flush()
            dm.unregister(search_datasource.name)
            return CommonResult.success()
        else:
            return CommonResult.fail(MessageCode.NOT_FOUND.code, message=f"{datasource_id}未找到")

    @transactional
    def modify(self, data: str) -> dict:
        m = munch.Munch(simplejson.loads(data))
        models.SearchDatasource.query.filter_by(id=m.id).update(m)
        db.session.flush()
        dm.register(models.SearchDatasource.query.filter_by(id=m.id).first())
        return CommonResult.success()


datasource_config = DataSourceConfig()
