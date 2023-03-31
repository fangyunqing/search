# @Time    : 2023/03/23 14:33
# @Author  : fyq
# @File    : search_config.py
# @Software: PyCharm

__author__ = 'fyq'

from abc import ABCMeta, abstractmethod
from typing import List, Dict

import munch
import redis
import simplejson
from flask import Flask, current_app
from flask_sqlalchemy.pagination import QueryPagination
from loguru import logger

from search import models, CommonResult, db, constant, MessageCode
from search.core.decorator import transactional
from search.core.parser import SearchParser
from search.core.search_context import scm
from search.exceptions import SearchException
from search.extend import thread_pool, redis_pool
from search.util.convert import data2obj


class ISearchConfig(metaclass=ABCMeta):

    @abstractmethod
    def get_search_info(self, search_id):
        pass

    @abstractmethod
    def get_search(self, data: dict) -> Dict:
        pass

    @abstractmethod
    def get_search_condition(self, search_id) -> Dict:
        pass

    @abstractmethod
    def get_search_sql(self, search_id) -> Dict:
        pass

    @abstractmethod
    def get_search_field(self, search_id) -> Dict:
        pass

    @abstractmethod
    def search_add(self, data: str) -> Dict:
        pass

    @abstractmethod
    def search_modify(self, data: str) -> Dict:
        pass

    @abstractmethod
    def search_parse(self, search_id) -> Dict:
        pass


class SearchConfig(ISearchConfig):

    def get_search_info(self, search_id) -> Dict:
        search: models.Search = models.Search.query.filter_by(id=search_id).first()
        search_sql_list: List[models.SearchSQL] = \
            models.SearchSQL.query.filter_by(search_id=search_id).order_by(models.SearchSQL.order).all()
        search_field_list: List[models.SearchField] = \
            models.SearchField.query.filter_by(search_id=search_id).order_by(models.SearchField.order).all()
        search_condition_list: List[models.SearchCondition] = \
            models.SearchCondition.query.filter_by(search_id=search_id).order_by(models.SearchCondition.order).all()

        res = {
            "search": search.to_dict(),
            "searchCondition": [search_condition.to_dict() for search_condition in search_condition_list],
            "searchSql": [search_sql.to_dict() for search_sql in search_sql_list],
            "searchField": [search_field.to_dict() for search_field in search_field_list]
        }

        return CommonResult.success(data=res)

    def search_parse(self, search_id) -> Dict:
        search: models.Search = \
            models.Search.query.filter_by(id=search_id).first()
        if search.status in [constant.SearchStatus.ERROR, constant.SearchStatus.PARSING]:
            thread_pool.submit(self._parse_search, search_id, current_app._get_current_object())
            return CommonResult.success()
        else:
            return CommonResult.fail(code=MessageCode.NOT_FOUND.code, message="未找到需要解析的查询")

    @transactional
    def search_modify(self, data: str) -> Dict:
        m = munch.Munch(simplejson.loads(data))
        search_id = self._modify(m)
        scm.delete_search_context(search_id)
        thread_pool.submit(self._parse_search, search_id, current_app._get_current_object())
        return CommonResult.success()

    @transactional
    def search_add(self, data: str) -> Dict:
        m = munch.Munch(simplejson.loads(data))
        search_id = self._add(m)
        thread_pool.submit(self._parse_search, search_id, current_app._get_current_object())

        return CommonResult.success()

    def get_search_sql(self, search_id) -> Dict:
        search_sql_list: List[models.SearchSQL] = \
            models.SearchSQL.query.filter_by(search_id=search_id).order_by(models.SearchSQL.order).all()
        return CommonResult.success(data=search_sql_list)

    def get_search_field(self, search_id) -> Dict:
        search_field_list: List[models.SearchField] = \
            models.SearchField.query.filter_by(search_id=search_id).order_by(models.SearchField.order).all()
        return CommonResult.success(data=search_field_list)

    def get_search(self, data: dict) -> Dict:
        m = munch.Munch(data)
        page_number = int(m.pop("pageNumber", 1))
        page_size = int(m.pop("pageSize", 50))
        q = []
        s_q = models.Search.query
        for k, v in m.items():
            if v:
                q.append(getattr(models.Search, k).like(f"%{v}%"))
        if len(q) > 0:
            s_q.filter(*q)
        p: QueryPagination = s_q.paginate(page=page_number, per_page=page_size, error_out=False)
        return CommonResult.success(data={"list": [s.to_dict() for s in p], "total": p.total})

    def get_search_condition(self, search_id) -> Dict:
        search_condition_list: List[models.SearchCondition] = \
            models.SearchCondition.query.filter_by(search_id=search_id).order_by(models.SearchCondition.order).all()
        return CommonResult.success(data=search_condition_list)

    @staticmethod
    def _parse_search(search_id: int, app: Flask):
        with app.app_context():
            search: models.Search = \
                models.Search.query.filter_by(id=search_id).first()
            if search.status not in [constant.SearchStatus.PARSING, constant.SearchStatus.ERROR]:
                return
            r = redis.Redis(connection_pool=redis_pool)
            key = f"{search.name}_{constant.RedisKeySuffix.SEARCH_PARSE}"
            if r.setnx(name=key, value=1):
                try:
                    search_parse = SearchParser()
                    search_parse.parse(search_id)
                except Exception as e:
                    search.status = constant.SearchStatus.ERROR
                    search.error = str(e)
                    db.session.commit()
                    logger.exception(e)
                finally:
                    r.delete(key)

    @classmethod
    @transactional
    def _add(cls, m: munch.Munch) -> int:
        search: models.Search = data2obj(m.search, models.Search)
        search_condition_list: List[models.SearchCondition] = data2obj(m.searchCondition, models.SearchCondition)
        search_sql_list: List[models.SearchSQL] = data2obj(m.searchSql, models.SearchSQL)
        search_field_list: List[models.SearchField] = data2obj(m.searchField, models.SearchField)
        db.session.add(search)
        db.session.flush()
        for search_condition in search_condition_list:
            search_condition.search_id = search.id
        for search_sql in search_sql_list:
            search_sql.search_id = search.id
        for search_field in search_field_list:
            search_field.search_id = search.id
        db.session.add_all(search_condition_list)
        db.session.add_all(search_sql_list)
        db.session.add_all(search_field_list)

        return search.id

    @classmethod
    @transactional
    def _modify(cls, m: munch.Munch) -> int:
        search_id = m.search["id"]
        search_name = m.search["name"]
        version = m.search["version"]
        m.search["status"] = constant.SearchStatus.PARSING
        search: models.Search = models.Search.query.filter_by(id=search_id, version=version).first()
        if not search:
            raise SearchException(f"查询[{search_name}]未找到到或者数据版本不正确")
        m.search["version"] = version + 1
        m.search["status"] = constant.SearchStatus.PARSING
        models.Search.query.filter_by(id=search_id).update(m.search)
        models.SearchCondition.query.filter_by(search_id=search_id).delete()
        models.SearchSQL.query.filter_by(search_id=search_id).delete()
        models.SearchField.query.filter_by(search_id=search_id).delete()
        db.session.flush()

        search_condition_list: List[models.SearchCondition] = data2obj(m.searchCondition, models.SearchCondition)
        search_sql_list: List[models.SearchSQL] = data2obj(m.searchSql, models.SearchSQL)
        search_field_list: List[models.SearchField] = data2obj(m.searchField, models.SearchField)

        for search_condition in search_condition_list:
            search_condition.search_id = search_id
            search_condition.id = None
        for search_sql in search_sql_list:
            search_sql.search_id = search_id
            search_sql.id = None
        for search_field in search_field_list:
            search_field.search_id = search_id
            search_field.id = None

        db.session.add_all(search_condition_list)
        db.session.add_all(search_sql_list)
        db.session.add_all(search_field_list)

        return search_id


search_config = SearchConfig()
