# @Time    : 2023/03/23 14:33
# @Author  : fyq
# @File    : search_config.py
# @Software: PyCharm

__author__ = 'fyq'

from abc import ABCMeta, abstractmethod
from copy import copy
from typing import List

import munch
import simplejson
from flask import Flask, current_app
from flask_sqlalchemy.pagination import QueryPagination
from loguru import logger

from search import models, CommonResult, db, constant
from search.core.decorator import transactional
from search.core.parser import SearchSqlParser, SqlParserInfo
from search.exceptions import SearchSqlParseException
from search.extend import thread_pool
from search.util.convert import data2obj



class ISearchConfig(metaclass=ABCMeta):

    @abstractmethod
    def get_search(self, data: dict):
        pass

    @abstractmethod
    def get_search_condition(self, search_id):
        pass

    @abstractmethod
    def get_search_sql(self, search_id):
        pass

    @abstractmethod
    def get_search_field(self, search_id):
        pass

    @abstractmethod
    def search_add(self, data: str):
        pass

    @abstractmethod
    def search_modify(self, data: str):
        pass


class SearchConfig(ISearchConfig):

    @transactional
    def search_modify(self, data: str):
        m = munch.Munch(simplejson.loads(data))
        search_id = m.search["id"]
        models.Search.query.filter_by(id=search_id).update(m.search)
        models.SearchCondition.filter_by(id=search_id).delete()
        models.SearchSQL.filter_by(id=search_id).delete()
        models.SearchField.filter_by(id=search_id).delete()
        db.session.flush()

        search_condition_list: List[models.SearchCondition] = data2obj(m.searchCondition, models.SearchCondition)
        search_sql_list: List[models.SearchSQL] = data2obj(m.searchSql, models.SearchSQL)
        search_field_list: List[models.SearchField] = data2obj(m.searchField, models.SearchField)

        for search_condition in search_condition_list:
            search_condition.search_id = search_id
        for search_sql in search_sql_list:
            search_sql.search_id = search_id
        for search_field in search_field_list:
            search_field.search_id = search_id

        db.session.add_all(search_condition_list)
        db.session.add_all(search_sql_list)
        db.session.add_all(search_field_list)

        search: models.Search = models.Search.query.filter_by(id=search_id).first(0)
        search.status = constant.SearchStatus.PARSING

        # 删除缓存 和 上下文

        thread_pool.submit(self._parse_search, search_id, current_app._get_current_object())

    @transactional
    def search_add(self, data: str):
        m = munch.Munch(simplejson.loads(data))
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

        thread_pool.submit(self._parse_search, search.id, current_app._get_current_object())

        return CommonResult.success()

    def get_search_sql(self, search_id):
        search_sql_list: List[models.SearchSQL] = \
            models.SearchSQL.query.filter_by(search_id=search_id).order_by(models.SearchSQL.order).all()
        return CommonResult.success(data=search_sql_list)

    def get_search_field(self, search_id):
        search_field_list: List[models.SearchField] = \
            models.SearchField.query.filter_by(search_id=search_id).order_by(models.SearchField.order).all()
        return CommonResult.success(data=search_field_list)

    def get_search(self, data: dict):
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

    def get_search_condition(self, search_id):
        search_condition_list: List[models.SearchCondition] = \
            models.SearchCondition.query.filter_by(search_id=search_id).order_by(models.SearchCondition.order).all()
        return CommonResult.success(data=search_condition_list)

    @staticmethod
    @transactional
    def _parse_search(search_id: int, app: Flask):
        with app.app_context():
            pass


search_config = SearchConfig()
