# @Time    : 2023/03/23 14:33
# @Author  : fyq
# @File    : search_config.py
# @Software: PyCharm

__author__ = 'fyq'

from abc import ABCMeta, abstractmethod
from typing import List, Dict, Tuple

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
from search.util import repeat
from search.util.convert import data2obj


class ISearchConfig(metaclass=ABCMeta):

    @abstractmethod
    def info(self, search_id):
        pass

    @abstractmethod
    def search(self, data: dict) -> Dict:
        pass

    @abstractmethod
    def condition(self, search_id) -> Dict:
        pass

    @abstractmethod
    def sql(self, search_id) -> Dict:
        pass

    @abstractmethod
    def field(self, search_id) -> Dict:
        pass

    @abstractmethod
    def sort(self, search_id) -> Dict:
        pass

    @abstractmethod
    def add(self, data: str) -> Dict:
        pass

    @abstractmethod
    def modify(self, data: str) -> Dict:
        pass

    @abstractmethod
    def parse(self, search_id, version) -> Dict:
        pass

    @abstractmethod
    def usable(self, search_id, version):
        pass

    @abstractmethod
    def disable(self, search_id, version):
        pass

    @abstractmethod
    def search_parameter(self, data: dict) -> Dict:
        pass

    @abstractmethod
    def modify_search_parameter(self, data: str):
        pass


class SearchConfig(ISearchConfig):

    def sort(self, search_id) -> Dict:
        search_sort_list: List[models.SearchSort] = \
            models.SearchSort.query.filter_by(search_id=search_id).order_by(models.SearchSort.order).all()
        return CommonResult.success(data=search_sort_list)

    def search_parameter(self, data: dict) -> Dict:
        m = munch.Munch(data)
        page_number = int(m.pop("pageNumber", 1))
        page_size = int(m.pop("pageSize", 50))
        q = []
        s_q = models.SearchParameter.query
        for k, v in m.items():
            if v:
                q.append(getattr(models.SearchParameter, k).like(f"%{v}%"))
        if len(q) > 0:
            s_q = s_q.filter(*q)
        p: QueryPagination = s_q.paginate(page=page_number, per_page=page_size, error_out=False)
        return CommonResult.success(data={"list": [s.to_dict() for s in p], "total": p.total})

    @transactional
    def modify_search_parameter(self, data: str):
        m = munch.Munch(simplejson.loads(data))
        sp: models.SearchParameter = models.SearchParameter.query.filter_by(id=m.id, version=m.version).first()
        if not sp:
            raise SearchException(f"参数[{sp.display}]未找到到或者数据版本不正确")

        sp.version = m.version + 1
        sp.value = m.value

        search_parameter_list: List[models.SearchParameter] = models.SearchParameter.query.all()
        r = redis.Redis(connection_pool=redis_pool)
        r.set(name=constant.RedisKey.SEARCH_CONFIG,
              value=simplejson.dumps([_.to_dict() for _ in search_parameter_list]))

        return CommonResult.success()

    @transactional
    def usable(self, search_id, version):
        search: models.Search = models.Search.query.filter_by(id=search_id, version=version).first()
        if not search:
            raise SearchException(f"查询[{search.name}]未找到到或者数据版本不正确")
        search.usable = "1"
        return CommonResult.success()

    @transactional
    def disable(self, search_id, version):
        search: models.Search = models.Search.query.filter_by(id=search_id, version=version).first()
        if not search:
            raise SearchException(f"查询[{search.name}]未找到到或者数据版本不正确")
        search.usable = "0"
        return CommonResult.success()

    def info(self, search_id) -> Dict:
        search: models.Search = models.Search.query.filter_by(id=search_id).first()
        search_sql_list: List[models.SearchSQL] = \
            models.SearchSQL.query.filter_by(search_id=search_id).order_by(models.SearchSQL.order).all()
        search_field_list: List[models.SearchField] = \
            models.SearchField.query.filter_by(search_id=search_id).order_by(models.SearchField.order).all()
        search_condition_list: List[models.SearchCondition] = \
            models.SearchCondition.query.filter_by(search_id=search_id).order_by(models.SearchCondition.order).all()
        search_field_list: List[munch.Munch] = [munch.Munch(search_field.to_dict())
                                                for search_field in search_field_list]
        search_sort_list: List[models.SearchSort] = (
            models.SearchSort.query.filter_by(search_id=search_id).order_by(models.SearchSort.order).all()
        )
        for search_field in search_field_list:
            search_field["result_fields"] = list(search_field.result_fields.split(","))
        res = {
            "search": search.to_dict(),
            "searchCondition": [search_condition.to_dict() for search_condition in search_condition_list],
            "searchSql": [search_sql.to_dict() for search_sql in search_sql_list],
            "searchField": search_field_list,
            "searchSort": [search_sort.to_dict() for search_sort in search_sort_list]
        }

        return CommonResult.success(data=res)

    def parse(self, search_id, version) -> Dict:
        search: models.Search = models.Search.query.filter_by(id=search_id, version=version).first()
        if not search:
            raise SearchException(f"查询[{search.name}]未找到到或者数据版本不正确")

        if search.status in [constant.SearchStatus.PARSING]:
            thread_pool.submit(self._parse_search, search_id, current_app._get_current_object())
            return CommonResult.success()
        else:
            return CommonResult.fail(code=MessageCode.NOT_FOUND.code, message="未找到需要解析的查询")

    def modify(self, data: str) -> Dict:
        m = munch.Munch(simplejson.loads(data))
        search_id, search_name = self._modify(m)
        thread_pool.submit(self._parse_search, search_id, current_app._get_current_object())
        return CommonResult.success()

    def add(self, data: str) -> Dict:
        m = munch.Munch(simplejson.loads(data))
        search_id, search_name = self._add(m)
        thread_pool.submit(self._parse_search, search_id, current_app._get_current_object())
        return CommonResult.success()

    def sql(self, search_id) -> Dict:
        search_sql_list: List[models.SearchSQL] = \
            models.SearchSQL.query.filter_by(search_id=search_id).order_by(models.SearchSQL.order).all()
        return CommonResult.success(data=search_sql_list)

    def field(self, search_id) -> Dict:
        search_field_list: List[models.SearchField] = \
            models.SearchField.query.filter_by(search_id=search_id).order_by(models.SearchField.order).all()
        search_field_list: List[munch.Munch] = [munch.Munch(search_field.to_dict())
                                                for search_field in search_field_list]
        for search_field in search_field_list:
            search_field["result_fields"] = list(search_field.result_fields.split(","))
        return CommonResult.success(data=search_field_list)

    def search(self, data: dict) -> Dict:
        m = munch.Munch(data)
        page_number = int(m.pop("pageNumber", 1))
        page_size = int(m.pop("pageSize", 50))
        q = []
        s_q = models.Search.query
        for k, v in m.items():
            if v:
                q.append(getattr(models.Search, k).like(f"%{v}%"))
        if len(q) > 0:
            s_q = s_q.filter(*q)
        p: QueryPagination = s_q.paginate(page=page_number, per_page=page_size, error_out=False)
        return CommonResult.success(data={"list": [s.to_dict() for s in p], "total": p.total})

    def condition(self, search_id) -> Dict:
        search_condition_list: List[models.SearchCondition] = \
            models.SearchCondition.query.filter_by(search_id=search_id).order_by(models.SearchCondition.order).all()
        return CommonResult.success(data=search_condition_list)

    @staticmethod
    def _parse_search(search_id: int, app: Flask):
        with app.app_context():
            search: models.Search = \
                models.Search.query.filter_by(id=search_id).first()
            try:
                search_parse = SearchParser()
                search_parse.parse(search_id)
            except Exception as e:
                search.status = constant.SearchStatus.PARSING_ERROR
                search.error = str(e)
                db.session.commit()
                logger.exception(e)

    @classmethod
    @transactional
    def _add(cls, m: munch.Munch) -> Tuple[int, str]:
        search: models.Search = data2obj(m.search, models.Search)
        search_condition_list: List[models.SearchCondition] = data2obj(m.searchCondition, models.SearchCondition)
        repeat_value = repeat(search_condition_list, "name")
        if len(repeat_value) > 0:
            raise SearchException(f"查询[{search.search_name}]条件名称存在重复[{repeat_value}]")
        search_sql_list: List[models.SearchSQL] = data2obj(m.searchSql, models.SearchSQL)
        repeat_value = repeat(search_sql_list, "name")
        if len(repeat_value) > 0:
            raise SearchException(f"查询[{search.search_name}]sql名称存在重复[{repeat_value}]")
        for sf in m.searchField:
            sf["result_fields"] = ",".join(sf["result_fields"])
        search_field_list: List[models.SearchField] = data2obj(m.searchField, models.SearchField)
        repeat_value = repeat(search_field_list, "name")
        if len(repeat_value) > 0:
            raise SearchException(f"查询[{search.search_name}]字段名称存在重复[{repeat_value}]")
        search_sort_list: List[models.SearchSort] = data2obj(m.searchSort, models.SearchSort)
        repeat_value = repeat(search_field_list, "field_name")
        if len(repeat_value) > 0:
            raise SearchException(f"查询[{search.search_name}]排序字段存在重复[{repeat_value}]")
        search.status = constant.SearchStatus.PARSING
        db.session.add(search)
        db.session.flush()
        for search_condition in search_condition_list:
            search_condition.search_id = search.id
        for search_sql in search_sql_list:
            search_sql.search_id = search.id
        for search_field in search_field_list:
            search_field.search_id = search.id
        for search_sort in search_sort_list:
            search_sort.search_id = search
        db.session.add_all(search_condition_list)
        db.session.add_all(search_sql_list)
        db.session.add_all(search_field_list)
        db.session.add_all(search_sort_list)

        return search.id, search.name

    @classmethod
    @transactional
    def _modify(cls, m: munch.Munch) -> Tuple[int, str]:
        search_id = m.search["id"]
        search_name = m.search["name"]
        version = m.search["version"]
        m.search["status"] = constant.SearchStatus.PARSING
        m.search["error"] = ""
        search: models.Search = models.Search.query.filter_by(id=search_id, version=version).first()
        if not search:
            raise SearchException(f"查询[{search_name}]未找到到或者数据版本不正确")
        elif search.status in [constant.SearchStatus.PARSING]:
            raise SearchException(f"查询[{search_name}]正在解析中不允许修改")
        m.search["version"] = version + 1
        models.Search.query.filter_by(id=search_id).update(m.search)
        models.SearchCondition.query.filter_by(search_id=search_id).delete()
        models.SearchSQL.query.filter_by(search_id=search_id).delete()
        models.SearchField.query.filter_by(search_id=search_id).delete()
        models.SearchSort.query.filter_by(search_id=search_id).delete()
        db.session.flush()

        search_condition_list: List[models.SearchCondition] = data2obj(m.searchCondition, models.SearchCondition)
        repeat_value = repeat(search_condition_list, "name")
        if len(repeat_value) > 0:
            raise SearchException(f"查询[{search_name}]条件名称存在重复[{repeat_value}]")
        search_sql_list: List[models.SearchSQL] = data2obj(m.searchSql, models.SearchSQL)
        repeat_value = repeat(search_sql_list, "name")
        if len(repeat_value) > 0:
            raise SearchException(f"查询[{search_name}]sql名称存在重复[{repeat_value}]")
        for sf in m.searchField:
            sf["result_fields"] = ",".join(sf["result_fields"])
        search_field_list: List[models.SearchField] = data2obj(m.searchField, models.SearchField)
        repeat_value = repeat(search_field_list, "name")
        if len(repeat_value) > 0:
            raise SearchException(f"查询[{search_name}]字段名称存在重复[{repeat_value}]")
        search_sort_list: List[models.SearchSort] = data2obj(m.searchSort, models.SearchSort)
        repeat_value = repeat(search_field_list, "field_name")
        if len(repeat_value) > 0:
            raise SearchException(f"查询[{search_name}]排序字段存在重复[{repeat_value}]")

        for search_condition in search_condition_list:
            search_condition.search_id = search_id
            search_condition.id = None
        for search_sql in search_sql_list:
            search_sql.search_id = search_id
            search_sql.id = None
        for search_field in search_field_list:
            search_field.search_id = search_id
            search_field.id = None
        for search_sort in search_sort_list:
            search_sort.search_id = search_id
            search_sort.id = None

        db.session.add_all(search_condition_list)
        db.session.add_all(search_sql_list)
        db.session.add_all(search_field_list)
        db.session.add_all(search_sort_list)

        scm.delete_search_context(search_name)

        return search_id, search_name


search_config = SearchConfig()
