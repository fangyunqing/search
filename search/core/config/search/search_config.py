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
from search.extend import thread_pool
from search.models import SearchSQLGenField, SearchFieldGenPath
from search.util.convert import data2obj

from pyparsing import Keyword, Combine, Suppress, alphanums, Word, Literal, pyparsing_unicode, OneOrMore, Optional, \
    Group, nums


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
            models.SearchSQL.query.filter_by(search_id=search_id).all()
        return CommonResult.success(data=search_sql_list)

    def get_search_field(self, search_id):
        search_field_list: List[models.SearchField] = \
            models.SearchField.query.filter_by(search_id=search_id).all()
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
            models.SearchCondition.query.filter_by(search_id=search_id).all()
        return CommonResult.success(data=search_condition_list)

    @staticmethod
    @transactional
    def _parse_search(search_id: int, app: Flask):
        with app.app_context():
            try:
                search: models.Search = models.Search.query.filter_by(id=search_id).first()
                search_sql_list: List[models.SearchSQL] = models.SearchSQL.query.filter_by(search_id=search_id).all()
                search_field_list: List[models.SearchField] = models.SearchField.query.filter_by(
                    search_id=search_id).all()
                for search_sql in search_sql_list:
                    from_key = Keyword("from", caseless=True)
                    t, s, e = list(from_key.scan_string(search_sql.exp, max_matches=1))[0]
                    # from后面的表达式
                    from_exp = search_sql.exp[s:]
                    condition = Combine(
                        Suppress(Literal("{")) + Word(alphanums) + Literal(".") + Word(alphanums) + Suppress(
                            Literal("}")))
                    o = OneOrMore(Word(alphanums + "+-/|&.'=*()><!%,#" + pyparsing_unicode.alphanums))
                    condition = OneOrMore(Optional(o) + condition + Optional(o))

                    conditions: List[str] = condition.parse_string(from_exp).as_list()
                    search_sql.from_exp = ",".join(conditions)
                    condition_list: List[str] = []
                    result_list: List[str] = []
                    for c_index, c in enumerate(conditions):
                        if c.startswith("result."):
                            r, f = c.split(".")
                            result_list.append(f)

                            ret = models.SearchSqlResult()
                            ret.search_sql_id = search_sql.id
                            ret.result_field = f
                            ret.field_name = conditions[c_index - 2]

                            db.session.add(ret)

                        elif c.startswith("condition."):
                            condition_list.append(c.replace("condition.", ""))

                    search_sql.condition_fields = ",".join(condition_list)

                    # 字段
                    def fill_field_list(token):
                        field_list.append(token[0])

                    select_exp: str = search_sql.exp[0: s].strip()
                    field_list: List[str] = []
                    field_key = Group(OneOrMore(Word(alphanums + ".='" + pyparsing_unicode.alphanums))) + Suppress(
                        Optional(","))
                    field_key.set_parse_action(fill_field_list)
                    select_key = Keyword("select", caseless=True)
                    all_key = Keyword("all", caseless=True)
                    distinct_key = Keyword("distinct", caseless=True)
                    top_key = Keyword("top", caseless=True) + Word(nums)
                    select_key = Group(select_key + Optional(all_key) + Optional(distinct_key) + Optional(top_key))
                    select_key.set_parse_action(fill_field_list)
                    (select_key + OneOrMore(field_key)).parse_string(select_exp)
                    search_sql.select_exp = " ".join(field_list[0])
                    for o in field_list[1:]:
                        sqf = SearchSQLGenField()
                        sqf.search_sql_id = search_sql.id
                        sqf.gen_field = o[-1]
                        if len(o) > 1:
                            sqf.exp_field = " ".join(o[0:len(o) - 1])

                        db.session.add(sqf)
                db.session.flush()

                def find(datas, fields, node, order):
                    for field in fields.split(","):
                        for i, data in enumerate(datas):
                            gen_fields = [sss.gen_field for sss in data.search_sql_gen_fields]
                            if field in gen_fields:
                                sf = SearchFieldGenPath()
                                sf.search_sql_id = data.id
                                sf.depend_field = field
                                sf.search_field_id = node.id
                                db.session.add(sf)
                                sf.order = order
                                new_datas = list(copy(datas))
                                new_datas.pop(i)
                                find(new_datas, ",".join(
                                    [rett.result_field for rett in data.result_fields]), node, order + 1)
                                break

                search_sql_list = models.SearchSQL.query.all()
                for sfl in search_field_list:
                    find(search_sql_list, sfl.result_fields, sfl, 0)
                search.status = constant.SearchStatus.TEST
                db.session.commit()
            except Exception as e:
                logger.exception(e)
                raise e


search_config = SearchConfig()
