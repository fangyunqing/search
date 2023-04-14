# @Time    : 23/03/02 13:35
# @Author  : fyq
# @File    : search_context.py
# @Software: PyCharm

__author__ = 'fyq'

import copy
import hashlib
import time
from abc import ABCMeta, abstractmethod
from concurrent.futures import Future
from dataclasses import dataclass, field
from typing import List, Dict, Set

import redis
import simplejson
from munch import Munch
from sortedcontainers import SortedKeyList

import search.constant as constant
from search import models
from search.core.search_md5 import SearchMd5
from search.entity.common_result import BaseDataClass
from search.exceptions import SearchException
from search.extend import redis_pool
from search.util.convert import data2munch, data2DictOrList


@dataclass
class SearchBuffer(BaseDataClass):
    # sql信息
    search_sql: Munch = None
    # sql字段信息
    search_sql_fields: List[Munch] = None
    # sql结果字段
    search_sql_results: List[Munch] = None
    # sql条件字段
    search_sql_conditions: List[Munch] = None
    # 字段集合 名称 别名
    field_list: List[str] = field(default_factory=lambda: [])
    # 临时表集合 名称 别名
    tmp_field_list: List[str] = field(default_factory=lambda: [])
    # 查询的字段
    select_fields: Set[str] = field(default_factory=lambda: set())
    # 临时的字段
    tmp_fields: Set[str] = field(default_factory=lambda: set())
    # 参数
    args: List[str] = field(default_factory=lambda: [])
    # 参数顺序
    args_seq: List[str] = field(default_factory=lambda: [])
    # 连接的字段
    join_fields: List[str] = field(default_factory=lambda: [])
    # from表达式
    where_expression: str = ""
    # 临时表名
    tmp_tablename: str = None


@dataclass
class SearchContext(BaseDataClass):
    # 查询
    search: Munch = None
    # 查询字段
    search_field_list: List[Munch] = None
    # 查询条件
    search_condition_dict: Dict[str, Munch] = None
    # 查询的数据
    search_md5: Munch = None
    # key
    search_key: str = None
    # 查询缓存建
    search_buffer_list: List[Munch] = field(default_factory=lambda: [])


class ISearchContextManager(metaclass=ABCMeta):

    @abstractmethod
    def get_search_context(self, search_md5: SearchMd5) -> SearchContext:
        pass

    @abstractmethod
    def delete_search_context(self, search_name: str):
        pass


class SearchContextManager(ISearchContextManager):

    def __init__(self):
        pass

    def get_search_context(self, search_md5: SearchMd5) -> SearchContext:

        search: models.Search = \
            models.Search.query.filter_by(name=search_md5.search_name).first()
        if not search:
            raise SearchException(f"{search_md5.search_name}不存在")
        if search.usable == "0" or search.status != constant.SearchStatus.ACCESS:
            raise SearchException(f"{search_md5.search_name}不可用")

        r = redis.Redis(connection_pool=redis_pool)

        while not r.setnx(name=constant.RedisKeySuffix.SEARCH_CONTEXT, value=1):
            time.sleep(100)

        try:
            key = f"{search.name}_v{search.version}_" + search_md5.search_md5
            value: bytes = r.get(key)
            if value:
                m = data2munch(simplejson.loads(value.decode()))
                sc = SearchContext()
                sc.search = m.search
                sc.search_key = m.search_key
                sc.search_md5 = m.search_md5
                sc.search_buffer_list = m.search_buffer_list
                sc.search_field_list = m.search_field_list
                sc.search_condition_dict = m.search_condition_dict
                return sc
            else:
                # 查询sql
                search_sql_list: List[models.SearchSQL] = models.SearchSQL.query.filter_by(
                    search_id=search.id).order_by(models.SearchSQL.order).all()
                # 查询字段
                search_field_list: List[models.SearchField] = models.SearchField.query.filter_by(
                    search_id=search.id).order_by(models.SearchField.order).all()
                search_sql_dict: Dict[int, models.SearchSQL] = {search_sql.id: search_sql
                                                                for search_sql in search_sql_list}
                # 查询条件
                search_condition_list: List[models.SearchCondition] = models.SearchCondition.query.filter_by(
                    search_id=search.id).order_by(models.SearchCondition.order).all()

                search_buffer_dict: Dict[int, SearchBuffer] = {}
                for search_field in search_field_list:
                    if search_field.name in search_md5.search_sort_field_list:
                        for sp in search_field.search_field_gen_paths:
                            if sp.search_sql_id in search_buffer_dict:
                                search_buffer = search_buffer_dict[sp.search_sql_id]
                            else:
                                search_buffer = self._pack_search_buffer(search_sql_dict[sp.search_sql_id])
                                search_buffer_dict[sp.search_sql_id] = search_buffer
                            search_buffer.select_fields.add(sp.depend_field)
                            if sp.order > 0:
                                search_buffer.tmp_fields.add(sp.depend_field)

                copy_search_sql_list = copy.copy(search_sql_list)
                for condition in search_md5.search_sort_condition_list:
                    if len(copy_search_sql_list) > 0:
                        for index, search_sql in enumerate(copy_search_sql_list):
                            if condition in [search_sql_condition.real_right
                                             for search_sql_condition in search_sql.conditions]:
                                if search_sql.id not in search_buffer_dict.keys():
                                    search_buffer_dict[search_sql.id] = self._pack_search_buffer(search_sql)
                                if search_sql.depend and len(search_sql.depend) > 0:
                                    for depend_search_sql_id in [int(search_sql_id)
                                                                 for search_sql_id in search_sql.depend.split(",")]:
                                        if depend_search_sql_id not in search_buffer_dict.keys():
                                            search_buffer_dict[search_sql.id] = \
                                                self._pack_search_buffer(search_sql_dict[depend_search_sql_id])
                                copy_search_sql_list.pop(index)
                                break

                for search_sql_id, search_buffer in search_buffer_dict.items():
                    for search_sql_result in search_buffer.search_sql_results:
                        target_search_buffer = search_buffer_dict[search_sql_result.depend_search_sql_id]
                        target_search_buffer.select_fields.add(search_sql_result.real_right)
                        target_search_buffer.tmp_fields.add(search_sql_result.real_right)

                search_buffer_list = SortedKeyList(iterable=search_buffer_dict.values(),
                                                   key=lambda item: item.search_sql.order)

                sc = SearchContext()
                sc.search = Munch(search.to_dict())
                sc.search_md5 = search_md5
                sc.search_key = key
                sc.search_buffer_list = [Munch(search_buffer.to_dict()) for search_buffer in search_buffer_list]
                sc.search_field_list = [Munch(search_field.to_dict()) for search_field in search_field_list]
                sc.search_condition_dict = {search_condition.name: Munch(search_condition.to_dict())
                                            for search_condition in search_condition_list}

                self._field(search_context=sc, search_md5=search_md5)
                self._condition(search_context=sc, search_md5=search_md5)

                r.setex(name=key, value=simplejson.dumps(data2DictOrList(sc.to_dict())), time=432000)

                return sc
        finally:
            r.delete(constant.RedisKeySuffix.SEARCH_CONTEXT)

    def delete_search_context(self, search_name: str):
        r = redis.Redis(connection_pool=redis_pool)
        while not r.setnx(name=constant.RedisKeySuffix.SEARCH_CONTEXT, value=1):
            time.sleep(100)
        try:
            search: models.Search = \
                models.Search.query.filter_by(name=search_name).first()
            all_keys = r.keys(pattern=f"{search.name}_*")
            if len(all_keys) > 0:
                r.delete(*all_keys)
        finally:
            r.delete(constant.RedisKeySuffix.SEARCH_CONTEXT)

    @classmethod
    def _condition(cls, search_context: SearchContext, search_md5: SearchMd5):
        for search_buffer in search_context.search_buffer_list:
            del_logic = False
            where_expression_list = []
            for fe in search_buffer.where_expression.split():
                if fe.startswith("condition."):
                    condition_name = fe.split(".")[1]
                    search_condition = search_context.search_condition_dict.get(condition_name, None)
                    if search_condition and condition_name in search_md5.search_sort_condition_list:
                        if search_condition.fuzzy_query == "1":
                            where_expression_list.append("'%'+%s+'%'")
                            where_expression_list[-2] = "LIKE"
                        else:
                            where_expression_list.append("%s")
                        search_buffer.args.append(search_md5.search_conditions[condition_name])
                        search_buffer.args_seq.append(condition_name)
                    else:
                        find_search_sql_condition_list = \
                            [search_sql_condition for search_sql_condition in search_buffer.search_sql_conditions
                             if condition_name == search_sql_condition.right.split(".")[-1]]
                        if len(find_search_sql_condition_list) == 0:
                            raise SearchException(f"{search_md5.search_name}不可描述的错误")
                        find_search_sql_condition = find_search_sql_condition_list[0]
                        for index in range(len(where_expression_list) - 1, -1, -1):
                            if not where_expression_list[index].endswith(find_search_sql_condition.left):
                                where_expression_list.pop(index)
                            else:
                                left = where_expression_list[index].replace(find_search_sql_condition.left, "").strip()
                                if len(left) == 0:
                                    where_expression_list.pop(index)
                                else:
                                    where_expression_list[index] = left
                                break

                        for index in range(len(where_expression_list) - 1, -1, -1):
                            if where_expression_list[index] in ["AND", "OR"]:
                                where_expression_list.pop(index)
                                break
                            elif where_expression_list[index] == "WHERE":
                                del_logic = True
                                break
                else:

                    if fe not in ["AND", "OR"] or not del_logic:
                        where_expression_list.append(fe)
                    elif fe in ["AND", "OR"] and del_logic:
                        del_logic = False
            search_buffer.where_expression = " ".join(where_expression_list)
            while "( )" in search_buffer.where_expression:
                search_buffer.where_expression = search_buffer.where_expression.replace("( )", "")

    @classmethod
    def _field(cls, search_context: SearchContext, search_md5: SearchMd5):
        tmp_table: Dict[str, str] = {}
        for search_buffer in search_context.search_buffer_list:
            group_by = False
            if search_buffer.search_sql.other_expression and 'GROUP BY' in search_buffer.search_sql.other_expression:
                group_by = True
            # 临时表名
            tmp_md5 = hashlib.md5(",".join(search_buffer.select_fields).encode(encoding='utf-8')).hexdigest()
            search_buffer.tmp_tablename = "#{}" + "_" + str(search_buffer.search_sql.id) + "_" + tmp_md5
            # 临时字段对应临时表明
            for tmp_field in search_buffer.tmp_fields:
                tmp_table[tmp_field] = search_buffer.tmp_tablename

            for sf in search_buffer.search_sql_fields:
                if sf.right.split(".")[-1] in search_buffer.select_fields:
                    if sf.left:
                        search_buffer.field_list.append(sf.left + " " + sf.right)
                    else:
                        search_buffer.field_list.append(sf.right)

                if sf.right.split(".")[-1] in search_buffer.tmp_fields:
                    if sf.left:
                        search_buffer.tmp_field_list.append(sf.left + " " + sf.right)
                    else:
                        search_buffer.tmp_field_list.append(sf.right)

            search_buffer.select_fields = [f.split()[-1].split(".")[-1] for f in search_buffer.field_list]
            search_buffer.tmp_fields = [f.split()[-1].split(".")[-1] for f in search_buffer.tmp_field_list]
            where_expression_list = []
            for fe in search_buffer.search_sql.where_expression.split():
                if fe.startswith("result."):
                    field_name = fe.split(".")[-1]
                    for rf in search_buffer.search_sql_results:
                        if rf.right.split(".")[-1] == field_name:
                            search_buffer.select_fields.append(field_name)
                            if group_by:
                                search_buffer.field_list.append(f"MAX({rf.left}) {field_name}")
                            else:
                                search_buffer.field_list.append(f"{rf.left} {field_name}")
                            search_buffer.join_fields.append(field_name)
                            where_expression_list.append(
                                f"(select {field_name} from {tmp_table[field_name]})")
                            break

                else:
                    where_expression_list.append(fe)

            search_buffer.where_expression = " ".join(where_expression_list)

    @classmethod
    def _pack_search_buffer(cls, search_sql: models.SearchSQL) -> SearchBuffer:
        search_buffer = SearchBuffer()
        search_buffer.search_sql = Munch(search_sql.to_dict())
        search_buffer.search_sql_results = \
            [Munch(result.to_dict()) for result in search_sql.results]
        search_buffer.search_sql_fields = \
            [Munch(f.to_dict()) for f in search_sql.fields]
        search_buffer.search_sql_conditions = \
            [Munch(condition.to_dict()) for condition in search_sql.conditions]
        return search_buffer


scm = SearchContextManager()
