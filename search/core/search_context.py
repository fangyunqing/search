# @Time    : 23/03/02 13:35
# @Author  : fyq
# @File    : search_context.py
# @Software: PyCharm

__author__ = 'fyq'

import copy
import hashlib
import time
from concurrent.futures import Future
from dataclasses import dataclass, field
from typing import List, Dict, Set, Optional

from sortedcontainers import SortedKeyList

from search import models
import search.constant as constant
import redis

from search.core.search_md5 import SearchMd5
from search.exceptions import SearchException
from search.extend import redis_pool
import simplejson as json
from munch import Munch


@dataclass
class SearchBuffer:
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
class SearchContext:
    # 查询
    search: Munch = None
    # 查询字段
    search_field_list: List[Munch] = None
    # 查询线程
    search_future: Future = None
    # 导出线程
    export_future: Future = None
    # 查询的数据
    search_md5: SearchMd5 = None
    # 查询缓存建
    search_buffer_list: List[SearchBuffer] = field(default_factory=lambda: [])


class SearchContextManager:

    def __init__(self):
        self._cts: Dict[int, Dict[str, SearchContext]] = {}
        self._field_cts: Dict[int, Dict[str, SearchContext]] = {}
        self._condition_cts: Dict[int, Dict[str, SearchContext]] = {}

    def get_search_context(self, search_md5: SearchMd5) -> Optional[SearchContext]:
        r = redis.Redis(connection_pool=redis_pool)
        key = f"{search_md5}_{constant.SEARCH_CONTEXT}"
        while not r.setnx(name=key, value=1):
            time.sleep(100)
        try:
            search: models.Search = \
                models.Search.query.filter_by(name=search_md5.search_name).first()
            if not search:
                raise SearchException(f"{search_md5.search_name}不存在")
            if not search.usable or constant.SearchStatus.ACCESS:
                raise SearchException(f"{search_md5.search_name}不可用")

            ct = self._cts.setdefault(search.id, {}).get(search_md5.search_md5, None)
            field_ct = self._field_cts.setdefault(search.id, {}).get(search_md5.search_field_md5, None)
            condition_ct = self._condition_cts.setdefault(search.id, {}).get(search_md5.search_condition_md5, None)

            if ct:
                return ct
            elif condition_ct:
                ct.search_md5 = search_md5
                for search_buffer in ct.search_buffer_list:
                    search_buffer.args.clear()
                    for condition_name in search_buffer.args_seq:
                        search_buffer.args.append(search_md5.search_conditions.get(condition_name, None))
                self._cts.setdefault(search.id, {})[search_md5.search_md5] = ct
                return ct
            elif field_ct:
                ct.search_md5 = search_md5
                self._condition(ct, search_md5)
                self._condition_cts.setdefault(search.id, {})[search_md5.search_condition_md5] = copy.deepcopy(ct)
                self._cts.setdefault(search.id, {})[search_md5.search_md5] = ct
                return ct
            else:
                # 查询sql
                search_sql_list: List[models.SearchSQL] = models.SearchSQL.query.filter_by(
                    search_id=search.id).order_by(models.SearchSQL.order).all()
                # 查询字段
                search_field_list: List[models.SearchField] = models.SearchField.query.filter_by(
                    search_id=search.id).order_by(models.SearchField.order).all()

                search_buffer_dict: Dict[int, SearchBuffer] = {}

                for search_field in search_field_list:
                    if search_field.name in search_md5.search_sort_field_list:
                        for sp in search_field.search_field_gen_paths:
                            if sp.search_sql_id in search_buffer_dict:
                                search_buffer = search_buffer_dict[sp.search_sql_id]
                            else:

                                find_search_sql_list = \
                                    [search_sql for search_sql in search_sql_list if search_sql.id == sp.search_sql_id]
                                if len(find_search_sql_list) == 0:
                                    raise SearchException(f"{search_md5.search_name}不可描述的错误")
                                find_search_sql = find_search_sql_list[0]
                                search_buffer = SearchBuffer()
                                search_buffer.search_sql = Munch(find_search_sql.to_dict())
                                search_buffer.search_sql_results = \
                                    [Munch(result.to_dict()) for result in find_search_sql.results]
                                search_buffer.search_sql_fields = \
                                    [Munch(f.to_dict()) for f in find_search_sql.fields]
                                search_buffer.search_sql_conditions = \
                                    [Munch(condition.to_dict()) for condition in find_search_sql.conditions]
                                search_buffer_dict[sp.search_sql_id] = search_buffer
                            search_buffer.select_fields.add(sp.depend_field)
                            if sp.order > 0:
                                search_buffer.tmp_fields.add(sp.depend_field)

                search_buffer_list = SortedKeyList(iterable=search_buffer_dict.values(),
                                                   key=lambda item: item.search_sql.order)

                sc = SearchContext()
                sc.search = Munch(search.to_dict())
                sc.search_md5 = search_md5
                sc.search_buffer_list = [search_buffer for search_buffer in search_buffer_list]
                sc.search_field_list = [Munch(search_field.to_dict()) for search_field in search_field_list]

                self._field(search_context=sc, search_md5=search_md5)
                self._field_cts.setdefault(search.id, {})[search_md5.search_field_md5] = copy.deepcopy(sc)
                self._condition(search_context=sc, search_md5=search_md5)
                self._condition_cts.setdefault(search.id, {})[search_md5.search_condition_md5] = copy.deepcopy(sc)
                self._cts.setdefault(search.id, {})[search_md5.search_md5] = sc
        finally:
            r.delete(key)

    @classmethod
    def _condition(cls, search_context: SearchContext, search_md5: SearchMd5):
        where_expression_list = []
        for search_buffer in search_context.search_buffer_list:
            for fe in search_buffer.search_sql.where_expression.split():
                if fe.startswith("condition."):
                    condition_name = fe.split(".")[1]
                    if condition_name in search_md5.search_sort_condition_list:
                        where_expression_list.append("%s")
                        search_buffer.args.append(search_md5.search_conditions[condition_name])
                        search_buffer.args_seq.append(condition_name)
                    else:
                        find_search_sql_condition_list = \
                            [search_sql_condition for search_sql_condition in search_buffer.search_sql_conditions
                             if condition_name == search_sql_condition.right]
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
                else:
                    where_expression_list.append(fe)
            search_buffer.where_expression = " ".join(where_expression_list)
            while "()" in search_buffer.where_expression:
                search_buffer.where_expression.replace("()", "")

    @classmethod
    def _field(cls, search_context: SearchContext, search_md5: SearchMd5):
        tmp_table: Dict[str, str] = {}
        for search_buffer in search_context.search_buffer_list:
            # 临时表名
            tmp_md5 = hashlib.md5(",".join(search_buffer.select_fields).encode(encoding='utf-8')).hexdigest()
            search_buffer.tmp_tablename = "#{}" + "_" + str(search_buffer.search_sql.id) + "_" + tmp_md5
            # 临时字段对应临时表明
            for tmp_field in search_buffer.tmp_fields:
                tmp_table[tmp_field] = search_buffer.tmp_tablename

            for sf in search_buffer.search_sql_fields:
                if sf.right in search_buffer.select_fields:
                    if sf.left:
                        search_buffer.field_list.append(sf.left + " " + sf.right)
                    else:
                        search_buffer.field_list.append(sf.right)

                if sf.right in search_buffer.tmp_fields:
                    if sf.left:
                        search_buffer.tmp_field_list.append(sf.left + " " + sf.right)
                    else:
                        search_buffer.tmp_field_list.append(sf.right)

            search_buffer.select_fields = [f.split()[-1] for f in search_buffer.field_list]
            search_buffer.tmp_fields = [f.split()[-1] for f in search_buffer.tmp_field_list]
            where_expression_list = []
            for fe in search_buffer.search_sql.where_expression.split():
                if fe.startswith("result."):
                    field_name = fe.split(".")[1]
                    for rf in search_buffer.search_sql.results:
                        if rf.right == field_name:
                            search_buffer.select_fields.append(field_name)
                            search_buffer.field_list.append(f"{rf.field_name} {field_name}")
                            break
                    search_buffer.join_fields.append(field_name)
                    where_expression_list.append(
                        f"(select {field_name} from {tmp_table[field_name]})")
                else:
                    where_expression_list.append(fe)
            search_buffer.where_expression = " ".join(where_expression_list)


scm = SearchContextManager()
