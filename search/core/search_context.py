# @Time    : 23/03/02 13:35
# @Author  : fyq
# @File    : search_context.py
# @Software: PyCharm

__author__ = 'fyq'

import hashlib
import time
from concurrent.futures import Future
from dataclasses import dataclass, field
from typing import List, Dict, Set, Optional

from sortedcontainers import SortedKeyList

from search import models
import search.constant as constant
import redis

from search.exceptions import SearchException
from search.extend import redis_pool
import simplejson as json


@dataclass
class SearchBuffer:
    # search_sql
    search_sql_object: models.SearchSQL = None
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
    # 连接的字段
    join_fields: List[str] = field(default_factory=lambda: [])
    # from表达式
    from_exp_list: List[str] = field(default_factory=lambda: [])
    # 临时表明
    tmp_tablename: str = None


@dataclass
class SearchContext:
    # search
    search_object: models.Search = None
    # search field
    search_field_list: List[models.SearchField] = None
    # 查询线程
    search_future: Future = None
    # 导出线程
    export_future: Future = None
    # 查询的数据
    search_exp: str = None
    # md5查询的数据
    search_exp_md5: str = None
    # 查询缓存建
    search_buffer_list: List[SearchBuffer] = field(default_factory=lambda: [])


class SearchContextManager:

    def __init__(self):
        self._cts: Dict[str, SearchContext] = {}

    def get_search_context(self, m: Dict, search_md5: str) -> Optional[SearchContext]:
        r = redis.Redis(connection_pool=redis_pool)
        key = f"{search_md5}_{constant.SEARCH_CONTEXT}"
        while not r.setnx(name=key, value=1):
            time.sleep(100)
        try:
            if search_md5 in self._cts:
                return self._cts[search_md5]
            else:
                search_name = m.get(constant.SEARCH_NAME)
                search_field_s = m.get(constant.SEARCH_FIELD)
                search_condition = m.get(constant.SEARCH_CONDITION)

                search_result: models.Search = models.Search.query.filter_by(name=search_name).first()
                if search_result:
                    search_buffer_dict: Dict[int, SearchBuffer] = {}
                    search_result: models.Search = models.Search.query.filter_by(name=search_name).first()
                    search_sql_list: List[models.SearchSQL] = models.SearchSQL.query.filter_by(
                        search_id=search_result.id).all()
                    search_field_list: List[models.SearchField] = models.SearchField.query.filter_by(
                        search_id=search_result.id).all()

                    for search_field in search_field_list:
                        if search_field.name in search_field_s:
                            for sp in search_field.search_field_gen_paths:
                                if sp.search_sql_id in search_buffer_dict:
                                    search_buffer = search_buffer_dict[sp.search_sql_id]
                                else:
                                    filter_search_sql_list = \
                                        list(filter(lambda search_sql: search_sql.id == sp.search_sql_id,
                                                    search_sql_list))
                                    assert len(filter_search_sql_list) > 0
                                    search_buffer = SearchBuffer()
                                    search_buffer.search_sql_object = filter_search_sql_list[0]
                                    search_buffer_dict[sp.search_sql_id] = search_buffer
                                search_buffer.select_fields.add(sp.depend_field)
                                if sp.order > 0:
                                    search_buffer.tmp_fields.add(sp.depend_field)

                    search_buffer_list = SortedKeyList(iterable=search_buffer_dict.values(),
                                                       key=lambda item: item.search_sql_object.order)
                    tmp_table: Dict[str, str] = {}
                    for search_buffer in search_buffer_list:

                        md5_list = list(list(search_buffer.select_fields) + list(search_buffer.tmp_fields))
                        search_buffer.tmp_tablename = "#{}" + "_" + str(search_buffer.search_sql_object.id) + "_" + \
                                                      hashlib.md5(
                                                         ",".join(md5_list).encode(encoding='utf-8')).hexdigest()
                        for tmp_field in search_buffer.tmp_fields:
                            tmp_table[tmp_field] = search_buffer.tmp_tablename

                        for sf in search_buffer.search_sql_object.search_sql_gen_fields:
                            if sf.gen_field in search_buffer.select_fields:
                                if sf.exp_field:
                                    search_buffer.field_list.append(sf.exp_field + " " + sf.gen_field)
                                else:
                                    search_buffer.field_list.append(sf.gen_field)

                            if sf.gen_field in search_buffer.tmp_fields:
                                if sf.exp_field:
                                    search_buffer.tmp_field_list.append(sf.exp_field + " " + sf.gen_field)
                                else:
                                    search_buffer.tmp_field_list.append(sf.gen_field)

                        search_buffer.select_fields = [f.split()[-1] for f in search_buffer.field_list]
                        search_buffer.tmp_fields = [f.split()[-1] for f in search_buffer.tmp_field_list]

                        from_exp: List[str] = search_buffer.search_sql_object.from_exp.split(",")
                        for fe in from_exp:
                            if fe.startswith("result."):
                                field_name = fe.split(".")[1]
                                for rf in search_buffer.search_sql_object.result_fields:
                                    if rf.result_field == field_name:
                                        search_buffer.select_fields.append(field_name)
                                        search_buffer.field_list.append(f"{rf.field_name} {field_name}")
                                        break
                                search_buffer.join_fields.append(field_name)
                                search_buffer.from_exp_list.append(f"(select {field_name} from {tmp_table[field_name]})")
                            elif fe.startswith("condition."):
                                condition_name = fe.split(".")[1]
                                if condition_name in search_condition:
                                    search_buffer.from_exp_list.append("%s")
                                    search_buffer.args.append(search_condition[condition_name])
                                else:
                                    search_buffer.from_exp_list = search_buffer.from_exp_list[
                                                                 0: len(search_buffer.from_exp_list) - 2]
                                    if search_buffer.from_exp_list[-1].upper() in ["OR", "AND"]:
                                        search_buffer.from_exp_list.pop(-1)
                            else:
                                search_buffer.from_exp_list.append(fe)
                        if search_buffer.from_exp_list[-1].upper() in ["WHERE"]:
                            search_buffer.from_exp_list.pop(-1)

                    sc = SearchContext()
                    sc.search_object = search_result
                    sc.search_exp = json.dumps(m)
                    sc.search_exp_md5 = search_md5
                    sc.search_buffer_list = [search_buffer for search_buffer in search_buffer_list]
                    sc.search_field_list = search_field_list
                    self._cts[search_md5] = sc
                    return sc
                else:
                    raise SearchException(f"{search_name}不存在或者已经失效")
        finally:
            r.delete(key)


scm = SearchContextManager()
