# @Time    : 2023/03/28 16:04
# @Author  : fyq
# @File    : parser.py
# @Software: PyCharm

__author__ = 'fyq'

from abc import ABCMeta, abstractmethod
from copy import copy
from typing import List, Dict

from loguru import logger

from search import models, db, constant
from search.core.decorator import transactional
from search.core.parser import SearchSqlParser, SqlParserInfo
from search.exceptions import SearchSqlParseException, SearchParseException


class ISearchParser(metaclass=ABCMeta):

    @abstractmethod
    def parse(self, search_id: int):
        pass


class SearchParser(ISearchParser):

    @classmethod
    def order_sql(cls, depend_dict: Dict[int, list], search_sql_id_list: List, order: int):
        res = {}
        pre_len = len(depend_dict)
        for search_sql_id in search_sql_id_list:
            for k, v in depend_dict.items():
                if search_sql_id in v:
                    v.pop(v.index(search_sql_id))
                    if len(v) == 0:
                        res[k] = order
            depend_dict = {k: v for k, v in depend_dict.items() if len(v) > 0}

        if pre_len == len(depend_dict):
            raise SearchParseException("sql排序失败")

        if len(depend_dict) > 0:
            res.update(cls.order_sql(depend_dict, list(res.keys()), order + 1))

        return res

    @classmethod
    def _find(cls, search_sql_list: List[models.SearchSQL], fields: str, search_field: models.SearchField, order):
        for field in fields.split(","):
            for search_sql_index, search_sql in enumerate(search_sql_list):
                right_list = [search_sql_field.real_right for search_sql_field in search_sql.fields]
                if field in right_list:
                    sf = models.SearchFieldGenPath()
                    sf.search_sql_id = search_sql.id
                    sf.depend_field = field
                    sf.search_field_id = search_field.id
                    sf.order = order
                    db.session.add(sf)
                    new_search_sql_list = list(copy(search_sql_list))
                    new_search_sql_list.pop(search_sql_index)
                    cls._find(new_search_sql_list, ",".join(
                        [search_sql_result.right for search_sql_result in search_sql.results]), search_field, order + 1)
                    break

    @transactional
    def parse(self, search_id: int):

        # 错误信息
        errors = []
        # 查询数据库
        search: models.Search = models.Search.query.filter_by(id=search_id).first()
        search_sql_list: List[models.SearchSQL] = \
            models.SearchSQL.query.filter_by(search_id=search_id).order_by(models.SearchSQL.order).all()
        search_field_list: List[models.SearchField] = \
            models.SearchField.query.filter_by(search_id=search_id).order_by(models.SearchField.order).all()
        search_condition_list: List[models.SearchCondition] = \
            models.SearchCondition.query.filter_by(search_id=search_id).order_by(models.SearchCondition.order).all()
        search_condition_dict: Dict[str, models.SearchCondition] = {search_condition.name: search_condition
                                                                    for search_condition in search_condition_list}
        # sql语句解析
        parse = SearchSqlParser()
        depend_condition = set()
        all_fields = []
        for search_sql in search_sql_list:
            info: SqlParserInfo = parse.parse(search_sql.expression)
            if len(info.from_expression) == 0:
                errors.append(f"sql[{search_sql.name}]from不能为空")
            if len(info.where_expression) == 0:
                errors.append(f"sql[{search_sql.name}]where不能为空")
            if len(info.fields) == 0:
                errors.append(f"sql[{search_sql.name}]字段不能为空")
            if len(info.conditions) == 0 and len(info.results) == 0:
                errors.append(f"sql[{search_sql.name}]条件字段和结果字段不能同时为空")

            search_sql.expression = info.expression
            search_sql.select_expression = info.select_expression
            search_sql.from_expression = info.from_expression
            search_sql.where_expression = info.where_expression
            search_sql.other_expression = info.other_expression

            for left, right in info.fields:
                sql_field = models.SearchSQLField()
                sql_field.search_sql_id = search_sql.id
                sql_field.left = left
                sql_field.right = right
                sql_field.real_right = right.split(".")[-1]
                db.session.add(sql_field)

                all_fields.append(sql_field.real_right)

            not_find_conditions = []
            for left, mid, right in info.conditions:
                sql_condition = models.SearchSqlCondition()
                sql_condition.search_sql_id = search_sql.id
                sql_condition.left = left
                sql_condition.mid = mid
                sql_condition.right = right
                sql_condition.real_right = right.split(".")[-1]
                if sql_condition.real_right not in search_condition_dict:
                    not_find_conditions.append(sql_condition.real_right)
                else:
                    sql_condition.depend_search_condition_id = search_condition_dict[sql_condition.real_right].id
                db.session.add(sql_condition)
                depend_condition.add(sql_condition.real_right)

            if len(not_find_conditions) > 0:
                errors.append(f"sql[{search_sql.name}]中{not_find_conditions}找条件中不存在")

            for left, mid, right in info.results:
                sql_result = models.SearchSqlResult()
                sql_result.search_sql_id = search_sql.id
                sql_result.left = left
                sql_result.mid = mid
                sql_result.right = right
                sql_result.real_right = right.split(".")[-1]
                db.session.add(sql_result)

        diff_conditions = set(search_condition_dict.keys()) - depend_condition
        if len(diff_conditions) > 0:
            logger.warning(f"条件{diff_conditions}未用到")

        if len(all_fields) > len(set(all_fields)):
            errors.append("sql语句生成的字段名重复")

        db.session.flush()

        major_search_id_list = []
        depend_dict = {}
        for search_sql_index, search_sql in enumerate(search_sql_list):
            if len(search_sql.results) == 0 and len(search_sql.conditions) > 0:
                search_sql.major = "1"
                search_sql.order = 0
                major_search_id_list.append(search_sql.id)
            elif len(search_sql.results) == 0:
                errors.append(f"sql[{search_sql.name}]不作为主查询,必须引用result")

            new_search_sql_list = copy(search_sql_list)
            new_search_sql_list.pop(search_sql_index)
            depend_search_sql_id_set = set()
            not_find_results = set()
            for search_sql_result in search_sql.results:
                find = False
                for new_search_sql in new_search_sql_list:
                    if search_sql_result.real_right in [new_search_sql_field.real_right
                                                        for new_search_sql_field in new_search_sql.fields]:
                        depend_search_sql_id_set.add(new_search_sql.id)
                        search_sql_result.depend_search_sql_id = new_search_sql.id
                        find = True
                        break
                if not find:
                    not_find_results.add(search_sql_result.real_right)
            if len(not_find_results) > 0:
                errors.append(f"sql[{search_sql.name}]结果字段[{not_find_results}]未找到生成的sql语句")

            if len(depend_search_sql_id_set) > 0:
                search_sql.depend = ",".join([str(d) for d in depend_search_sql_id_set])
                depend_dict[search_sql.id] = list(depend_search_sql_id_set)

        if len(major_search_id_list) == 0:
            errors.append("sql语句中必须有主查询(没有引用其他sql的就是主查询)")
        if len(major_search_id_list) > 1:
            errors.append("sql语句中只有一个主查询(没有引用其他sql的就是主查询)")

        # sql排序
        try:
            order_dict = self.order_sql(depend_dict, major_search_id_list, 1)
            for search_sql in search_sql_list:
                if search_sql.id in order_dict.keys():
                    search_sql.order = order_dict[search_sql.id]
                    search_sql.major = "0"
        except SearchParseException as e:
            errors.append(str(e))

        # 字符解析
        for search_field in search_field_list:
            not_find_results = []
            for result in search_field.result_fields.split(","):
                if result not in all_fields:
                    not_find_results.append(result)
            if len(not_find_results) > 0:
                errors.append(f"字段[{search_field.name}]中结果字段{not_find_results}未在结果集中存在")
            self._find(search_sql_list, search_field.result_fields, search_field, 0)

        if len(errors) > 0:
            raise SearchParseException(",".join(errors))

        search.status = constant.SearchStatus.ACCESS
