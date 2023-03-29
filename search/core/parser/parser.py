# @Time    : 2023/03/28 16:04
# @Author  : fyq
# @File    : parser.py
# @Software: PyCharm

__author__ = 'fyq'

from abc import ABCMeta, abstractmethod
from copy import copy
from typing import List

from flask import Flask

from search import models, db, constant
from search.core.parser import SearchSqlParser, SqlParserInfo
from search.exceptions import SearchSqlParseException


class ISearchParser(metaclass=ABCMeta):

    @abstractmethod
    def parse(self, search_id: int):
        pass


class SearchParser(ISearchParser):

    @classmethod
    def _find(cls, search_sql_list: List[models.SearchSQL], fields: str, search_field: models.SearchField, order):
        for field in fields.split(","):
            for search_sql_index, search_sql in enumerate(search_sql_list):
                right_list = [search_sql_field.right for search_sql_field in search_sql.fields]
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

    def parse(self, search_id: int):

        search: models.Search = models.Search.query.filter_by(id=search_id).first()
        search_sql_list: List[models.SearchSQL] = \
            models.SearchSQL.query.filter_by(search_id=search_id).order_by(models.SearchSQL.order).all()
        search_field_list: List[models.SearchField] = \
            models.SearchField.query.filter_by(search_id=search_id).order_by(models.SearchField.order).all()

        parse = SearchSqlParser()
        for search_sql in search_sql_list:
            info: SqlParserInfo = parse.parse(search_sql.expression)
            if len(info.from_expression) == 0:
                raise SearchSqlParseException(f"sql[{search_sql.name}]from不能为空")
            if len(info.where_expression) == 0:
                raise SearchSqlParseException(f"sql[{search_sql.name}]where不能为空")
            if len(info.fields) == 0:
                raise SearchSqlParseException(f"sql[{search_sql.name}]字段不能为空")
            if len(info.conditions) == 0 and len(info.results) == 0:
                raise SearchSqlParseException(f"sql[{search_sql.name}]条件字段和结果字段不能同时为空")

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
                db.session.add(sql_field)

            for left, mid, right in info.conditions:
                sql_condition = models.SearchSqlCondition()
                sql_condition.search_sql_id = search_sql.id
                sql_condition.left = left
                sql_condition.mid = mid
                sql_condition.right = right
                db.session.add(sql_condition)

            for left, mid, right in info.results:
                sql_result = models.SearchSqlResult()
                sql_result.search_sql_id = search_sql.id
                sql_result.left = left
                sql_result.mid = mid
                sql_result.right = right
                db.session.add(sql_result)

        db.session.flush()

        for search_field in search_field_list:
            self._find(search_sql_list, search_field.result_fields, search_field, 0)
        search.status = constant.SearchStatus.TEST
        db.session.commit()

