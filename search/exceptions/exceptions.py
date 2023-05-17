# @Time    : 23/02/22 13:32
# @Author  : fyq
# @File    : exceptions.py
# @Software: PyCharm

__author__ = 'fyq'

from typing import Union, List


class SearchException(Exception):
    pass


class ProgressException(SearchException):
    pass


class SearchUtilException(SearchException):
    pass


class SearchParseException(SearchException):
    pass


class SearchSqlParseException(SearchParseException):
    pass


class MustNotHaveSubSelectException(SearchSqlParseException):
    pass


class FileNotFindSearchException(SearchException):
    pass


class FieldNameException(SearchException):

    def __init__(self, sql_name: str, fields: Union[str, List]):
        if isinstance(fields, str):
            f = fields
        elif isinstance(fields, list):
            f = ",".join(fields)
        else:
            f = "unknown"
        super(FieldNameException, self).__init__(f"sql查询[{sql_name}]中查询字段[{f}]请以(i,n,u,s,t,d,b)开头")


class SearchStrategyException(SearchException):

    def __init__(self):
        super(SearchStrategyException, self).__init__("服务器繁忙,请稍后再试")
